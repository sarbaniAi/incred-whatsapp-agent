"""InCred TeleSales WhatsApp Bot — Databricks App.

VaaniSeva architecture pattern:
- Lakebase (Postgres) for customer journey state
- psycopg connection pool with OAuth credential rotation
- FastAPI with WhatsApp webhook + simulator + dashboard
- LLM via Databricks FMAPI (GPT-OSS-120B)
"""

import json
import logging
import os
import re
import random
import uuid
from contextlib import asynccontextmanager
from threading import Thread

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config ---
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "fin-whatsapp-bot")
LAKEBASE_BRANCH = "production"
LAKEBASE_ENDPOINT = "primary"
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "ep-gentle-dawn-e14eix56.database.eastus2.azuredatabricks.net")
LAKEBASE_DB = "databricks_postgres"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-gpt-oss-120b")

# --- DB Layer (VaaniSeva pattern) ---
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from threading import Lock
import time


class CredentialConnection(psycopg.Connection):
    """Custom connection with OAuth credential rotation (from VaaniSeva)."""
    workspace_client = None
    _cached_credential = None
    _cache_timestamp = None
    _cache_duration = 3000  # 50 min
    _cache_lock = Lock()

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        if cls.workspace_client is None:
            raise ValueError("workspace_client must be set")
        kwargs["password"] = cls._get_cached_credential()
        return super().connect(conninfo, **kwargs)

    @classmethod
    def _get_cached_credential(cls):
        with cls._cache_lock:
            now = time.time()
            if cls._cached_credential and cls._cache_timestamp and now - cls._cache_timestamp < cls._cache_duration:
                return cls._cached_credential
            endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"
            try:
                result = cls.workspace_client.postgres.generate_database_credential(endpoint=endpoint_path)
                cls._cached_credential = result.token
            except (AttributeError, TypeError):
                credential = cls.workspace_client.api_client.do("POST", "/api/2.0/postgres/credentials",
                    body={"endpoint": endpoint_path})
                cls._cached_credential = credential.get("token", "")
            cls._cache_timestamp = now
            return cls._cached_credential


_pool = None


def init_pool():
    global _pool
    if _pool is not None:
        return
    from databricks.sdk import WorkspaceClient
    wc = WorkspaceClient()
    CredentialConnection.workspace_client = wc

    try:
        sp = wc.current_service_principal.me()
        username = sp.application_id
    except Exception:
        username = wc.current_user.me().user_name

    conninfo = f"dbname={LAKEBASE_DB} user={username} host={LAKEBASE_HOST} port=5432 sslmode=require"
    _pool = ConnectionPool(conninfo=conninfo, connection_class=CredentialConnection,
        min_size=1, max_size=10, timeout=30.0, open=True,
        kwargs={"autocommit": True, "row_factory": dict_row, "keepalives": 1,
                "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5})
    with _pool.connection() as conn:
        conn.execute("SELECT 1")
    logger.info("Lakebase pool initialized")


def _get_fresh_conn():
    """Get a fresh connection with new credential."""
    if CredentialConnection.workspace_client is None:
        from databricks.sdk import WorkspaceClient
        CredentialConnection.workspace_client = WorkspaceClient()

    wc = CredentialConnection.workspace_client
    endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"
    try:
        result = wc.postgres.generate_database_credential(endpoint=endpoint_path)
        token = result.token
    except (AttributeError, TypeError):
        credential = wc.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})
        token = credential.get("token", "")

    try:
        sp = wc.current_service_principal.me()
        username = sp.application_id
    except Exception:
        username = wc.current_user.me().user_name

    return psycopg.connect(
        f"dbname={LAKEBASE_DB} user={username} host={LAKEBASE_HOST} port=5432 sslmode=require",
        password=token, autocommit=True, row_factory=dict_row,
    )


# Cache a single connection
_conn = None
_conn_time = 0


def _get_conn():
    global _conn, _conn_time
    now = time.time()
    # Refresh every 45 minutes or if dead
    if _conn is None or (now - _conn_time) > 2700:
        try:
            if _conn:
                _conn.close()
        except Exception:
            pass
        _conn = _get_fresh_conn()
        _conn_time = now
    return _conn


def db_query(sql, params=None):
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                return cur.fetchall()
            return []
    except Exception as e:
        logger.error(f"DB query error: {e}")
        # Reset connection on error
        global _conn
        _conn = None
        return []


def db_one(sql, params=None):
    rows = db_query(sql, params)
    return rows[0] if rows else None


def db_exec(sql, params=None):
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except Exception as e:
        logger.error(f"DB exec error: {e}")
        global _conn
        _conn = None


# --- LLM ---
_wc = None


def call_llm(messages):
    global _wc
    if _wc is None:
        from databricks.sdk import WorkspaceClient
        _wc = WorkspaceClient()
    try:
        # Use SDK api_client for reliable auth from Databricks Apps
        response = _wc.api_client.do(
            "POST",
            f"/serving-endpoints/{LLM_ENDPOINT}/invocations",
            body={"messages": messages, "max_tokens": 500, "temperature": 0.4},
        )
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        # Handle structured content (reasoning + text list)
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    return item.get("text", "")
            return " ".join(item.get("text", str(item)) for item in content if isinstance(item, dict) and item.get("type") == "text")
        return content if content else ""
    except Exception as e:
        logger.error(f"LLM error: {e}")
        return ""


AGENT_SYSTEM_PROMPT = """You are InCred Finance's WhatsApp AI assistant helping customers complete their personal loan application.

CONTEXT: The customer dropped off during their loan application. You are re-engaging them via WhatsApp to help them complete it.

RULES:
1. Be warm, friendly, conversational — like a helpful bank advisor, NOT robotic
2. Use Hinglish (Hindi + English mix) by default. Match the customer's language
3. Keep responses SHORT (max 3-4 lines) — this is WhatsApp, not email
4. If customer asks about loan details, interest rates, eligibility — answer helpfully
5. If customer is hesitant or has objections — be empathetic, explain benefits
6. NEVER share full PAN, Aadhaar, or income back to the customer
7. If customer asks something unrelated to the loan — gently redirect

LOAN PRODUCT DETAILS (use when customer asks):
- Personal Loan: Rs 50,000 to Rs 25,00,000
- Interest Rate: 10.49% to 18% p.a. (depends on profile)
- Tenure: 12 to 60 months
- No collateral required
- Processing fee: 1-2%
- Disbursal: Within 24 hours after approval

You are given the customer's current journey state and the last few messages. Generate a natural, helpful response."""


def agent_respond(customer_context, user_message, conversation_history=None):
    """Use LLM to generate a contextual response for customer questions or objections."""
    messages = [{"role": "system", "content": AGENT_SYSTEM_PROMPT}]

    # Add customer context
    ctx = (f"Customer: {customer_context.get('name','')} from {customer_context.get('city','')}\n"
           f"Current step: {customer_context.get('current_step','')}\n"
           f"Loan requested: {customer_context.get('loan_amount_requested','not yet')}\n"
           f"Eligibility: {customer_context.get('eligibility_status','pending')}")
    messages.append({"role": "system", "content": f"Customer context:\n{ctx}"})

    # Add recent conversation history
    if conversation_history:
        for msg in conversation_history[-6:]:
            messages.append(msg)

    messages.append({"role": "user", "content": user_message})

    response = call_llm(messages)
    return response if response else "Main samajh nahi payi. Kya aap dobara bata sakte hain?"


def is_question_or_objection(message):
    """Detect if message is a question or objection (not a form field answer)."""
    lower = message.lower().strip()
    # Questions
    if any(w in lower for w in ["?", "kya", "kaise", "kitna", "kyun", "why", "how", "what", "when", "interest",
                                  "rate", "emi", "tenure", "processing", "fee", "time", "long", "safe", "secure"]):
        return True
    # Objections
    if any(w in lower for w in ["nahi chahiye", "not interested", "later", "busy", "sochna", "think",
                                  "trust", "fraud", "scam", "why should", "kyon", "zarurat nahi"]):
        return True
    return False


# --- Agent Logic ---
BASIC_FIELDS = ["dob", "gender", "pan", "address_line1", "address_city", "address_pincode"]
ELIGIBILITY_FIELDS = ["marital_status", "employment_type", "monthly_income", "company_name", "loan_amount_requested"]

FIELD_QUESTIONS = {
    "dob": "Aapki date of birth kya hai? (DD/MM/YYYY format mein)",
    "gender": "Aapka gender kya hai?\n1. Male\n2. Female\n3. Other",
    "pan": "Aapka PAN card number kya hai? (jaise ABCDE1234F)",
    "address_line1": "Aapka pura address kya hai? (Ghar/Flat no, Street)",
    "address_city": "Aap kis shehar mein rehte hain?",
    "address_pincode": "Aapke area ka PIN code kya hai? (6 digit)",
    "marital_status": "Aapki marital status kya hai?\n1. Single\n2. Married\n3. Divorced\n4. Widowed",
    "employment_type": "Aap kya kaam karte hain?\n1. Salaried\n2. Self-Employed\n3. Business Owner",
    "monthly_income": "Aapki monthly income kitni hai? (Rs mein, sirf number)",
    "company_name": "Aapki company/business ka naam kya hai?",
    "loan_amount_requested": "Aapko kitna loan chahiye? (Rs mein, jaise 500000)",
}

VALIDATORS = {
    "dob": lambda v: bool(re.match(r"^\d{2}/\d{2}/\d{4}$", v.strip())),
    "gender": lambda v: v.strip().lower() in ["male","female","other","1","2","3","m","f"],
    "pan": lambda v: bool(re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", v.strip().upper())),
    "address_pincode": lambda v: bool(re.match(r"^\d{6}$", v.strip())),
    "monthly_income": lambda v: v.strip().replace(",","").replace("rs","").replace("Rs","").isdigit(),
    "loan_amount_requested": lambda v: v.strip().replace(",","").replace("rs","").replace("Rs","").replace("lakh","").replace("lac","").strip().isdigit(),
    "marital_status": lambda v: v.strip().lower() in ["single","married","divorced","widowed","1","2","3","4"],
    "employment_type": lambda v: v.strip().lower() in ["salaried","self-employed","self employed","business","business owner","1","2","3"],
}

NORMALIZERS = {
    "gender": lambda v: {"1":"Male","2":"Female","3":"Other","m":"Male","f":"Female"}.get(v.strip().lower(), v.strip().title()),
    "pan": lambda v: v.strip().upper(),
    "marital_status": lambda v: {"1":"Single","2":"Married","3":"Divorced","4":"Widowed"}.get(v.strip(), v.strip().title()),
    "employment_type": lambda v: {"1":"Salaried","2":"Self-Employed","3":"Business Owner","self employed":"Self-Employed","business":"Business Owner"}.get(v.strip().lower(), v.strip().title()),
    "monthly_income": lambda v: str(int(float(v.strip().replace(",","").replace("rs","").replace("Rs","")))),
    "loan_amount_requested": lambda v: str(int(float(v.strip().replace(",","").replace("rs","").replace("Rs","").replace("lakh","00000").replace("lac","00000")))),
}

_sessions = {}  # phone -> {state, customer, app}


def get_or_create_session(phone):
    if phone in _sessions:
        return _sessions[phone]
    cust = db_one("SELECT * FROM customers WHERE phone = %s", (phone,))
    if not cust:
        return None
    app = db_one("SELECT * FROM loan_applications WHERE customer_id = %s ORDER BY id DESC LIMIT 1", (cust["id"],))
    if not app:
        return None
    _sessions[phone] = {"customer": cust, "app": app, "phase": "INIT"}
    return _sessions[phone]


def get_missing_fields(app, step):
    fields = BASIC_FIELDS if step == "BASIC_DETAILS" else ELIGIBILITY_FIELDS
    return [f for f in fields if not app.get(f)]


def get_next_step(app):
    if get_missing_fields(app, "BASIC_DETAILS"):
        return "BASIC_DETAILS"
    if get_missing_fields(app, "ELIGIBILITY_DETAILS"):
        return "ELIGIBILITY_DETAILS"
    if app.get("eligibility_status") in (None, "PENDING"):
        return "ELIGIBILITY_CHECK"
    if app.get("loan_amount_offered") and app.get("bank_statement_status") in (None, "PENDING"):
        return "LOAN_OFFER"
    return "COMPLETED"


def mock_eligibility(app):
    income = float(app.get("monthly_income") or 0)
    requested = float(app.get("loan_amount_requested") or 0)
    max_loan = income * 20
    offered = min(requested, max_loan)
    if offered < 50000:
        return {"eligible": False, "reason": "Income too low"}
    rate = round(random.uniform(10.49, 16.5), 2)
    tenure = 36
    emi = round(offered * rate / 100 / 12 / (1 - (1 + rate / 100 / 12) ** (-tenure)))
    return {"eligible": True, "amount": offered, "rate": rate, "tenure": tenure, "emi": emi}


def process_message(phone, message):
    """Core agent logic — process a WhatsApp message and return reply."""
    session = get_or_create_session(phone)
    if not session:
        return "Namaste! Aapka phone number humare records mein nahi mila. Kya aapne InCred app par mobile verify kiya hai?"

    cust = session["customer"]
    app = session["app"]
    first_name = cust["name"].split()[0]
    phase = session.get("phase", "INIT")

    # Log inbound
    db_exec("INSERT INTO conversation_log (customer_id, phone, direction, message_text) VALUES (%s,%s,'INBOUND',%s)",
        (cust["id"], phone, message[:500]))

    # INIT -> Welcome
    if phase == "INIT":
        step = get_next_step(app)
        step_msg = {
            "BASIC_DETAILS": "aapka loan application almost ready hai! Bas kuch basic details chahiye",
            "ELIGIBILITY_DETAILS": "basic details mil gaye! Ab income aur employment details chahiye",
            "ELIGIBILITY_CHECK": "sab details hain! Eligibility check karte hain",
            "LOAN_OFFER": "aapka loan offer ready hai!",
            "COMPLETED": "aapka application complete ho chuka hai!",
        }.get(step, "aapka loan application complete karte hain")
        session["phase"] = "CONSENT"
        reply = (f"Namaste {first_name}! Main InCred Finance ki AI assistant hoon.\n\n"
                 f"{first_name}, {step_msg}.\n\nKya hum abhi shuru karein? (YES reply karein)")
        _log_and_return(cust["id"], phone, reply)
        return reply

    # CONSENT
    if phase == "CONSENT":
        lower = message.lower().strip()
        if any(w in lower for w in ["yes","haan","ha","ok","sure","ji","chalo","start","y"]):
            session["phase"] = "COLLECTING"
            db_exec("UPDATE loan_applications SET whatsapp_session_active = true WHERE id = %s", (app["id"],))
            step = get_next_step(app)
            missing = get_missing_fields(app, step)
            if missing:
                q = FIELD_QUESTIONS.get(missing[0], f"Please provide {missing[0]}")
                session["current_field"] = missing[0]
                reply = f"Dhanyavaad! Chaliye shuru karte hain.\n\n{q}"
            else:
                reply = _handle_step_transition(session, first_name)
            _log_and_return(cust["id"], phone, reply)
            return reply
        if any(w in lower for w in ["no","nahi","later","baad","cancel"]):
            session["phase"] = "INIT"
            reply = f"Koi baat nahi {first_name}! Jab bhi ready hon, 'Hi' bhej dijiye."
            _log_and_return(cust["id"], phone, reply)
            return reply
        return "YES ya NO reply karein. Kya hum aage badhein?"

    # COLLECTING fields
    if phase == "COLLECTING":
        # Check if customer is asking a question or raising objection — use LLM
        if is_question_or_objection(message):
            customer_ctx = {**cust, **app}
            conv_history = session.get("conv_history", [])
            llm_reply = agent_respond(customer_ctx, message, conv_history)
            if llm_reply:
                # Add to conversation history
                conv_history.append({"role": "user", "content": message})
                conv_history.append({"role": "assistant", "content": llm_reply})
                session["conv_history"] = conv_history[-10:]  # Keep last 10
                # Re-ask the current field after answering
                current_field = session.get("current_field")
                if current_field:
                    q = FIELD_QUESTIONS.get(current_field, "")
                    reply = f"{llm_reply}\n\n---\nChaliye continue karte hain:\n{q}"
                else:
                    reply = llm_reply
                _log_and_return(cust["id"], phone, reply)
                return reply

        current_field = session.get("current_field")
        if not current_field:
            step = get_next_step(app)
            missing = get_missing_fields(app, step)
            if not missing:
                reply = _handle_step_transition(session, first_name)
                _log_and_return(cust["id"], phone, reply)
                return reply
            current_field = missing[0]
            session["current_field"] = current_field

        # Validate
        validator = VALIDATORS.get(current_field)
        if validator and not validator(message):
            hints = {"dob":"DD/MM/YYYY format, jaise 15/03/1990","pan":"10 characters, jaise ABCDE1234F",
                     "address_pincode":"6 digits, jaise 110001","monthly_income":"Sirf number, jaise 50000",
                     "loan_amount_requested":"Sirf number, jaise 500000"}
            reply = f"Format sahi nahi hai. {hints.get(current_field, 'Check karein')}"
            _log_and_return(cust["id"], phone, reply)
            return reply

        # Normalize and save
        normalizer = NORMALIZERS.get(current_field)
        value = normalizer(message) if normalizer else message.strip()
        app[current_field] = value

        if current_field in ("monthly_income", "loan_amount_requested"):
            db_exec(f"UPDATE loan_applications SET {current_field} = %s, updated_at = NOW() WHERE id = %s", (float(value), app["id"]))
        else:
            db_exec(f"UPDATE loan_applications SET {current_field} = %s, updated_at = NOW() WHERE id = %s", (value, app["id"]))

        # Next field
        step = get_next_step(app)
        missing = get_missing_fields(app, step)
        if missing:
            next_field = missing[0]
            session["current_field"] = next_field
            q = FIELD_QUESTIONS.get(next_field, f"Please provide {next_field}")
            total = len(BASIC_FIELDS if step == "BASIC_DETAILS" else ELIGIBILITY_FIELDS)
            done = total - len(missing)
            progress = f"({done}/{total} done) " if done > 0 else ""
            reply = f"Noted! {progress}\n\n{q}"
        else:
            reply = _handle_step_transition(session, first_name)

        _log_and_return(cust["id"], phone, reply)
        return reply

    # BANK_STATEMENT — waiting for PDF upload or AA link confirmation
    if phase == "BANK_STATEMENT":
        # Check if this is a PDF upload (handled by webhook, not here)
        if session.get("pdf_received"):
            db_exec("UPDATE loan_applications SET bank_statement_status = 'UPLOADED', current_step = 'COMPLETED', updated_at = NOW() WHERE id = %s", (app["id"],))
            reply = (f"Bahut dhanyavaad {first_name}! Aapka bank statement mil gaya hai.\n\n"
                     f"Aapka loan application ab COMPLETE ho gaya hai! InCred team 24 ghante mein aapse contact karegi.\n\n"
                     f"Application Reference: INCRED-{app['id']:06d}")
            session["phase"] = "DONE"
            _log_and_return(cust["id"], phone, reply)
            return reply
        # Customer might say they used the AA link
        lower = message.lower().strip()
        if any(w in lower for w in ["done", "ho gaya", "submitted", "sent", "bhej diya", "kar diya"]):
            db_exec("UPDATE loan_applications SET bank_statement_status = 'UPLOADED', current_step = 'COMPLETED', updated_at = NOW() WHERE id = %s", (app["id"],))
            reply = (f"Bahut badhiya {first_name}! Aapka application ab COMPLETE hai!\n\n"
                     f"InCred team 24 ghante mein aapse contact karegi.\n"
                     f"Application Reference: INCRED-{app['id']:06d}")
            session["phase"] = "DONE"
            _log_and_return(cust["id"], phone, reply)
            return reply
        # Use LLM for any other message
        customer_ctx = {**cust, **app}
        llm_reply = agent_respond(customer_ctx, message)
        reply = f"{llm_reply}\n\nBank statement ke liye Account Aggregator link use karein ya WhatsApp par PDF bhejein."
        _log_and_return(cust["id"], phone, reply)
        return reply

    # OFFER_PENDING
    if phase == "OFFER_PENDING":
        lower = message.lower().strip()
        if any(w in lower for w in ["yes","haan","accept","ok","sure"]):
            session["phase"] = "BANK_STATEMENT"
            reply = (f"Bahut accha {first_name}! Last step: Bank statement.\n\n"
                     f"Aapko Account Aggregator ka link bhej rahe hain:\n"
                     f"https://incred.com/aa-link/demo-{app['id']}\n\n"
                     f"Ya WhatsApp par PDF bhej sakte hain.")
        elif any(w in lower for w in ["no","nahi","decline"]):
            reply = f"Koi baat nahi {first_name}. Agar aapka mann badle toh 'Hi' bhejein."
            session["phase"] = "INIT"
        else:
            reply = "Loan offer accept karne ke liye YES, ya decline ke liye NO bhejein."
        _log_and_return(cust["id"], phone, reply)
        return reply

    return "Namaste! 'Hi' bhejein to shuru karein."


def _handle_step_transition(session, first_name):
    app = session["app"]
    step = get_next_step(app)

    if step == "ELIGIBILITY_DETAILS":
        db_exec("UPDATE loan_applications SET current_step = 'ELIGIBILITY_DETAILS' WHERE id = %s", (app["id"],))
        missing = get_missing_fields(app, "ELIGIBILITY_DETAILS")
        if missing:
            session["current_field"] = missing[0]
            q = FIELD_QUESTIONS.get(missing[0])
            return f"Basic details complete! Ab eligibility ke liye kuch aur details.\n\n{q}"

    if step == "ELIGIBILITY_CHECK":
        db_exec("UPDATE loan_applications SET current_step = 'ELIGIBILITY_CHECK' WHERE id = %s", (app["id"],))
        result = mock_eligibility(app)
        if result["eligible"]:
            amt = f"{int(result['amount']):,}"
            app["loan_amount_offered"] = result["amount"]
            app["interest_rate"] = result["rate"]
            app["tenure_months"] = result["tenure"]
            app["emi_amount"] = result["emi"]
            db_exec("""UPDATE loan_applications SET loan_amount_offered=%s, interest_rate=%s,
                tenure_months=%s, emi_amount=%s, eligibility_status='ELIGIBLE', current_step='LOAN_OFFER' WHERE id=%s""",
                (result["amount"], result["rate"], result["tenure"], result["emi"], app["id"]))
            session["phase"] = "OFFER_PENDING"
            return (f"Congratulations {first_name}! Aap eligible hain!\n\n"
                    f"Loan: Rs {amt}\nRate: {result['rate']}% p.a.\nTenure: {result['tenure']} months\n"
                    f"EMI: Rs {int(result['emi']):,}/month\n\nAccept karein? (YES/NO)")
        else:
            db_exec("UPDATE loan_applications SET eligibility_status='NOT_ELIGIBLE' WHERE id=%s", (app["id"],))
            session["phase"] = "INIT"
            return f"Sorry {first_name}, abhi eligible nahi hain. Kuch mahino baad try karein."

    if step == "COMPLETED":
        session["phase"] = "INIT"
        return f"Aapka application complete hai {first_name}! InCred team jaldi contact karegi."

    # Default: ask next missing field
    missing = get_missing_fields(app, step)
    if missing:
        session["current_field"] = missing[0]
        return FIELD_QUESTIONS.get(missing[0], f"Please provide {missing[0]}")
    return "Application process complete!"


def _log_and_return(cust_id, phone, reply):
    try:
        db_exec("INSERT INTO conversation_log (customer_id, phone, direction, message_text) VALUES (%s,%s,'OUTBOUND',%s)",
            (cust_id, phone, reply[:500]))
    except Exception as e:
        logger.error(f"Log error: {e}")


# --- FastAPI App ---
def _startup_bg():
    import traceback
    try:
        logger.info("Initializing Lakebase connection...")
        conn = _get_conn()
        result = db_one("SELECT COUNT(*) as cnt FROM customers")
        logger.info(f"Lakebase OK: {result}")
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        logger.error(traceback.format_exc())


def _init_simple_connection():
    """Fallback: single psycopg connection without pool."""
    global _pool
    from databricks.sdk import WorkspaceClient
    wc = WorkspaceClient()
    endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"
    try:
        result = wc.postgres.generate_database_credential(endpoint=endpoint_path)
        token = result.token
    except (AttributeError, TypeError):
        credential = wc.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})
        token = credential.get("token", "")

    try:
        sp = wc.current_service_principal.me()
        username = sp.application_id
    except Exception:
        username = wc.current_user.me().user_name

    logger.info(f"Connecting as {username} to {LAKEBASE_HOST}/{LAKEBASE_DB}")

    # Use a minimal pool (size 1)
    conninfo = f"dbname={LAKEBASE_DB} user={username} host={LAKEBASE_HOST} port=5432 sslmode=require"

    class SimpleConn(psycopg.Connection):
        _token = token
        @classmethod
        def connect(cls, conninfo="", **kwargs):
            kwargs["password"] = cls._token
            return super().connect(conninfo, **kwargs)

    _pool = ConnectionPool(conninfo=conninfo, connection_class=SimpleConn,
        min_size=1, max_size=3, timeout=30.0, open=True,
        kwargs={"autocommit": True, "row_factory": dict_row})
    with _pool.connection() as conn:
        conn.execute("SELECT 1")
    logger.info("Fallback simple connection SUCCESS")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Thread(target=_startup_bg, daemon=True).start()
    yield


app = FastAPI(title="InCred TeleSales Bot", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        return HTMLResponse(f.read())


@app.get("/health")
async def health():
    try:
        result = db_one("SELECT COUNT(*) as cnt FROM customers")
        return {"status": "ok", "customers": result.get("cnt",0) if result else 0, "service": "incred-telesales-bot"}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


# --- WhatsApp Webhook (Twilio / Kaleyra) ---
@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    body = await request.json() if request.headers.get("content-type","").startswith("application/json") else dict(await request.form())
    phone = str(body.get("From", body.get("from", ""))).replace("whatsapp:", "")
    message = str(body.get("Body", body.get("text", body.get("message", ""))))

    if not phone:
        return {"status": "ignored"}

    if not phone.startswith("+"):
        phone = f"+91{phone}" if not phone.startswith("91") else f"+{phone}"

    # Check for media attachment (PDF bank statement)
    num_media = int(body.get("NumMedia", body.get("num_media", 0)) or 0)
    media_url = body.get("MediaUrl0", body.get("media_url", ""))
    media_type = body.get("MediaContentType0", body.get("media_content_type", ""))

    if num_media > 0 and media_url:
        logger.info(f"WhatsApp media from {phone}: {media_type} | {media_url}")
        reply = _handle_media_upload(phone, media_url, media_type, message)
    elif message:
        logger.info(f"WhatsApp from {phone}: {message}")
        reply = process_message(phone, message)
    else:
        reply = "Namaste! Kaise madad kar sakte hain?"

    # Return TwiML with reply
    from xml.etree.ElementTree import Element, tostring
    resp = Element("Response")
    msg_elem = Element("Message")
    msg_elem.text = reply
    resp.append(msg_elem)
    return HTMLResponse(content=f'<?xml version="1.0" encoding="UTF-8"?>{tostring(resp, encoding="unicode")}',
                        media_type="application/xml")


def _send_whatsapp_reply(to_phone, message):
    """Send WhatsApp reply via Twilio REST API."""
    try:
        import urllib.request as ureq
        import urllib.parse
        import base64
        auth = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
        data = urllib.parse.urlencode({
            "From": TWILIO_WHATSAPP_FROM,
            "To": f"whatsapp:{to_phone}",
            "Body": message,
        }).encode()
        req = ureq.Request(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            data=data, headers={"Authorization": f"Basic {auth}"}, method="POST")
        ureq.urlopen(req, timeout=10)
        logger.info(f"Reply sent to {to_phone} via API")
    except Exception as e:
        logger.error(f"Send reply error: {e}")


def _handle_media_upload(phone, media_url, media_type, caption=""):
    """Handle PDF/image upload from WhatsApp (bank statement)."""
    session = get_or_create_session(phone)
    if not session:
        return "Aapka phone number nahi mila. Pehle 'Hi' bhejein."

    cust = session["customer"]
    app = session["app"]
    first_name = cust["name"].split()[0]

    # Check if it's a PDF or image
    is_pdf = "pdf" in (media_type or "").lower()
    is_image = any(t in (media_type or "").lower() for t in ["image", "jpg", "jpeg", "png"])

    if is_pdf or is_image:
        doc_type = "PDF" if is_pdf else "image"
        logger.info(f"Bank statement {doc_type} received from {first_name}: {media_url}")

        # Save to Lakebase
        db_exec("INSERT INTO conversation_log (customer_id, phone, direction, message_text, message_type) VALUES (%s,%s,'INBOUND',%s,'MEDIA')",
            (cust["id"], phone, f"[{doc_type.upper()}] {media_url}"))

        # Update application status
        db_exec("UPDATE loan_applications SET bank_statement_status = 'UPLOADED', current_step = 'COMPLETED', updated_at = NOW() WHERE id = %s",
            (app["id"],))

        # Mark session
        session["pdf_received"] = True
        session["phase"] = "DONE"

        # In production: forward media_url to InCred's underwriting API
        # requests.post("https://incred-api/upload-statement", json={"url": media_url, "customer_id": cust["id"]})

        return (f"Dhanyavaad {first_name}! Aapka bank statement ({doc_type}) successfully mil gaya hai.\n\n"
                f"Aapka loan application ab COMPLETE ho gaya hai!\n"
                f"InCred team 24 ghante mein aapse contact karegi.\n\n"
                f"Application Reference: INCRED-{app['id']:06d}")
    else:
        return f"Yeh file type support nahi hai. Please PDF ya image mein bank statement bhejein."


@app.get("/webhook/whatsapp")
async def whatsapp_verify():
    return {"status": "verified"}


# --- Simulator API ---
@app.post("/api/simulate")
async def simulate(request: Request):
    body = await request.json()
    phone = body.get("phone", "+919910175907")
    message = body.get("message", "")
    if not message:
        raise HTTPException(400, "message required")
    reply = process_message(phone, message)
    return {"reply": reply, "phone": phone}


@app.post("/api/simulate/reset")
async def simulate_reset(request: Request):
    body = await request.json()
    phone = body.get("phone", "+919910175907")
    _sessions.pop(phone, None)
    return {"status": "reset", "phone": phone}


@app.get("/api/sample-customers")
async def sample_customers():
    rows = db_query("""SELECT c.phone, c.name, c.city, la.current_step, la.drop_off_step
        FROM customers c JOIN loan_applications la ON la.customer_id = c.id
        WHERE la.current_step != 'COMPLETED'
        ORDER BY la.drop_off_at DESC LIMIT 10""")
    return rows


# --- Nudge Trigger API (outbound WhatsApp) ---
TWILIO_SID = os.environ.get("TWILIO_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")


@app.post("/api/nudge")
async def trigger_nudge(request: Request):
    """Trigger outbound WhatsApp nudge to a dropped customer."""
    body = await request.json()
    customer_id = body.get("customer_id")

    if not customer_id:
        # Pick the next un-nudged incomplete customer
        cust = db_one("""SELECT c.id, c.name, c.phone, la.current_step, la.id as app_id
            FROM customers c JOIN loan_applications la ON la.customer_id = c.id
            WHERE la.current_step != 'COMPLETED' AND la.whatsapp_nudge_sent = false
            ORDER BY la.drop_off_at ASC LIMIT 1""")
    else:
        cust = db_one("""SELECT c.id, c.name, c.phone, la.current_step, la.id as app_id
            FROM customers c JOIN loan_applications la ON la.customer_id = c.id
            WHERE c.id = %s""", (customer_id,))

    if not cust:
        return {"status": "no_customers", "message": "No customers to nudge"}

    first_name = cust["name"].split()[0]
    step = cust["current_step"]

    step_msg = {
        "OTP_VERIFIED": "aapka personal loan application shuru hua tha lekin complete nahi hua. Bas kuch details chahiye!",
        "BASIC_DETAILS": "aapne basic details dena shuru kiya tha. Bas thoda aur chahiye!",
        "ELIGIBILITY_DETAILS": "aapke basic details mil gaye. Ab eligibility check ke liye income details chahiye.",
        "LOAN_OFFER": "aapka loan offer ready hai! Dekhiye kitna mil sakta hai.",
        "BANK_STATEMENT": "loan offer accept karne ke baad, bas bank statement upload karna hai.",
    }.get(step, "aapka loan application complete karte hain.")

    nudge_msg = (
        f"Namaste {first_name}! InCred Finance yahan se.\n\n"
        f"{first_name}, {step_msg}\n\n"
        f"Abhi complete karein — sirf 2 minute lagenge! Reply *YES* to continue."
    )

    # Send via Twilio API
    import urllib.parse
    try:
        import urllib.request as ureq
        import base64
        auth = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
        data = urllib.parse.urlencode({
            "From": TWILIO_WHATSAPP_FROM,
            "To": f"whatsapp:{cust['phone']}",
            "Body": nudge_msg,
        }).encode()
        req = ureq.Request(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            data=data, headers={"Authorization": f"Basic {auth}"}, method="POST")
        resp = ureq.urlopen(req)
        msg_data = json.loads(resp.read())
        msg_sid = msg_data.get("sid", "")
        logger.info(f"Nudge sent to {cust['name']} ({cust['phone']}): {msg_sid}")

        # Mark as nudged
        db_exec("UPDATE loan_applications SET whatsapp_nudge_sent = true, updated_at = NOW() WHERE id = %s", (cust["app_id"],))

        return {
            "status": "sent",
            "customer": cust["name"],
            "phone": cust["phone"],
            "step": step,
            "message": nudge_msg,
            "twilio_sid": msg_sid,
        }
    except Exception as e:
        logger.error(f"Nudge error: {e}")
        return {"status": "error", "message": str(e)[:300]}


@app.get("/api/nudge-candidates")
async def nudge_candidates():
    """Get list of customers eligible for nudge."""
    return db_query("""SELECT c.id, c.name, c.phone, c.city, la.current_step, la.drop_off_step, la.whatsapp_nudge_sent
        FROM customers c JOIN loan_applications la ON la.customer_id = c.id
        WHERE la.current_step != 'COMPLETED'
        ORDER BY la.whatsapp_nudge_sent ASC, la.drop_off_at ASC LIMIT 20""")


# --- Dashboard API ---
@app.get("/api/stats")
async def get_stats():
    return db_one("""SELECT
        (SELECT COUNT(*) FROM customers) as total_customers,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step != 'COMPLETED') as total_dropped,
        (SELECT COUNT(*) FROM loan_applications WHERE whatsapp_nudge_sent = true) as nudge_sent,
        (SELECT COUNT(*) FROM loan_applications WHERE whatsapp_session_active = true) as active_sessions,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step = 'OTP_VERIFIED') as at_otp,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step = 'BASIC_DETAILS') as at_basic,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step IN ('ELIGIBILITY_DETAILS','ELIGIBILITY_CHECK')) as at_eligibility,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step = 'LOAN_OFFER') as at_offer,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step = 'BANK_STATEMENT') as at_bank,
        (SELECT COUNT(*) FROM loan_applications WHERE current_step = 'COMPLETED') as completed
    """) or {}


@app.get("/api/customers")
async def list_customers():
    return db_query("""SELECT c.id, c.name, c.phone, c.city, c.email,
        la.current_step, la.drop_off_step, la.loan_amount_requested, la.loan_amount_offered,
        la.eligibility_status, la.whatsapp_nudge_sent, la.whatsapp_session_active
        FROM customers c JOIN loan_applications la ON la.customer_id = c.id
        ORDER BY la.updated_at DESC LIMIT 50""")


@app.get("/api/conversations/{phone}")
async def get_conversations(phone: str):
    return db_query("SELECT direction, message_text, created_at FROM conversation_log WHERE phone = %s ORDER BY id", (phone,))
