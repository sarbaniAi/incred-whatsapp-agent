// InCred Reference Verification Dashboard

let currentSimSession = null;

// --- Tab Navigation ---
function switchTab(tab) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  document.querySelector(`[data-tab="${tab}"]`).classList.add('active');

  if (tab === 'dashboard') loadDashboard();
  if (tab === 'applicants') loadApplicants();
  if (tab === 'queue') loadQueue('PENDING');
  if (tab === 'simulator') loadSampleRefs();
  if (tab === 'agents') loadAgents();
}

// --- Dashboard ---
async function loadDashboard() {
  try {
    const [stats, verifications] = await Promise.all([
      fetch('/api/stats').then(r => r.json()),
      fetch('/api/verifications?limit=20').then(r => r.json()),
    ]);
    renderStats(stats);
    renderVerifications(verifications);
  } catch (e) {
    console.error('Dashboard load error:', e);
  }
}

function renderStats(s) {
  const grid = document.getElementById('stats-grid');
  const totalVerified = (s.positive || 0) + (s.negative || 0) + (s.inconclusive || 0);
  const successRate = totalVerified > 0 ? ((s.positive || 0) / totalVerified * 100).toFixed(1) : '0.0';
  const avgMin = s.avg_duration ? (s.avg_duration / 60).toFixed(1) : '0';

  grid.innerHTML = `
    <div class="stat-card blue">
      <div class="stat-value">${s.total_applicants || 0}</div>
      <div class="stat-label">Total Applicants</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${s.total_references || 0}</div>
      <div class="stat-label">Total References</div>
    </div>
    <div class="stat-card green">
      <div class="stat-value">${s.total_verified || 0}</div>
      <div class="stat-label">Verified</div>
    </div>
    <div class="stat-card green">
      <div class="stat-value">${s.positive || 0}</div>
      <div class="stat-label">Positive</div>
    </div>
    <div class="stat-card red">
      <div class="stat-value">${s.negative || 0}</div>
      <div class="stat-label">Negative</div>
    </div>
    <div class="stat-card yellow">
      <div class="stat-value">${s.inconclusive || 0}</div>
      <div class="stat-label">Inconclusive</div>
    </div>
    <div class="stat-card primary">
      <div class="stat-value">${s.queue_pending || 0}</div>
      <div class="stat-label">Queue Pending</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${successRate}%</div>
      <div class="stat-label">Success Rate</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${s.whatsapp_verified || 0}</div>
      <div class="stat-label">WhatsApp Verified</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${s.call_verified || 0}</div>
      <div class="stat-label">Call Verified</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">${avgMin}m</div>
      <div class="stat-label">Avg Duration</div>
    </div>
    <div class="stat-card blue">
      <div class="stat-value">${s.queue_active || 0}</div>
      <div class="stat-label">Active Now</div>
    </div>
  `;
}

function renderVerifications(rows) {
  const tbody = document.querySelector('#verifications-table tbody');
  tbody.innerHTML = rows.map(v => `
    <tr>
      <td>${v.id}</td>
      <td>${v.ref_name || ''}</td>
      <td><span class="badge badge-${(v.ref_type || '').toLowerCase()}">${v.ref_type || ''}</span></td>
      <td>${v.applicant_name || ''}</td>
      <td><span class="badge badge-${(v.channel || '').toLowerCase()}">${v.channel || ''}</span></td>
      <td><span class="badge badge-${(v.status || '').toLowerCase()}">${v.status || ''}</span></td>
      <td>${v.outcome_reason || '-'}</td>
      <td>${v.duration_seconds ? (v.duration_seconds / 60).toFixed(1) + 'm' : '-'}</td>
    </tr>
  `).join('');
}

// --- Applicants ---
async function loadApplicants() {
  try {
    const rows = await fetch('/api/applicants').then(r => r.json());
    const tbody = document.querySelector('#applicants-table tbody');
    tbody.innerHTML = rows.map(a => `
      <tr onclick="showRefs(${a.id}, '${a.name}')">
        <td><strong>${a.name}</strong></td>
        <td>${a.city || ''}</td>
        <td>${a.employer || ''}</td>
        <td>${a.amount_requested ? '₹' + Number(a.amount_requested).toLocaleString('en-IN') : '-'}</td>
        <td><span class="badge badge-${(a.app_status || '').toLowerCase().replace(/_/g, '-')}">${a.app_status || ''}</span></td>
        <td>${a.ref_count || 0}</td>
        <td>${a.verified_refs || 0}</td>
        <td>${a.positive_refs || 0}</td>
      </tr>
    `).join('');
  } catch (e) {
    console.error('Applicants error:', e);
  }
}

async function showRefs(applicantId, name) {
  try {
    const refs = await fetch(`/api/references/${applicantId}`).then(r => r.json());
    const card = document.getElementById('ref-details-card');
    document.getElementById('ref-details-title').textContent = `References for ${name}`;
    const tbody = document.querySelector('#ref-details-table tbody');
    tbody.innerHTML = refs.map(r => `
      <tr>
        <td>${r.ref_name}</td>
        <td>${r.ref_phone}</td>
        <td>${r.ref_type}</td>
        <td>${r.relationship || '-'}</td>
        <td><span class="badge badge-${(r.verification_status || 'pending').toLowerCase()}">${r.verification_status || 'Not Started'}</span></td>
        <td>${r.channel || '-'}</td>
        <td>${r.outcome_reason || '-'}</td>
      </tr>
    `).join('');
    card.style.display = 'block';
  } catch (e) {
    console.error('Refs error:', e);
  }
}

// --- Queue ---
async function loadQueue(status) {
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  event && event.target && event.target.classList.add('active');
  try {
    const rows = await fetch(`/api/queue?status=${status}`).then(r => r.json());
    document.getElementById('queue-count').textContent = rows.length;
    const tbody = document.querySelector('#queue-table tbody');
    tbody.innerHTML = rows.map(q => {
      const priorityColors = { 1: '#e63946', 2: '#e76f51', 3: '#e9c46a', 4: '#2a9d8f', 5: '#457b9d' };
      return `
        <tr>
          <td><span style="color:${priorityColors[q.priority] || '#666'};font-weight:700">P${q.priority}</span></td>
          <td>${q.ref_name}</td>
          <td>${q.ref_type}</td>
          <td>${q.applicant_name}</td>
          <td><span class="badge badge-${(q.channel || '').toLowerCase()}">${q.channel}</span></td>
          <td>${q.assigned_agent || '-'}</td>
          <td>${q.retry_count || 0}</td>
          <td>${q.scheduled_at ? new Date(q.scheduled_at).toLocaleString() : '-'}</td>
        </tr>
      `;
    }).join('');
  } catch (e) {
    console.error('Queue error:', e);
  }
}

// --- Agents ---
async function loadAgents() {
  try {
    const rows = await fetch('/api/agents').then(r => r.json());
    const tbody = document.querySelector('#agents-table tbody');
    tbody.innerHTML = rows.map(a => {
      const rate = a.total_verifications > 0
        ? ((a.positive_count / a.total_verifications) * 100).toFixed(1) + '%'
        : '-';
      return `
        <tr>
          <td>${a.id}</td>
          <td>${a.name}</td>
          <td>${a.team}</td>
          <td>${a.daily_target}</td>
          <td>${a.total_verifications}</td>
          <td>${a.positive_count}</td>
          <td>${rate}</td>
          <td>${a.avg_duration ? (a.avg_duration / 60).toFixed(1) + 'm' : '-'}</td>
        </tr>
      `;
    }).join('');
  } catch (e) {
    console.error('Agents error:', e);
  }
}

// --- WhatsApp Simulator ---
async function loadSampleRefs() {
  try {
    const refs = await fetch('/api/sample-references').then(r => r.json());
    const container = document.getElementById('sample-refs');
    container.innerHTML = refs.map(r => `
      <div class="ref-card" onclick="startWithRef('${r.ref_phone}', '${r.ref_name}')">
        <div class="ref-name">${r.ref_name}</div>
        <div class="ref-meta">${r.ref_type} | ${r.applicant_name} | ${r.ref_phone}</div>
      </div>
    `).join('');
  } catch (e) {
    console.error('Sample refs error:', e);
  }
}

function startWithRef(phone, name) {
  document.getElementById('sim-phone').value = phone;
  document.getElementById('chat-ref-name').textContent = name;
  startSimulation();
}

async function startSimulation() {
  const phone = document.getElementById('sim-phone').value.trim();
  if (!phone) return alert('Enter a phone number');

  const chatMessages = document.getElementById('chat-messages');
  chatMessages.innerHTML = '<div class="chat-empty">Connecting...</div>';

  try {
    const res = await fetch('/api/simulate/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ phone }),
    });

    if (!res.ok) {
      const err = await res.json();
      chatMessages.innerHTML = `<div class="chat-empty">Error: ${err.detail || 'Unknown error'}</div>`;
      return;
    }

    const data = await res.json();
    currentSimSession = data.session_id;

    if (data.reference) {
      document.getElementById('chat-ref-name').textContent = data.reference.ref_name || 'Reference';
    }

    chatMessages.innerHTML = '';
    addMessage('agent', data.agent_message);

    document.getElementById('chat-input').disabled = false;
    document.getElementById('chat-send').disabled = false;
    document.getElementById('chat-input').focus();
  } catch (e) {
    chatMessages.innerHTML = `<div class="chat-empty">Connection error: ${e.message}</div>`;
  }
}

async function sendMessage() {
  const input = document.getElementById('chat-input');
  const message = input.value.trim();
  if (!message || !currentSimSession) return;

  input.value = '';
  addMessage('user', message);

  const sendBtn = document.getElementById('chat-send');
  sendBtn.disabled = true;
  input.disabled = true;

  try {
    const res = await fetch('/api/simulate/message', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: currentSimSession, message }),
    });

    const data = await res.json();
    addMessage('agent', data.agent_message);
  } catch (e) {
    addMessage('agent', 'Error: Could not reach the agent.');
  }

  sendBtn.disabled = false;
  input.disabled = false;
  input.focus();
}

function addMessage(role, text) {
  const container = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.className = `msg msg-${role}`;
  const now = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  div.innerHTML = `${text}<div class="msg-time">${now}</div>`;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
}

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
  loadDashboard();
});
