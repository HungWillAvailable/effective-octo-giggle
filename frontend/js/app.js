/**
 * app.js — Frontend logic cho Telegram Video Uploader Dashboard.
 *
 * Tính năng:
 *   - WebSocket kết nối real-time để nhận task updates
 *   - Submit URL upload qua REST API
 *   - Drag & Drop file lên dropzone
 *   - Hiển thị active tasks với progress bar thời gian thực
 *   - History table cho completed tasks
 *   - System stats (disk, active tasks)
 *   - Toast notifications
 *   - Persist config (channel ID, max part size) vào localStorage
 */

'use strict';

// ─────────────────────────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────────────────────────
const state = {
  tasks: {},          // task_id → task object
  ws: null,
  wsRetries: 0,
  maxWsRetries: 10,
  wsReconnectDelay: 2000,
  diskInfo: { free: 0, total: 0, used: 0 },
};

// ─────────────────────────────────────────────────────────────────
// DOM REFS
// ─────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

const dom = {
  statusBadge:    $('status-badge'),
  statusText:     $('status-text'),

  // Stats
  statActive:     $('stat-active'),
  statDone:       $('stat-done'),
  statDisk:       $('stat-disk'),
  statDiskFill:   $('stat-disk-fill'),
  statDiskSub:    $('stat-disk-sub'),

  // Upload form
  urlInput:       $('url-input'),
  uploadBtn:      $('upload-btn'),
  dropzone:       $('dropzone'),
  fileInput:      $('file-input'),

  // Task list
  activeList:     $('active-tasks-list'),
  historyBody:    $('history-tbody'),
  historySection: $('history-section'),
  activeSection:  $('active-section'),

  // Config
  configChannelId: $('config-channel-id'),
  configMaxPart:   $('config-max-part'),
  configSaveBtn:   $('config-save-btn'),

  // Misc
  warningBanner:  $('warning-banner'),
  warningText:    $('warning-text'),
  cleanupBtn:     $('cleanup-btn'),
  toastContainer: $('toast-container'),
};

// ─────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

function formatTime(ts) {
  if (!ts) return '—';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function timeSince(ts) {
  if (!ts) return '';
  const secs = Math.floor(Date.now() / 1000 - ts);
  if (secs < 60)    return `${secs}s trước`;
  if (secs < 3600)  return `${Math.floor(secs / 60)}m trước`;
  return `${Math.floor(secs / 3600)}h trước`;
}

function statusLabel(status) {
  const map = {
    pending:     '⏳ Chờ',
    downloading: '⬇️ Đang tải',
    processing:  '✂️ FFmpeg',
    uploading:   '📤 Upload',
    done:        '✅ Xong',
    error:       '❌ Lỗi',
    cancelled:   '⛔ Hủy',
  };
  return map[status] || status;
}

function getBadgeClass(status) {
  return `task-badge badge-${status}`;
}

// ─────────────────────────────────────────────────────────────────
// TOAST
// ─────────────────────────────────────────────────────────────────

function showToast(message, type = 'info', duration = 4000) {
  const icons = { success: '✅', error: '❌', info: 'ℹ️', warning: '⚠️' };
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span>${icons[type] || 'ℹ️'}</span><span>${message}</span>`;
  dom.toastContainer.prepend(toast);
  setTimeout(() => {
    toast.style.animation = 'toast-out 0.3s ease forwards';
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// ─────────────────────────────────────────────────────────────────
// WEBSOCKET
// ─────────────────────────────────────────────────────────────────

function connectWebSocket() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${location.host}/ws`;

  state.ws = new WebSocket(wsUrl);

  state.ws.onopen = () => {
    console.log('🔌 WebSocket connected');
    state.wsRetries = 0;
    state.wsReconnectDelay = 2000;
    setConnectionStatus('connected');
  };

  state.ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      handleWsMessage(msg);
    } catch (e) {
      console.warn('Invalid WS message:', e);
    }
  };

  state.ws.onclose = () => {
    setConnectionStatus('disconnected');
    if (state.wsRetries < state.maxWsRetries) {
      state.wsRetries++;
      console.log(`🔄 WS reconnect in ${state.wsReconnectDelay}ms (attempt ${state.wsRetries})`);
      setTimeout(connectWebSocket, state.wsReconnectDelay);
      state.wsReconnectDelay = Math.min(state.wsReconnectDelay * 1.5, 30000);
    }
  };

  state.ws.onerror = (e) => {
    console.error('WS error:', e);
    setConnectionStatus('error');
  };

  // Keep-alive ping every 25 seconds
  setInterval(() => {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send('ping');
    }
  }, 25000);
}

function handleWsMessage(msg) {
  switch (msg.type) {
    case 'task_update':
      state.tasks[msg.task.id] = msg.task;
      renderTasks();
      updateStats();
      // Toast on terminal states
      if (msg.task.status === 'done') {
        showToast(`✅ Upload hoàn tất: ${msg.task.filename}`, 'success');
      } else if (msg.task.status === 'error') {
        showToast(`❌ Lỗi: ${msg.task.filename} — ${msg.task.error}`, 'error', 7000);
      }
      break;

    case 'task_list':
      state.tasks = {};
      msg.tasks.forEach(t => { state.tasks[t.id] = t; });
      renderTasks();
      updateStats();
      break;

    case 'system_status':
      state.diskInfo = {
        free:  msg.disk_free,
        total: msg.disk_total,
        used:  msg.disk_used,
      };
      renderDiskStats();
      break;
  }
}

function setConnectionStatus(status) {
  dom.statusBadge.className = `status-badge ${status}`;
  const labels = {
    connected:    '🟢 Đã kết nối',
    disconnected: '🔴 Mất kết nối',
    error:        '❌ Lỗi kết nối',
  };
  dom.statusText.textContent = labels[status] || status;
}

// ─────────────────────────────────────────────────────────────────
// STATS RENDERING
// ─────────────────────────────────────────────────────────────────

function updateStats() {
  const tasks = Object.values(state.tasks);
  const active = tasks.filter(t =>
    ['downloading','processing','uploading','pending'].includes(t.status)
  ).length;
  const done = tasks.filter(t => t.status === 'done').length;

  dom.statActive.textContent = active;
  dom.statDone.textContent = done;
}

function renderDiskStats() {
  const { free, total } = state.diskInfo;
  if (!total) return;
  const pct = ((total - free) / total * 100).toFixed(1);
  dom.statDisk.textContent = formatBytes(free);
  dom.statDiskSub.textContent = `Còn trống / ${formatBytes(total)} tổng`;
  dom.statDiskFill.style.width = `${pct}%`;
  // Đổi màu khi gần đầy
  if (parseFloat(pct) > 85) {
    dom.statDiskFill.style.background = 'var(--grad-error)';
  } else if (parseFloat(pct) > 70) {
    dom.statDiskFill.style.background = 'var(--grad-warning)';
  } else {
    dom.statDiskFill.style.background = 'var(--grad-primary)';
  }
}

// ─────────────────────────────────────────────────────────────────
// TASK RENDERING
// ─────────────────────────────────────────────────────────────────

const ACTIVE_STATUSES  = new Set(['pending','downloading','processing','uploading']);
const HISTORY_STATUSES = new Set(['done','error','cancelled']);

function renderTasks() {
  const tasks = Object.values(state.tasks)
    .sort((a, b) => b.created_at - a.created_at);

  const active  = tasks.filter(t => ACTIVE_STATUSES.has(t.status));
  const history = tasks.filter(t => HISTORY_STATUSES.has(t.status));

  renderActiveList(active);
  renderHistory(history);
}

function renderActiveList(tasks) {
  if (tasks.length === 0) {
    dom.activeList.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">🎬</div>
        <div class="empty-title">Chưa có task nào đang chạy</div>
        <div class="empty-sub">Upload một video để bắt đầu</div>
      </div>`;
    return;
  }

  dom.activeList.innerHTML = tasks.map(t => buildTaskCard(t)).join('');

  // Bind delete buttons
  tasks.forEach(t => {
    const btn = document.getElementById(`del-${t.id}`);
    if (btn) btn.onclick = () => deleteTask(t.id);
  });
}

function buildTaskCard(task) {
  const pct = Math.min(100, Math.max(0, task.progress || 0));
  const badgeClass = getBadgeClass(task.status);
  const isRunning  = ACTIVE_STATUSES.has(task.status);

  // Parts dots
  let partsDots = '';
  if (task.parts_total > 1) {
    partsDots = '<div class="parts-indicator">';
    for (let i = 1; i <= task.parts_total; i++) {
      let cls = 'part-dot';
      if (i <= task.parts_done) cls += ' done';
      else if (i === task.parts_done + 1 && isRunning) cls += ' active';
      partsDots += `<div class="${cls}" title="Part ${i}"></div>`;
    }
    partsDots += '</div>';
  }

  return `
    <div class="task-card status-${task.status}" id="task-${task.id}">
      <div class="task-header">
        <div class="task-name" title="${task.filename}">${task.filename}</div>
        <div class="task-actions">
          <button class="btn btn-danger" id="del-${task.id}" title="Xóa khỏi danh sách">✕</button>
        </div>
      </div>
      <div class="task-meta">
        <span class="${badgeClass}">${statusLabel(task.status)}</span>
        ${task.size ? `<span class="task-badge badge-pending">📦 ${formatBytes(task.size)}</span>` : ''}
        ${task.parts_total > 1
          ? `<span class="task-badge badge-pending">📁 ${task.parts_done}/${task.parts_total} phần</span>`
          : ''}
      </div>
      <div class="progress-wrap">
        <div class="progress-header">
          <span class="progress-msg">${task.message || ''}</span>
          <span class="progress-pct">${pct.toFixed(1)}%</span>
        </div>
        <div class="progress-bar-track">
          <div class="progress-bar-fill" style="width: ${pct}%"></div>
        </div>
      </div>
      ${partsDots}
      ${task.error ? `<div style="margin-top:8px;font-size:0.75rem;color:#f87171;font-family:'JetBrains Mono',monospace;word-break:break-all;">${task.error}</div>` : ''}
    </div>`;
}

function renderHistory(tasks) {
  if (tasks.length === 0) {
    dom.historyBody.innerHTML = `
      <tr><td colspan="5" style="text-align:center;padding:28px;color:var(--text-muted);font-size:0.82rem;">
        Chưa có lịch sử upload
      </td></tr>`;
    return;
  }

  dom.historyBody.innerHTML = tasks.map(t => `
    <tr>
      <td class="td-filename" title="${t.filename}">${t.filename}</td>
      <td><span class="${getBadgeClass(t.status)}">${statusLabel(t.status)}</span></td>
      <td class="td-mono">${t.size ? formatBytes(t.size) : '—'}</td>
      <td class="td-mono">${t.parts_total > 1 ? `${t.parts_total} phần` : '1 phần'}</td>
      <td class="td-mono" style="color:var(--text-muted);">
        ${t.completed_at ? timeSince(t.completed_at) : formatTime(t.created_at)}
      </td>
      <td>
        <button class="btn btn-danger btn-sm" onclick="deleteTask('${t.id}')">✕</button>
      </td>
    </tr>
  `).join('');
}

// ─────────────────────────────────────────────────────────────────
// API CALLS
// ─────────────────────────────────────────────────────────────────

async function submitUrl() {
  const url = dom.urlInput.value.trim();
  if (!url) { showToast('Vui lòng nhập URL video', 'warning'); return; }
  if (!url.startsWith('http://') && !url.startsWith('https://')) {
    showToast('URL phải bắt đầu bằng http:// hoặc https://', 'error');
    return;
  }

  dom.uploadBtn.disabled = true;
  dom.uploadBtn.innerHTML = '<div class="spinner"></div> Đang gửi...';

  try {
    const res = await fetch('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Lỗi không xác định');

    dom.urlInput.value = '';
    showToast(`📋 Task tạo thành công: ${data.filename}`, 'success');
  } catch (e) {
    showToast(`❌ ${e.message}`, 'error');
  } finally {
    dom.uploadBtn.disabled = false;
    dom.uploadBtn.innerHTML = '🚀 Upload Video';
  }
}

async function submitFile(file) {
  if (!file) return;
  if (!file.type.startsWith('video/')) {
    showToast(`File "${file.name}" không phải video`, 'error');
    return;
  }

  showToast(`📤 Đang gửi file: ${file.name}`, 'info');

  const formData = new FormData();
  formData.append('file', file);

  try {
    const res = await fetch('/api/tasks/file', {
      method: 'POST',
      body: formData,
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Lỗi upload file');
    showToast(`✅ File đã nhận: ${data.filename}`, 'success');
  } catch (e) {
    showToast(`❌ ${e.message}`, 'error');
  }
}

async function deleteTask(taskId) {
  try {
    await fetch(`/api/tasks/${taskId}`, { method: 'DELETE' });
    delete state.tasks[taskId];
    renderTasks();
    updateStats();
  } catch (e) {
    showToast('Không xóa được task', 'error');
  }
}

async function cleanupAll() {
  dom.cleanupBtn.disabled = true;
  dom.cleanupBtn.innerHTML = '<div class="spinner"></div>';
  try {
    const res = await fetch('/api/cleanup', { method: 'POST' });
    const data = await res.json();
    showToast(`🗑️ Đã dọn dẹp xong (${data.tasks_removed} tasks)`, 'success');
    // Refresh task list
    const tasksRes = await fetch('/api/tasks');
    const tasksData = await tasksRes.json();
    state.tasks = {};
    tasksData.tasks.forEach(t => { state.tasks[t.id] = t; });
    renderTasks();
    updateStats();
    // Refresh status
    const statusRes = await fetch('/api/status');
    const statusData = await statusRes.json();
    state.diskInfo = { free: statusData.disk_free, total: statusData.disk_total, used: statusData.disk_used };
    renderDiskStats();
  } catch (e) {
    showToast('Lỗi khi dọn dẹp', 'error');
  } finally {
    dom.cleanupBtn.disabled = false;
    dom.cleanupBtn.innerHTML = '🗑️ Dọn dẹp';
  }
}

// ─────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────

function loadConfig() {
  const saved = JSON.parse(localStorage.getItem('videobot-config') || '{}');
  if (saved.channelId)  dom.configChannelId.value = saved.channelId;
  if (saved.maxPartGb)  dom.configMaxPart.value = saved.maxPartGb;
}

function saveConfig() {
  const config = {
    channelId: dom.configChannelId.value.trim(),
    maxPartGb: dom.configMaxPart.value.trim(),
  };
  localStorage.setItem('videobot-config', JSON.stringify(config));
  showToast('💾 Đã lưu cấu hình (cần restart server để áp dụng)', 'success');
}

// ─────────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────────

async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    const data = await res.json();

    if (data.config_errors && data.config_errors.length > 0) {
      dom.warningBanner.classList.add('visible');
      dom.warningText.innerHTML = `
        <strong>⚠️ Cấu hình chưa hoàn chỉnh:</strong><br>
        ${data.config_errors.map(e => `• ${e}`).join('<br>')}
        <br><em>Vui lòng sao chép .env.example thành .env và điền đầy đủ thông tin.</em>
      `;
    } else {
      dom.warningBanner.classList.remove('visible');
    }
  } catch (e) {
    console.warn('Health check failed:', e);
  }
}

// ─────────────────────────────────────────────────────────────────
// DRAG & DROP
// ─────────────────────────────────────────────────────────────────

function initDropzone() {
  const zone = dom.dropzone;

  zone.addEventListener('click', () => dom.fileInput.click());

  zone.addEventListener('dragover', (e) => {
    e.preventDefault();
    zone.classList.add('drag-over');
  });

  ['dragleave', 'dragend'].forEach(evt => {
    zone.addEventListener(evt, () => zone.classList.remove('drag-over'));
  });

  zone.addEventListener('drop', (e) => {
    e.preventDefault();
    zone.classList.remove('drag-over');
    const file = e.dataTransfer.files[0];
    if (file) submitFile(file);
  });

  dom.fileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file) {
      submitFile(file);
      dom.fileInput.value = ''; // Reset để có thể chọn lại cùng file
    }
  });
}

// ─────────────────────────────────────────────────────────────────
// EVENT LISTENERS
// ─────────────────────────────────────────────────────────────────

function initEventListeners() {
  // Upload URL
  dom.uploadBtn.addEventListener('click', submitUrl);
  dom.urlInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') submitUrl();
  });

  // Paste URL tiện lợi
  dom.urlInput.addEventListener('paste', () => {
    setTimeout(() => {
      if (dom.urlInput.value.trim().startsWith('http')) {
        dom.uploadBtn.focus();
      }
    }, 50);
  });

  // Config
  dom.configSaveBtn.addEventListener('click', saveConfig);

  // Cleanup
  dom.cleanupBtn.addEventListener('click', cleanupAll);
}

// ─────────────────────────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────────────────────────

async function init() {
  console.log('🚀 VideoBot Dashboard đang khởi tạo...');

  // Load initial config from localStorage
  loadConfig();

  // Load initial system status
  try {
    const [statusRes, tasksRes] = await Promise.all([
      fetch('/api/status'),
      fetch('/api/tasks'),
    ]);
    const statusData = await statusRes.json();
    const tasksData  = await tasksRes.json();

    state.diskInfo = {
      free:  statusData.disk_free,
      total: statusData.disk_total,
      used:  statusData.disk_used,
    };
    renderDiskStats();

    state.tasks = {};
    tasksData.tasks.forEach(t => { state.tasks[t.id] = t; });
    renderTasks();
    updateStats();
  } catch (e) {
    console.warn('Initial load failed:', e);
  }

  // Health check để hiện warning nếu cần
  checkHealth();

  // Init UI components
  initDropzone();
  initEventListeners();

  // Connect WebSocket
  connectWebSocket();

  // Update "time since" labels mỗi 30 giây
  setInterval(renderTasks, 30_000);
}

// Start!
document.addEventListener('DOMContentLoaded', init);
