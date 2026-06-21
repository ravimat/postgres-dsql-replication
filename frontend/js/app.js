/**
 * CDC Dashboard — Main Application
 * Fetches metrics from API Gateway → Lambda → CloudWatch
 */

const CDC = (() => {
    // Configuration — set via environment or inline during deployment
    const CONFIG = {
        apiBaseUrl: window.CDC_API_URL || '/api', // API Gateway endpoint
        refreshInterval: parseInt(localStorage.getItem('cdc_refresh_interval') || '5') * 1000,
        metricPeriod: 3600, // 1 hour of history
    };

    let charts = {};
    let refreshTimer = null;

    // ─── Initialization ───────────────────────────────────────────────
    function init() {
        setupNavigation();
        loadConnectionSettings();
        try { initCharts(); } catch(e) { console.warn('Charts not available:', e.message); }
        refresh();
        startAutoRefresh();
        loadTableMapping();
    }

    // ─── Navigation ───────────────────────────────────────────────────
    function setupNavigation() {
        document.querySelectorAll('.feature-card').forEach(card => {
            card.addEventListener('click', (e) => {
                e.preventDefault();
                const page = card.dataset.page;
                switchPage(page);
            });
        });
    }

    function switchPage(pageName) {
        document.querySelectorAll('.feature-card').forEach(c => c.classList.remove('active'));
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));

        const link = document.querySelector(`.feature-card[data-page="${pageName}"]`);
        const page = document.getElementById(`page-${pageName}`);

        if (link) link.classList.add('active');
        if (page) page.classList.add('active');
    }

    // ─── Charts Setup ─────────────────────────────────────────────────
    function initCharts() {
        const chartDefaults = {
            responsive: true,
            maintainAspectRatio: true,
            animation: { duration: 300 },
            plugins: {
                legend: { display: false },
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { minute: 'HH:mm' } },
                    grid: { color: 'rgba(45,51,72,0.5)' },
                    ticks: { color: '#8b92a8', font: { size: 10 } },
                },
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(45,51,72,0.5)' },
                    ticks: { color: '#8b92a8', font: { size: 10 } },
                },
            },
        };

        charts.tps = new Chart(document.getElementById('chart-tps'), {
            type: 'line',
            data: { datasets: [{ data: [], borderColor: '#3b82f6', borderWidth: 2, fill: true, backgroundColor: 'rgba(59,130,246,0.08)', pointRadius: 0, tension: 0.3 }] },
            options: { ...chartDefaults },
        });

        charts.lag = new Chart(document.getElementById('chart-lag'), {
            type: 'line',
            data: { datasets: [{ data: [], borderColor: '#f59e0b', borderWidth: 2, fill: true, backgroundColor: 'rgba(245,158,11,0.08)', pointRadius: 0, tension: 0.3 }] },
            options: { ...chartDefaults },
        });

        charts.queue = new Chart(document.getElementById('chart-queue'), {
            type: 'bar',
            data: { datasets: [{ data: [], backgroundColor: 'rgba(139,92,246,0.6)', borderRadius: 3 }] },
            options: { ...chartDefaults },
        });

        charts.failed = new Chart(document.getElementById('chart-failed'), {
            type: 'line',
            data: { datasets: [{ data: [], borderColor: '#ef4444', borderWidth: 2, fill: true, backgroundColor: 'rgba(239,68,68,0.08)', pointRadius: 0, tension: 0.3, stepped: 'after' }] },
            options: { ...chartDefaults },
        });
    }

    // ─── Data Fetching ────────────────────────────────────────────────
    async function fetchMetrics() {
        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/metrics`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (err) {
            console.error('Failed to fetch metrics:', err);
            setConnectionStatus('error', 'API Error');
            return null;
        }
    }

    async function fetchHealth() {
        try {
            const url = `${CONFIG.apiBaseUrl}/health`;
            const response = await fetch(url);
            // Accept 503 (unhealthy) — still contains valid JSON status data
            if (response.status !== 200 && response.status !== 503) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (err) {
            console.error('Failed to fetch health:', err);
            return null;
        }
    }

    // ─── Refresh / Update ─────────────────────────────────────────────
    async function refresh() {
        const [metrics, health] = await Promise.all([fetchMetrics(), fetchHealth()]);

        if (metrics) {
            updateKPIs(metrics);
            updateCharts(metrics);
            updateTableStatus(metrics.tables || []);
            updateEventLog(metrics.events || []);
            setConnectionStatus('connected', 'Connected');
        }

        if (health) {
            updateServiceInfo(health);
            if (health.control_state) {
                updateControlStateBadge(health.control_state);
            }
            if (health.tables && health.tables.length) {
                updateTableStatus(health.tables);
            }
            // Show table validation warning if present
            const warningEl = document.getElementById('tableWarning');
            if (warningEl) {
                if (health.table_warning) {
                    warningEl.style.display = 'block';
                    warningEl.textContent = '⚠️ ' + health.table_warning;
                } else {
                    warningEl.style.display = 'none';
                }
            }
        }

        // Always fetch load test table stats (independent of main service)
        fetchLoadTestTables();

        document.getElementById('lastUpdated').textContent =
            `Last updated: ${new Date().toLocaleTimeString()}`;
    }

    function startAutoRefresh() {
        if (refreshTimer) clearInterval(refreshTimer);
        refreshTimer = setInterval(refresh, CONFIG.refreshInterval);
    }

    // ─── KPI Updates ──────────────────────────────────────────────────
    function updateKPIs(metrics) {
        const latest = metrics.latest || {};

        setText('kpi-tps', formatNumber(latest.EventsAppliedPerSecond || 0, 1));
        setText('kpi-lag', formatBytes(latest.ReplicationLagBytes || 0));
        setText('kpi-applied', formatNumber(latest.EventsAppliedTotal || 0));
        setText('kpi-failed', formatNumber(latest.EventsFailed || 0));
        setText('kpi-queue', formatNumber(latest.BatchQueueDepth || 0));
        setText('kpi-lsn', latest.CheckpointLSN || '0/0');
    }

    // ─── Chart Updates ────────────────────────────────────────────────
    function updateCharts(metrics) {
        if (!charts.tps) return;  // Charts not initialized
        const timeseries = metrics.timeseries || {};

        if (timeseries.EventsAppliedPerSecond) {
            charts.tps.data.datasets[0].data = toChartData(timeseries.EventsAppliedPerSecond);
            charts.tps.update('none');
        }

        if (timeseries.ReplicationLagBytes) {
            charts.lag.data.datasets[0].data = toChartData(timeseries.ReplicationLagBytes);
            charts.lag.update('none');
        }

        if (timeseries.BatchQueueDepth) {
            charts.queue.data.datasets[0].data = toChartData(timeseries.BatchQueueDepth);
            charts.queue.update('none');
        }

        if (timeseries.EventsFailed) {
            charts.failed.data.datasets[0].data = toChartData(timeseries.EventsFailed);
            charts.failed.update('none');
        }
    }

    function toChartData(datapoints) {
        return datapoints.map(dp => ({
            x: new Date(dp.timestamp),
            y: dp.value,
        }));
    }

    // ─── Table Status ─────────────────────────────────────────────────
    function updateTableStatus(tables) {
        const tbody = document.getElementById('tableStatusBody');
        if (!tables.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No tables being replicated</td></tr>';
            return;
        }

        // Split into sample tables and user tables
        const userTables = tables.filter(t => !t.name.includes('sample_'));
        const sampleTables = tables.filter(t => t.name.includes('sample_'));

        // Update main table status (user tables only)
        if (userTables.length) {
            tbody.innerHTML = userTables.map(t => renderTableRow(t)).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No user tables being replicated yet</td></tr>';
        }

        // Update sample table status on Load Test page
        const sampleTbody = document.getElementById('sampleTableStatusBody');
        if (sampleTbody) {
            fetchLoadTestTables();
        }
    }

    async function fetchLoadTestTables() {
        const sampleTbody = document.getElementById('sampleTableStatusBody');
        if (!sampleTbody) return;
        try {
            const resp = await fetch(`${CONFIG.apiBaseUrl}/loadtest/tables`);
            if (resp.status === 200 || resp.status === 503) {
                const data = await resp.json();
                if (data.tables && data.tables.length) {
                    sampleTbody.innerHTML = data.tables.map(t => renderTableRow(t)).join('');
                    localStorage.setItem('cdc_last_loadtest', JSON.stringify(data.tables));
                } else {
                    const stored = localStorage.getItem('cdc_last_loadtest');
                    if (stored) {
                        try {
                            sampleTbody.innerHTML = JSON.parse(stored).map(t => renderTableRow(t)).join('');
                        } catch(e) {}
                    }
                }
            }
        } catch(e) {
            const stored = localStorage.getItem('cdc_last_loadtest');
            if (stored) {
                try {
                    sampleTbody.innerHTML = JSON.parse(stored).map(t => renderTableRow(t)).join('');
                } catch(e2) {}
            }
        }
    }

    function renderTableRow(t) {
        const lastUpdated = t.lastEvent ? new Date(t.lastEvent * 1000).toLocaleString() : '--';
        return `
            <tr>
                <td><code>${t.name}</code></td>
                <td><span class="badge badge-${t.errors > 0 ? 'red' : t.eventsApplied > 0 ? 'green' : 'orange'}">${t.errors > 0 ? 'errors' : t.eventsApplied > 0 ? 'active' : 'idle'}</span></td>
                <td>${lastUpdated}</td>
                <td>${formatNumber(t.eventsApplied || 0)}</td>
                <td>${t.operations ? Object.entries(t.operations).map(([k,v]) => k + ':' + v).join(' ') : '--'}</td>
                <td>${t.errors || 0}</td>
            </tr>
        `;
    }

    // ─── Event Log ────────────────────────────────────────────────────
    function updateEventLog(events) {
        const container = document.getElementById('eventLog');
        if (!events.length) {
            container.innerHTML = '<div class="empty-state">No recent events</div>';
            return;
        }

        container.innerHTML = events.slice(0, 100).map(e => `
            <div class="event-item">
                <span class="event-time">${new Date(e.timestamp).toLocaleTimeString()}</span>
                <span class="event-level ${e.level}">${e.level}</span>
                <span class="event-message">${escapeHtml(e.message)}</span>
            </div>
        `).join('');
    }

    // ─── Service Info ─────────────────────────────────────────────────
    function updateServiceInfo(health) {
        setText('infoPlugin', health.plugin || '--');
        setText('infoConflict', health.conflict_mode || '--');
        setText('infoInstance', health.instance_id || '--');
        const uptime = health.uptime_seconds;
        if (uptime != null) {
            const h = Math.floor(uptime / 3600);
            const m = Math.floor((uptime % 3600) / 60);
            setText('infoUptime', `${h}h ${m}m`);
        }
        setText('infoRegion', health.region || 'us-east-1');

        // Populate ALL config fields from health endpoint (always update to reflect current .env)
        const srcInput = document.getElementById('cfgSourceDSN');
        const tgtInput = document.getElementById('cfgTargetDSN');
        if (srcInput && health.source_dsn) {
            srcInput.value = health.source_dsn;
        }
        if (tgtInput && health.target_endpoint) {
            tgtInput.value = health.target_endpoint;
        }
        const batchInput = document.getElementById('cfgBatchSize');
        const conflictInput = document.getElementById('cfgConflictMode');
        const slotInput = document.getElementById('cfgSlotName');
        const modeInput = document.getElementById('cfgReplicationMode');
        if (batchInput && health.batch_size) batchInput.value = health.batch_size;
        if (conflictInput && health.conflict_mode) conflictInput.value = health.conflict_mode;
        if (slotInput && health.slot_name) slotInput.value = health.slot_name;
        if (modeInput && health.replication_mode) modeInput.value = health.replication_mode;
    }

    // ─── Configuration ────────────────────────────────────────────────
    async function saveConfig() {
        const payload = {
            batch_size: parseInt(document.getElementById('cfgBatchSize').value, 10),
            conflict_mode: document.getElementById('cfgConflictMode').value,
            slot_name: document.getElementById('cfgSlotName').value.trim(),
            replication_mode: document.getElementById('cfgReplicationMode').value,
        };

        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/config`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const msg = document.getElementById('configResult');
            if (msg) {
                msg.style.display = 'block';
                msg.className = 'validation-message success';
                msg.textContent = '✓ Configuration saved! Service restarting with new settings.';
            }
        } catch (err) {
            console.error('Failed to save config:', err);
            const msg = document.getElementById('configResult');
            if (msg) {
                msg.style.display = 'block';
                msg.className = 'validation-message error';
                msg.textContent = '✗ Failed to save: ' + err.message;
            }
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────
    function setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }

    function setConnectionStatus(state, text) {
        const container = document.getElementById('connectionStatus');
        const dot = container.querySelector('.status-dot');
        const label = container.querySelector('.status-text');
        dot.className = `status-dot ${state}`;
        label.textContent = text;
    }

    function formatNumber(n, decimals = 0) {
        if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
        if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
        return Number(n).toFixed(decimals);
    }

    function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function showToast(message, type = 'info') {
        const existing = document.querySelector('.toast');
        if (existing) existing.remove();

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        document.body.appendChild(toast);

        requestAnimationFrame(() => toast.classList.add('show'));
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }


    // ─── Load Test ───────────────────────────────────────────────────────
    let loadTestTimer = null;
    let loadTestCommandId = null;

    async function startLoadTest() {
        const config = {
            duration: parseInt(document.getElementById('ltDuration').value, 10),
            orders_per_sec: parseInt(document.getElementById('ltOrdersPerSec').value, 10),
            threads: parseInt(document.getElementById('ltThreads').value, 10),
            mode: document.getElementById('ltMode').value,
        };

        // Update UI
        document.getElementById('btnStartTest').style.display = 'none';
        document.getElementById('btnStopTest').style.display = 'inline-flex';
        const progressEl = document.getElementById('loadtestProgress');
        if (progressEl) progressEl.style.display = 'block';
        const resultsEl = document.getElementById('loadtestResults');
        if (resultsEl) resultsEl.style.display = 'none';
        document.getElementById('loadtestStatusBadge').innerHTML = '<span class="badge badge-blue">Running</span>';
        clearLoadTestLog();
        appendLog('Starting load test...', 'highlight');
        appendLog(`Config: duration=${config.duration}s, orders/sec=${config.orders_per_sec}, threads=${config.threads}, mode=${config.mode}`, 'highlight');

        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/loadtest`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config),
            });

            if (!response.ok) {
                const errData = await response.json().catch(() => ({}));
                throw new Error(errData.error || `HTTP ${response.status}`);
            }
            const result = await response.json();
            loadTestCommandId = result.command_id;
            appendLog(`Test started. Command ID: ${loadTestCommandId}`, 'success');

            // Start polling for status
            loadTestTimer = setInterval(pollLoadTestStatus, 5000);
        } catch (err) {
            appendLog(`Failed to start: ${err.message}`, 'error');
            resetLoadTestUI();
        }
    }

    async function stopLoadTest() {
        appendLog('Stopping load test...', 'highlight');
        if (loadTestTimer) {
            clearInterval(loadTestTimer);
            loadTestTimer = null;
        }
        resetLoadTestUI();
    }

    async function pollLoadTestStatus() {
        if (!loadTestCommandId) return;

        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/loadtest/status?command_id=${loadTestCommandId}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();

            if (data.output) {
                updateLoadTestLog(data.output);
            }

            if (data.progress) {
                updateLoadTestProgress(data.progress);
            }

            if (data.status === 'complete') {
                clearInterval(loadTestTimer);
                loadTestTimer = null;
                appendLog('\n✓ Load test completed!', 'success');
                document.getElementById('loadtestStatusBadge').innerHTML = '<span class="badge badge-green">Complete</span>';
                resetLoadTestUI();
                if (data.results) {
                    showLoadTestResults(data.results);
                }
            } else if (data.status === 'failed') {
                clearInterval(loadTestTimer);
                loadTestTimer = null;
                appendLog(`\n✗ Load test failed: ${data.error || 'Unknown error'}`, 'error');
                document.getElementById('loadtestStatusBadge').innerHTML = '<span class="badge badge-red">Failed</span>';
                resetLoadTestUI();
            }
        } catch (err) {
            appendLog(`Poll error: ${err.message}`, 'error');
        }
    }

    function updateLoadTestProgress(progress) {
        if (progress.elapsed) setText('lt-elapsed', progress.elapsed);
        if (progress.orders_placed !== undefined) setText('lt-orders', formatNumber(progress.orders_placed));
        if (progress.total_dml_operations !== undefined) setText('lt-dml', formatNumber(progress.total_dml_operations));
        if (progress.effective_tps !== undefined) setText('lt-tps', formatNumber(progress.effective_tps, 1));
        if (progress.errors !== undefined) setText('lt-errors', formatNumber(progress.errors));
    }

    function showLoadTestResults(results) {
        const el = document.getElementById('loadtestResults');
        if (el) el.style.display = 'block';
        const tbody = document.getElementById('ltIntegrityBody');

        if (results.integrity) {
            const tables = Object.entries(results.integrity);
            tbody.innerHTML = tables.map(([table, data]) => `
                <tr>
                    <td><code>${table}</code></td>
                    <td>${formatNumber(data.source || 0)}</td>
                    <td>${formatNumber(data.target || 0)}</td>
                    <td><span class="badge badge-${data.match ? 'green' : 'red'}">${data.match ? 'PASS' : 'FAIL'}</span></td>
                    <td>${data.diff || 0}</td>
                </tr>
            `).join('');
        }
    }

    function clearLoadTestLog() {
        document.getElementById('loadtestLog').innerHTML = '';
    }

    function appendLog(message, className) {
        const container = document.getElementById('loadtestLog');
        const line = document.createElement('div');
        line.className = `log-line ${className || ''}`;
        line.textContent = message;
        container.appendChild(line);
        container.scrollTop = container.scrollHeight;
    }

    function updateLoadTestLog(output) {
        const container = document.getElementById('loadtestLog');
        const lines = output.split('\n').filter(l => l.trim());
        // Only show last 100 lines
        const recentLines = lines.slice(-100);
        container.innerHTML = recentLines.map(line => {
            let cls = '';
            if (line.includes('ERROR') || line.includes('✗')) cls = 'error';
            else if (line.includes('✓') || line.includes('complete')) cls = 'success';
            else if (line.includes('[') && line.includes(']')) cls = 'highlight';
            return `<div class="log-line ${cls}">${escapeHtml(line)}</div>`;
        }).join('');
        container.scrollTop = container.scrollHeight;
    }

    function resetLoadTestUI() {
        document.getElementById('btnStartTest').style.display = 'inline-flex';
        document.getElementById('btnStopTest').style.display = 'none';
    }

    // ─── Public API ───────────────────────────────────────────────────
    // ─── Connection Settings ─────────────────────────────────────────
    function loadConnectionSettings() {
        const apiUrl = localStorage.getItem('cdc_api_url');
        const interval = localStorage.getItem('cdc_refresh_interval');

        if (apiUrl) CONFIG.apiBaseUrl = apiUrl;
        if (interval) CONFIG.refreshInterval = parseInt(interval) * 1000;

        // Populate form fields if on config page
        const apiInput = document.getElementById('cfgApiUrl');
        const intervalInput = document.getElementById('cfgRefreshInterval');
        if (apiInput) apiInput.value = CONFIG.apiBaseUrl;
        if (intervalInput) intervalInput.value = CONFIG.refreshInterval / 1000;
    }

    function saveConnectionSettings() {
        const apiUrl = document.getElementById('cfgApiUrl').value.trim();
        const interval = document.getElementById('cfgRefreshInterval').value;

        if (apiUrl) {
            CONFIG.apiBaseUrl = apiUrl;
            localStorage.setItem('cdc_api_url', apiUrl);
        }
        CONFIG.refreshInterval = parseInt(interval) * 1000;
        localStorage.setItem('cdc_refresh_interval', interval);

        // Restart polling with new settings
        if (refreshTimer) clearInterval(refreshTimer);
        startAutoRefresh();
        refresh();

        alert('Connection settings saved! Dashboard will now poll: ' + apiUrl + '/health');
    }


    // ─── Replication Control ─────────────────────────────────────────
    async function controlReplication(action) {
        const stateMap = {start: 'running', resume: 'running', pause: 'paused', stop: 'stopped'};
        const newState = stateMap[action] || action;
        try {
            const url = CONFIG.apiBaseUrl + '/control';
            const resp = await fetch(url, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({state: newState}),
            });
            const data = await resp.json();
            if (!resp.ok) {
                alert('Error: ' + (data.error || 'Request failed'));
            } else if (data.status === 'ok' || data.state) {
                updateControlBadge(newState);
                alert('Replication state changed to: ' + newState);
            } else {
                alert('Error: ' + (data.error || 'Unknown'));
            }
        } catch (e) {
            alert('Failed to change state: ' + e.message);
        }
    }

    function updateControlBadge(state) {
        const badge = document.getElementById('controlStateBadge');
        if (!badge) return;
        badge.textContent = state.charAt(0).toUpperCase() + state.slice(1);
        badge.className = 'badge badge-' + state;
    }

    // ─── Table Mapping ───────────────────────────────────────────────
    function validateTableMapping() {
        const textarea = document.getElementById('rulesTextarea');
        const msgDiv = document.getElementById('validationMessage');
        if (!textarea) return false;
        if (msgDiv) msgDiv.style.display = 'block';
        try {
            const data = JSON.parse(textarea.value);
            if (!data.rules || !Array.isArray(data.rules)) {
                throw new Error('JSON must have a "rules" array');
            }
            for (const rule of data.rules) {
                if (rule['rule-type'] !== 'selection') continue;
                if (!rule['object-locator'] || !rule['object-locator']['schema-name'] || !rule['object-locator']['table-name']) {
                    throw new Error('Each rule must have object-locator with schema-name and table-name');
                }
                if (!['include', 'exclude'].includes(rule['rule-action'])) {
                    throw new Error('rule-action must be "include" or "exclude"');
                }
            }
            const count = data.rules.filter(r => r['rule-type'] === 'selection').length;
            msgDiv.className = 'validation-message success';
            msgDiv.textContent = '✓ Valid — ' + count + ' selection rule(s) found';
            return true;
        } catch (e) {
            msgDiv.className = 'validation-message error';
            msgDiv.textContent = '✗ ' + e.message;
            return false;
        }
    }

    async function applyTableMapping() {
        if (!validateTableMapping()) return;
        const rulesJson = document.getElementById('rulesTextarea').value;
        try {
            const resp = await fetch(CONFIG.apiBaseUrl + '/table-mapping', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: rulesJson,
            });
            const data = await resp.json();
            if (data.status === 'applied') {
                alert('Table mapping rules applied! (' + (data.rules_count || '?') + ' rules). Changes take effect immediately.');
                loadTableMapping();
            } else {
                alert('Error: ' + (data.error || JSON.stringify(data)));
            }
        } catch (e) {
            alert('Failed to apply rules: ' + e.message);
        }
    }

    async function loadTableMapping() {
        const display = document.getElementById('currentRulesDisplay');
        if (!display) return;
        try {
            const resp = await fetch(CONFIG.apiBaseUrl + '/table-mapping');
            const data = await resp.json();
            if (data.rules) {
                display.textContent = JSON.stringify(data, null, 2);
            } else if (data.content) {
                display.textContent = data.content;
            } else {
                display.textContent = JSON.stringify(data, null, 2);
            }
        } catch (e) {
            display.textContent = 'No rules configured (replicating ALL tables)';
        }
    }

    function uploadRulesFile() {
        const input = document.getElementById('rulesFileInput');
        if (!input || !input.files[0]) return;
        const reader = new FileReader();
        reader.onload = function(e) {
        document.getElementById('rulesTextarea').value = e.target.result;
            validateTableMapping();
        };
        reader.readAsText(input.files[0]);
    }


    function friendlyError(raw) {
        if (!raw) return 'Unknown error';
        const lower = raw.toLowerCase();
        if (lower.includes('could not connect') || lower.includes('connection refused') || lower.includes('timeout'))
            return 'Database unreachable — check if the instance is running and network/security groups allow access.';
        if (lower.includes('password authentication failed'))
            return 'Authentication failed — check username/password in your Secrets Manager secret.';
        if (lower.includes('no pg_hba.conf entry'))
            return 'Connection rejected by server — SSL may be required or IP not allowed.';
        if (lower.includes('does not exist'))
            return 'Database not found — check the dbname in your secret.';
        if (lower.includes('name or service not known') || lower.includes('could not translate host'))
            return 'Hostname not found — check the host value in your secret.';
        if (lower.includes('operationalerror') || lower.includes('traceback'))
            return 'Connection failed — ensure the database is running and credentials are correct.';
        return raw.length > 150 ? raw.substring(0, 150) + '...' : raw;
    }

    async function testConnectivity() {
        const resultDiv = document.getElementById('connectivityResult');
        const sourceDSN = document.getElementById('cfgSourceDSN').value.trim();
        const targetDSN = document.getElementById('cfgTargetDSN').value.trim();

        if (!sourceDSN && !targetDSN) {
            resultDiv.style.display = 'block';
            resultDiv.className = 'validation-message error';
            resultDiv.textContent = '✗ Enter at least one DSN to test';
            return;
        }

        resultDiv.style.display = 'block';
        resultDiv.className = 'validation-message';
        resultDiv.textContent = '⏳ Testing connectivity...';

        // Save DSN first (so the values are persisted on EC2 before testing)
        try {
            await fetch(`${CONFIG.apiBaseUrl}/dsn`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({source_dsn: sourceDSN, target_dsn: targetDSN}),
            });
        } catch (e) { /* save failed — still proceed with test */ }

        try {
            const resp = await fetch(`${CONFIG.apiBaseUrl}/test-connection`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({source_dsn: sourceDSN, target_dsn: targetDSN}),
            });
            const data = await resp.json();
            if (data.source_ok && data.target_ok) {
                resultDiv.className = 'validation-message success';
                resultDiv.textContent = '✓ Both connections successful! Source: ' + (data.source_version || 'OK') + ' | Target: ' + (data.target_status || 'OK');
            } else {
                resultDiv.className = 'validation-message error';
                let msg = '';
                if (data.source_error) msg += '✗ Source: ' + friendlyError(data.source_error) + '\n';
                if (data.target_error) msg += '✗ Target: ' + friendlyError(data.target_error);
                if (data.source_ok) msg += '✓ Source: ' + (data.source_version || 'OK') + '\n';
                if (data.target_ok) msg += '✓ Target: ' + (data.target_status || 'OK');
                resultDiv.textContent = msg || '✗ Connection failed';
            }
        } catch (e) {
            resultDiv.className = 'validation-message error';
            resultDiv.textContent = '✗ Failed to test: ' + e.message;
        }
    }

    async function saveDSN() {
        const sourceDSN = document.getElementById('cfgSourceDSN').value.trim();
        const targetDSN = document.getElementById('cfgTargetDSN').value.trim();
        const resultDiv = document.getElementById('connectivityResult');

        try {
            const resp = await fetch(`${CONFIG.apiBaseUrl}/dsn`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({source_dsn: sourceDSN, target_dsn: targetDSN}),
            });
            const data = await resp.json();
            if (data.status === 'ok') {
                resultDiv.style.display = 'block';
                resultDiv.className = 'validation-message success';
                resultDiv.textContent = '✓ DSN saved. Service will restart with new connections.';
            } else {
                resultDiv.style.display = 'block';
                resultDiv.className = 'validation-message error';
                resultDiv.textContent = '✗ ' + (data.error || 'Failed to save');
            }
        } catch (e) {
            resultDiv.style.display = 'block';
            resultDiv.className = 'validation-message error';
            resultDiv.textContent = '✗ Failed to save: ' + e.message;
        }
    }

return { init, refresh, saveConfig, saveConnectionSettings, startLoadTest, stopLoadTest, controlReplication, applyTableMapping, validateTableMapping, uploadRulesFile, loadTableMapping, testConnectivity, saveDSN };
})();

// Boot
document.addEventListener('DOMContentLoaded', CDC.init);
    // ─── Table Mapping & Replication Control ──────────────────────────────────────

    async function controlReplication(action) {
        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/control`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action }),
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const result = await response.json();

            // Update badge immediately
            updateControlStateBadge(result.status || action);
            showToast(`Replication ${action}: ${result.status || 'OK'}`, 'success');

            // Refresh to get latest health
            setTimeout(refresh, 1000);
        } catch (err) {
            console.error('Control action failed:', err);
            showToast(`Failed to ${action}: ${err.message}`, 'error');
        }
    }

    function updateControlStateBadge(state) {
        const badge = document.getElementById('controlStateBadge');
        if (!badge) return;

        const stateMap = {
            running: { text: '\u{1F7E2} Running', cls: 'running' },
            paused: { text: '\u{1F7E1} Paused', cls: 'paused' },
            stopped: { text: '\u{1F534} Stopped', cls: 'stopped' },
            start: { text: '\u{1F7E2} Running', cls: 'running' },
            resume: { text: '\u{1F7E2} Running', cls: 'running' },
            pause: { text: '\u{1F7E1} Paused', cls: 'paused' },
            stop: { text: '\u{1F534} Stopped', cls: 'stopped' },
        };

        const info = stateMap[state] || stateMap.running;
        badge.textContent = info.text;
        badge.className = `status-badge ${info.cls}`;

        // Toggle Start/Stop buttons
        const btnStart = document.getElementById('btnStartReplication');
        const btnStop = document.getElementById('btnStopReplication');
        const isRunning = (state === 'running' || state === 'start' || state === 'resume');
        if (btnStart) btnStart.style.display = isRunning ? 'none' : 'inline-flex';
        if (btnStop) btnStop.style.display = isRunning ? 'inline-flex' : 'none';
    }

    function validateTableMapping() {
        const textarea = document.getElementById('rulesTextarea');
        const msgDiv = document.getElementById('validationMessage');
        const text = textarea.value.trim();

        if (!text) {
            showValidationMessage(msgDiv, 'Please enter or upload a rules JSON document.', 'error');
            return false;
        }

        try {
            const parsed = JSON.parse(text);

            if (!parsed.rules || !Array.isArray(parsed.rules)) {
                showValidationMessage(msgDiv, 'Invalid: JSON must contain a "rules" array.', 'error');
                return false;
            }

            // Validate each rule
            for (let i = 0; i < parsed.rules.length; i++) {
                const rule = parsed.rules[i];
                if (rule['rule-type'] !== 'selection') {
                    showValidationMessage(msgDiv, `Rule ${i + 1}: rule-type must be "selection".`, 'error');
                    return false;
                }
                if (!rule['object-locator'] || !rule['object-locator']['schema-name'] || !rule['object-locator']['table-name']) {
                    showValidationMessage(msgDiv, `Rule ${i + 1}: missing object-locator with schema-name and table-name.`, 'error');
                    return false;
                }
                if (!['include', 'exclude'].includes(rule['rule-action'])) {
                    showValidationMessage(msgDiv, `Rule ${i + 1}: rule-action must be "include" or "exclude".`, 'error');
                    return false;
                }
            }

            const includes = parsed.rules.filter(r => r['rule-action'] === 'include').length;
            const excludes = parsed.rules.filter(r => r['rule-action'] === 'exclude').length;
            showValidationMessage(msgDiv, `\u2713 Valid! ${parsed.rules.length} rules (${includes} include, ${excludes} exclude).`, 'success');
            return true;
        } catch (e) {
            showValidationMessage(msgDiv, `Invalid JSON: ${e.message}`, 'error');
            return false;
        }
    }

    async function applyTableMapping() {
        if (!validateTableMapping()) return;

        const textarea = document.getElementById('rulesTextarea');
        const rules = JSON.parse(textarea.value.trim());

        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/table-mapping`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(rules),
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const result = await response.json();
            showToast(`Table mapping applied! ${result.rules_count || ''} rules active.`, 'success');

            // Refresh the active rules display
            loadTableMapping();
        } catch (err) {
            console.error('Failed to apply table mapping:', err);
            showToast(`Failed to apply: ${err.message}`, 'error');
        }
    }

    async function loadTableMapping() {
        const display = document.getElementById('currentRulesDisplay');
        if (!display) return;

        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/table-mapping`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();

            if (data && data.rules && data.rules.length > 0) {
                display.textContent = JSON.stringify(data, null, 2);
            } else {
                display.textContent = 'No rules configured — ALL tables will be replicated.';
            }
        } catch (err) {
            display.textContent = 'Unable to fetch current rules (API unavailable).';
        }
    }

    function uploadRulesFile(event) {
        const file = event.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = function(e) {
            const textarea = document.getElementById('rulesTextarea');
            textarea.value = e.target.result;
            showToast(`Loaded ${file.name}`, 'success');
        };
        reader.onerror = function() {
            showToast('Failed to read file', 'error');
        };
        reader.readAsText(file);
    }

    function showValidationMessage(el, message, type) {
        if (!el) return;
        el.style.display = 'block';
        el.textContent = message;
        el.className = `validation-message ${type}`;
    }

    
