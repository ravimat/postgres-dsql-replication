/**
 * CDC Dashboard — Main Application
 * Fetches metrics from API Gateway → Lambda → CloudWatch
 */

const CDC = (() => {
    // Configuration — set via environment or inline during deployment
    const CONFIG = {
        apiBaseUrl: window.CDC_API_URL || '/api', // API Gateway endpoint
        refreshInterval: 15000, // 15 seconds
        metricPeriod: 3600, // 1 hour of history
    };

    let charts = {};
    let refreshTimer = null;

    // ─── Initialization ───────────────────────────────────────────────
    function init() {
        setupNavigation();
        initCharts();
        refresh();
        startAutoRefresh();
    }

    // ─── Navigation ───────────────────────────────────────────────────
    function setupNavigation() {
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const page = link.dataset.page;
                switchPage(page);
            });
        });
    }

    function switchPage(pageName) {
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));

        const link = document.querySelector(`[data-page="${pageName}"]`);
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
            const response = await fetch(`${CONFIG.apiBaseUrl}/health`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
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
        }

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

        tbody.innerHTML = tables.map(t => `
            <tr>
                <td><code>${t.name}</code></td>
                <td><span class="badge badge-${t.status === 'active' ? 'green' : t.status === 'lagging' ? 'orange' : 'red'}">${t.status}</span></td>
                <td>${t.lastEvent ? new Date(t.lastEvent).toLocaleTimeString() : '--'}</td>
                <td>${formatNumber(t.eventsApplied || 0)}</td>
                <td>${formatBytes(t.lag || 0)}</td>
                <td>${t.errors || 0}</td>
            </tr>
        `).join('');
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
        setText('infoCluster', health.cluster || '--');
        setText('infoService', health.service || '--');
        setText('infoTaskDef', health.taskDefinition || '--');
        setText('infoUptime', health.uptime || '--');
        setText('infoRegion', health.region || '--');
    }

    // ─── Configuration ────────────────────────────────────────────────
    async function saveConfig() {
        const payload = {
            batch_size: parseInt(document.getElementById('cfgBatchSize').value, 10),
            conflict_mode: document.getElementById('cfgConflictMode').value,
        };

        try {
            const response = await fetch(`${CONFIG.apiBaseUrl}/config`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            showToast('Configuration saved successfully', 'success');
        } catch (err) {
            console.error('Failed to save config:', err);
            showToast('Failed to save configuration', 'error');
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

    // ─── Public API ───────────────────────────────────────────────────
    return { init, refresh, saveConfig };
})();

// Boot
document.addEventListener('DOMContentLoaded', CDC.init);
