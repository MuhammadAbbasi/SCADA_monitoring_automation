async function updateDashboard() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();

        // Update Macro Health
        document.getElementById('total-inv').textContent = data.macro_health.total_inverters || 0;
        document.getElementById('online-inv').textContent = data.macro_health.online || 0;
        document.getElementById('tripped-inv').textContent = data.macro_health.tripped || 0;
        document.getElementById('comms-lost').textContent = data.macro_health.comms_lost || 0;
        document.getElementById('last-sync').textContent = `Last Sync: ${data.last_updated || '--:--'}`;

        // Update Active Alerts
        const alertsContainer = document.getElementById('alerts-container');
        if (data.anomalies && data.anomalies.length > 0) {
            alertsContainer.innerHTML = data.anomalies.map(alert => `
                <div class="alert-item severity-${alert.severity}">
                    <div>
                        <div class="alert-main">${alert.inverter}: ${alert.type}</div>
                        <div class="alert-sub">${alert.details}</div>
                    </div>
                </div>
            `).join('');
        } else {
            alertsContainer.innerHTML = '<p class="placeholder-text" style="color: var(--success-color)">No active anomalies. Plant operating normally.</p>';
        }

        // Update History
        const historyBody = document.getElementById('history-body');
        if (data.historical_alarms && data.historical_alarms.length > 0) {
            historyBody.innerHTML = data.historical_alarms.reverse().slice(0, 50).map(alarm => `
                <tr>
                    <td>${alarm.timestamp}</td>
                    <td>${alarm.inverter}</td>
                    <td>${alarm.type}</td>
                    <td><span class="severity-tag tag-${alarm.severity}">${alarm.severity}</span></td>
                    <td>${alarm.details || ''}</td>
                </tr>
            `).join('');
        }

    } catch (error) {
        console.error('Error fetching dashboard data:', error);
    }
}

// Initial update and periodic polling (every 10 seconds)
updateDashboard();
setInterval(updateDashboard, 10000);
