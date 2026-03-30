async function updateDashboard() {
    try {
        const response = await fetch('/api/status');
        const timeSeriesData = await response.json();

        const timestamps = Object.keys(timeSeriesData).sort();
        if (timestamps.length === 0) {
            console.warn("No data available for today.");
            return;
        }

        const latestTs = timestamps[timestamps.length - 1];
        const data = timeSeriesData[latestTs];

        // Update Macro Health
        document.getElementById('total-inv').textContent = data.macro_health.total_inverters || 0;
        document.getElementById('online-inv').textContent = data.macro_health.online || 0;
        document.getElementById('tripped-inv').textContent = data.macro_health.tripped || 0;
        document.getElementById('comms-lost').textContent = data.macro_health.comms_lost || 0;
        document.getElementById('last-sync').textContent = `Last Sync: ${latestTs.split(' ')[1]}`;

        // Update File Ingestion Status
        if (data.file_statuses) {
            Object.entries(data.file_statuses).forEach(([fileKey, statusObj]) => {
                const card = document.getElementById(`file-${fileKey}`);
                if (!card) return;
                const indicator = card.querySelector('.file-indicator');
                const statusText = card.querySelector('.file-status-text');
                indicator.className = `file-indicator ${statusObj.status}`;
                if (statusObj.status === 'success') {
                    card.classList.remove('card-pending');
                    if (statusText) statusText.textContent = statusObj.timestamp || '';
                } else {
                    card.classList.add('card-pending');
                    if (statusText) statusText.textContent = 'Awaiting Data';
                }
            });
        }

        // Update Inverter Health Matrix
        const healthGrid = document.getElementById('inverter-health-grid');
        if (data.inverter_health && Object.keys(data.inverter_health).length > 0) {
            const FLAG_KEYS   = ['pr', 'temp', 'dc_current', 'ac_power'];
            const FLAG_LABELS = ['PR', 'Temp', 'DC', 'AC'];

            healthGrid.innerHTML = Object.entries(data.inverter_health).map(([inv, flags]) => {
                // Shorten "INV TX1-01" → "TX1-01" for compact display
                const shortName = inv.replace('INV ', '');
                const leds = FLAG_KEYS.map((key, i) => {
                    const color = flags[key] || 'grey';
                    return `<span class="led-dot ${color}" title="${FLAG_LABELS[i]}: ${color.toUpperCase()}"></span>`;
                }).join('');
                const overall = flags.overall_status || 'grey';
                return `
                    <div class="inv-card overall-border-${overall}" title="${inv}">
                        <span class="inv-name">${shortName}</span>
                        <div class="inv-leds">${leds}</div>
                        <span class="inv-overall led-dot ${overall}"></span>
                    </div>`;
            }).join('');
        } else if (healthGrid) {
            healthGrid.innerHTML = '<p class="placeholder-text">Waiting for data updates...</p>';
        }

        // Update Active Alerts (from current timestamp)
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

        // Update History (Aggregate all anomalies from the whole day)
        const historyBody = document.getElementById('history-body');
        let allAnomalies = [];
        timestamps.forEach(ts => {
            if (timeSeriesData[ts].anomalies) {
                allAnomalies = allAnomalies.concat(timeSeriesData[ts].anomalies);
            }
        });

        if (allAnomalies.length > 0) {
            historyBody.innerHTML = allAnomalies.reverse().slice(0, 100).map(alarm => `
                <tr>
                    <td>${alarm.timestamp.split(' ')[1]}</td>
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
