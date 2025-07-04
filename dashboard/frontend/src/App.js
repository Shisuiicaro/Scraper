import React, { useState, useEffect } from 'react';
import './App.css';

const API_URL = 'http://localhost:5001/api';

function App() {
    const [scripts, setScripts] = useState([]);
    const [scheduledJobs, setScheduledJobs] = useState([]);
    const [selectedScript, setSelectedScript] = useState('');
    const [cron, setCron] = useState({ minute: '*', hour: '*', day: '*', month: '*', day_of_week: '*' });

    useEffect(() => {
        fetchScripts();
        fetchScheduledJobs();
        const interval = setInterval(() => {
            fetchScheduledJobs();
        }, 5000); // Refresh jobs every 5 seconds
        return () => clearInterval(interval);
    }, []);

    const fetchScripts = async () => {
        try {
            const response = await fetch(`${API_URL}/scripts`);
            const data = await response.json();
            setScripts(data);
            if (data.length > 0) {
                setSelectedScript(data[0]);
            }
        } catch (error) {
            console.error('Error fetching scripts:', error);
        }
    };

    const fetchScheduledJobs = async () => {
        try {
            const response = await fetch(`${API_URL}/scheduled-jobs`);
            const data = await response.json();
            setScheduledJobs(data);
        } catch (error) {
            console.error('Error fetching scheduled jobs:', error);
        }
    };

    const handleRunScript = async () => {
        if (!selectedScript) return;
        try {
            await fetch(`${API_URL}/run-script`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ script: selectedScript }),
            });
            alert(`Script ${selectedScript} started!`);
        } catch (error) {
            console.error('Error running script:', error);
        }
    };

    const handleScheduleScript = async () => {
        if (!selectedScript) return;
        try {
            const cronString = `${cron.minute} ${cron.hour} ${cron.day} ${cron.month} ${cron.day_of_week}`;
            await fetch(`${API_URL}/schedule-script`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ script: selectedScript, cron: cron }),
            });
            alert(`Script ${selectedScript} scheduled with cron: ${cronString}`);
            fetchScheduledJobs();
        } catch (error) {
            console.error('Error scheduling script:', error);
        }
    };

    const handleCancelJob = async (jobId) => {
        try {
            await fetch(`${API_URL}/cancel-job`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ job_id: jobId }),
            });
            alert(`Job ${jobId} cancelled!`);
            fetchScheduledJobs();
        } catch (error) {
            console.error('Error cancelling job:', error);
        }
    };

    const handleCronChange = (e) => {
        const { name, value } = e.target;
        setCron(prev => ({ ...prev, [name]: value }));
    };

    return (
        <div className="App">
            <header className="App-header">
                <h1>Workspace Control Panel</h1>
            </header>
            <main className="container">
                <div className="card">
                    <h2>Run Script Manually</h2>
                    <select value={selectedScript} onChange={(e) => setSelectedScript(e.target.value)}>
                        {scripts.map(script => (
                            <option key={script} value={script}>{script}</option>
                        ))}
                    </select>
                    <button onClick={handleRunScript}>Run Script</button>
                </div>

                <div className="card">
                    <h2>Schedule Script (Cron)</h2>
                    <select value={selectedScript} onChange={(e) => setSelectedScript(e.target.value)}>
                        {scripts.map(script => (
                            <option key={script} value={script}>{script}</option>
                        ))}
                    </select>
                    <div className="cron-inputs">
                        <input type="text" name="minute" value={cron.minute} onChange={handleCronChange} placeholder="Minute" />
                        <input type="text" name="hour" value={cron.hour} onChange={handleCronChange} placeholder="Hour" />
                        <input type="text" name="day" value={cron.day} onChange={handleCronChange} placeholder="Day" />
                        <input type="text" name="month" value={cron.month} onChange={handleCronChange} placeholder="Month" />
                        <input type="text" name="day_of_week" value={cron.day_of_week} onChange={handleCronChange} placeholder="Day of Week" />
                    </div>
                    <button onClick={handleScheduleScript}>Schedule Script</button>
                </div>

                <div className="card">
                    <h2>Scheduled Jobs</h2>
                    <ul>
                        {scheduledJobs.map(job => (
                            <li key={job.id}>
                                <span>{job.id} - Next run: {new Date(job.next_run_time).toLocaleString()}</span>
                                <button onClick={() => handleCancelJob(job.id)}>Cancel</button>
                            </li>
                        ))}
                    </ul>
                </div>
            </main>
        </div>
    );
}

export default App;
