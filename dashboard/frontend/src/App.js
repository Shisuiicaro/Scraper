import React, { useState, useEffect, useRef } from 'react';
import './App.css';

const API_URL = 'http://localhost:5000/api';

function App() {
    const [availableScripts, setAvailableScripts] = useState([]);
    const [selectedScripts, setSelectedScripts] = useState([]);
    const [runningTasks, setRunningTasks] = useState([]);
    const [activeTaskDetail, setActiveTaskDetail] = useState(null);
    const outputRef = useRef(null);

    // Fetch initial data
    useEffect(() => {
        fetchAvailableScripts();
        const interval = setInterval(fetchRunningTasks, 2000); // Poll for task status
        return () => clearInterval(interval);
    }, []);

    // Scroll to bottom of output
    useEffect(() => {
        if (outputRef.current) {
            outputRef.current.scrollTop = outputRef.current.scrollHeight;
        }
    }, [activeTaskDetail]);

    const fetchAvailableScripts = async () => {
        try {
            console.log('Fetching scripts from:', `${API_URL}/scripts`);
            const response = await fetch(`${API_URL}/scripts`);
            const data = await response.json();
            console.log('Received scripts:', data);
            setAvailableScripts(data);
        } catch (error) {
            console.error('Error fetching scripts:', error);
        }
    };

    const fetchRunningTasks = async () => {
        try {
            const response = await fetch(`${API_URL}/task-status`);
            setRunningTasks(await response.json());
        } catch (error) {
            console.error('Error fetching tasks:', error);
        }
    };

    const fetchTaskDetail = async (taskId) => {
        try {
            const response = await fetch(`${API_URL}/task-status/${taskId}`);
            const data = await response.json();
            setActiveTaskDetail(data);
            // Keep polling for the active task details
            if (data.status === 'running' || data.status === 'starting') {
                setTimeout(() => fetchTaskDetail(taskId), 1000);
            }
        } catch (error) {
            console.error('Error fetching task details:', error);
        }
    };

    const handleScriptSelection = (script) => {
        setSelectedScripts(prev => [...prev, script]);
    };

    const handleRemoveScript = (index) => {
        setSelectedScripts(prev => prev.filter((_, i) => i !== index));
    };

    const handleRunSequence = async () => {
        if (selectedScripts.length === 0) return;
        try {
            const response = await fetch(`${API_URL}/run-task`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ scripts: selectedScripts }),
            });
            const data = await response.json();
            fetchTaskDetail(data.task_id);
            setSelectedScripts([]); // Clear selection after running
        } catch (error) {
            console.error('Error running task sequence:', error);
        }
    };

    const handleStopTask = async (taskId) => {
        try {
            await fetch(`${API_URL}/stop-task/${taskId}`, { method: 'POST' });
            fetchRunningTasks(); // Refresh list
        } catch (error) {
            console.error('Error stopping task:', error);
        }
    };

    const handleViewTask = (taskId) => {
        fetchTaskDetail(taskId);
    };

    return (
        <div className="App">
            <header className="App-header">
                <h1>Script Automation Dashboard</h1>
            </header>
            <main className="container">
                <div className="control-panel">
                    <div className="card script-selector">
                        <h2>Build a Sequence</h2>
                        <p>Select scripts from the list to add them to the execution sequence.</p>
                        <div className="script-list">
                            {availableScripts.map(script => (
                                <button key={script} onClick={() => handleScriptSelection(script)} className="script-item">
                                    {script}
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="card sequence-display">
                        <h2>Execution Sequence</h2>
                        {selectedScripts.length === 0 ? (
                            <p>No scripts selected.</p>
                        ) : (
                            <ol>
                                {selectedScripts.map((script, index) => (
                                    <li key={index}>
                                        <span>{script}</span>
                                        <button onClick={() => handleRemoveScript(index)} className="remove-btn">×</button>
                                    </li>
                                ))}
                            </ol>
                        )}
                        <button onClick={handleRunSequence} disabled={selectedScripts.length === 0} className="run-btn">
                            Run Sequence
                        </button>
                    </div>
                </div>

                <div className="card task-monitor">
                    <h2>Active & Recent Tasks</h2>
                    <ul>
                        {runningTasks.map(task => (
                            <li key={task.id} className={`task-item-status ${task.status}`}>
                                <span><strong>ID:</strong> {task.id.substring(0, 8)}...</span>
                                <span><strong>Scripts:</strong> {task.scripts.join(', ')}</span>
                                <span className="status">{task.status}</span>
                                <div>
                                    <button onClick={() => handleViewTask(task.id)}>View</button>
                                    {task.status === 'running' && 
                                        <button onClick={() => handleStopTask(task.id)} className="stop-btn">Stop</button>
                                    }
                                </div>
                            </li>
                        ))}
                    </ul>
                </div>

                {activeTaskDetail && (
                    <div className="card output-viewer">
                        <h2>Task Output: {activeTaskDetail.id.substring(0, 8)}... ({activeTaskDetail.status})</h2>
                        <pre ref={outputRef} className="output-log">
                            {activeTaskDetail.output.join('')}
                        </pre>
                        <button onClick={() => setActiveTaskDetail(null)} className="close-btn">Close</button>
                    </div>
                )}
            </main>
        </div>
    );
}

export default App;
