import React, { useState, useEffect } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faCalendarAlt, faExclamationTriangle, faCheckCircle, faSpinner, faClock, faLink, faGamepad } from '@fortawesome/free-solid-svg-icons';
import './MonitoringDashboard.css';

const API_URL = process.env.REACT_APP_API_URL || 'http://161.97.78.253:5000/api';

const MonitoringDashboard = () => {
    const [stats, setStats] = useState({
        scripts: {
            running: [],
            failed: [],
            nextToRun: [],
            completed: []
        },
        newGames: {
            adult: 0,
            software: 0,
            aio: 0,
            vr: 0
        },
        invalidated: {
            total: 0,
            percentage: 0
        },
        newMatches: {
            adult: 0,
            software: 0,
            aio: 0,
            vr: 0
        },
        recentGames: []
    });

    useEffect(() => {
        // Fetch initial data
        fetchMonitoringData();
        
        // Set up polling interval
        const interval = setInterval(fetchMonitoringData, 30000); // Update every 30 seconds
        
        return () => clearInterval(interval);
    }, []);

    const fetchMonitoringData = async () => {
        try {
            // Fetch script status
            const tasksResponse = await fetch(`${API_URL}/task-status`);
            const tasksData = await tasksResponse.json();
            
            // Fetch scheduled tasks
            const schedulesResponse = await fetch(`${API_URL}/schedules`);
            const schedulesData = await schedulesResponse.json();
            
            // Fetch stats data (this endpoint needs to be implemented in the backend)
            const statsResponse = await fetch(`${API_URL}/monitoring-stats`);
            const statsData = await statsResponse.json();
            
            // Fetch recent games (this endpoint needs to be implemented in the backend)
            const recentGamesResponse = await fetch(`${API_URL}/recent-games`);
            const recentGamesData = await recentGamesResponse.json();
            
            // Update state with fetched data
            setStats({
                scripts: {
                    running: tasksData.filter(task => task.status === 'running'),
                    failed: tasksData.filter(task => task.status === 'error'),
                    nextToRun: schedulesData.slice(0, 3), // Next 3 scheduled tasks
                    completed: tasksData.filter(task => task.status === 'finished').slice(0, 5) // Last 5 completed tasks
                },
                newGames: statsData.newGames,
                invalidated: statsData.invalidated,
                newMatches: statsData.newMatches,
                recentGames: recentGamesData
            });
        } catch (error) {
            console.error('Error fetching monitoring data:', error);
        }
    };

    return (
        <div className="monitoring-dashboard">
            <h2>Monitoring Dashboard</h2>
            
            <div className="dashboard-grid">
                {/* Scripts Status Section */}
                <div className="dashboard-card scripts-status">
                    <h3>Scripts Status</h3>
                    <div className="scripts-grid">
                        <div className="script-status-card running">
                            <h4><FontAwesomeIcon icon={faSpinner} spin /> Running</h4>
                            {stats.scripts.running.length > 0 ? (
                                <ul>
                                    {stats.scripts.running.map(task => (
                                        <li key={task.id}>{task.scripts.join(', ')}</li>
                                    ))}
                                </ul>
                            ) : (
                                <p>No scripts currently running</p>
                            )}
                        </div>
                        
                        <div className="script-status-card failed">
                            <h4><FontAwesomeIcon icon={faExclamationTriangle} /> Failed</h4>
                            {stats.scripts.failed.length > 0 ? (
                                <ul>
                                    {stats.scripts.failed.map(task => (
                                        <li key={task.id}>{task.scripts.join(', ')}</li>
                                    ))}
                                </ul>
                            ) : (
                                <p>No failed scripts</p>
                            )}
                        </div>
                        
                        <div className="script-status-card next">
                            <h4><FontAwesomeIcon icon={faClock} /> Next to Run</h4>
                            {stats.scripts.nextToRun.length > 0 ? (
                                <ul>
                                    {stats.scripts.nextToRun.map(schedule => (
                                        <li key={schedule.id}>
                                            {schedule.scripts.join(', ')}
                                            <div className="schedule-time">
                                                <FontAwesomeIcon icon={faCalendarAlt} />
                                                {new Date(schedule.next_run).toLocaleString()}
                                            </div>
                                        </li>
                                    ))}
                                </ul>
                            ) : (
                                <p>No scheduled scripts</p>
                            )}
                        </div>
                        
                        <div className="script-status-card completed">
                            <h4><FontAwesomeIcon icon={faCheckCircle} /> Recently Completed</h4>
                            {stats.scripts.completed.length > 0 ? (
                                <ul>
                                    {stats.scripts.completed.map(task => (
                                        <li key={task.id}>{task.scripts.join(', ')}</li>
                                    ))}
                                </ul>
                            ) : (
                                <p>No recently completed scripts</p>
                            )}
                        </div>
                    </div>
                </div>
                
                {/* Stats Section */}
                <div className="dashboard-card stats">
                    <h3>Statistics</h3>
                    <div className="stats-grid">
                        <div className="stat-card new-games">
                            <h4><FontAwesomeIcon icon={faGamepad} /> New Games</h4>
                            <div className="stat-grid">
                                <div>
                                    <span className="stat-label">Adult:</span>
                                    <span className="stat-value">{stats.newGames.adult}</span>
                                </div>
                                <div>
                                    <span className="stat-label">Software:</span>
                                    <span className="stat-value">{stats.newGames.software}</span>
                                </div>
                                <div>
                                    <span className="stat-label">AIO:</span>
                                    <span className="stat-value">{stats.newGames.aio}</span>
                                </div>
                                <div>
                                    <span className="stat-label">VR:</span>
                                    <span className="stat-value">{stats.newGames.vr}</span>
                                </div>
                            </div>
                        </div>
                        
                        <div className="stat-card invalidated">
                            <h4><FontAwesomeIcon icon={faExclamationTriangle} /> Invalidated Games</h4>
                            <div className="stat-grid">
                                <div>
                                    <span className="stat-label">Total:</span>
                                    <span className="stat-value">{stats.invalidated.total}</span>
                                </div>
                                <div>
                                    <span className="stat-label">Link Integration:</span>
                                    <span className="stat-value">{stats.invalidated.percentage}%</span>
                                </div>
                            </div>
                        </div>
                        
                        <div className="stat-card new-matches">
                            <h4><FontAwesomeIcon icon={faLink} /> New Matches</h4>
                            <div className="stat-grid">
                                <div>
                                    <span className="stat-label">Adult:</span>
                                    <span className="stat-value">{stats.newMatches.adult}</span>
                                </div>
                                <div>
                                    <span className="stat-label">Software:</span>
                                    <span className="stat-value">{stats.newMatches.software}</span>
                                </div>
                                <div>
                                    <span className="stat-label">AIO:</span>
                                    <span className="stat-value">{stats.newMatches.aio}</span>
                                </div>
                                <div>
                                    <span className="stat-label">VR:</span>
                                    <span className="stat-value">{stats.newMatches.vr}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                {/* Recent Games Section */}
                <div className="dashboard-card recent-games">
                    <h3>Recent Games</h3>
                    <div className="recent-games-grid">
                        {stats.recentGames.length > 0 ? (
                            stats.recentGames.map(game => (
                                <div key={game.title} className="game-card">
                                    {game.image && <img src={game.image} alt={game.title} />}
                                    <div className="game-info">
                                        <h4>{game.title}</h4>
                                        <p className="game-date">{new Date(game.uploadDate).toLocaleDateString()}</p>
                                        <p className="game-category">{game.category}</p>
                                    </div>
                                </div>
                            ))
                        ) : (
                            <p>No recent games available</p>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MonitoringDashboard;