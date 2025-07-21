import React, { useState } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faTimes, faCalendarAlt } from '@fortawesome/free-solid-svg-icons';
import './ScheduleModal.css';

const ScheduleModal = ({ isOpen, onClose, onSchedule, selectedScripts }) => {
    const [scheduleConfig, setScheduleConfig] = useState({
        enabled: true,
        frequency: 'daily',
        time: '12:00',
        days: ['monday', 'wednesday', 'friday']
    });

    if (!isOpen) return null;

    const handleFrequencyChange = (e) => {
        setScheduleConfig({
            ...scheduleConfig,
            frequency: e.target.value
        });
    };

    const handleTimeChange = (e) => {
        setScheduleConfig({
            ...scheduleConfig,
            time: e.target.value
        });
    };

    const handleDayToggle = (day) => {
        const currentDays = [...scheduleConfig.days];
        if (currentDays.includes(day)) {
            setScheduleConfig({
                ...scheduleConfig,
                days: currentDays.filter(d => d !== day)
            });
        } else {
            setScheduleConfig({
                ...scheduleConfig,
                days: [...currentDays, day]
            });
        }
    };

    const handleSubmit = () => {
        onSchedule(scheduleConfig);
        onClose();
    };

    return (
        <div className="schedule-modal-overlay">
            <div className="schedule-modal">
                <div className="schedule-modal-header">
                    <h3><FontAwesomeIcon icon={faCalendarAlt} /> Schedule Scripts</h3>
                    <button className="close-button" onClick={onClose}>
                        <FontAwesomeIcon icon={faTimes} />
                    </button>
                </div>
                
                <div className="schedule-modal-content">
                    <div className="selected-scripts">
                        <h4>Selected Scripts:</h4>
                        <ul>
                            {selectedScripts.map((script, index) => (
                                <li key={index}>{script}</li>
                            ))}
                        </ul>
                    </div>
                    
                    <div className="schedule-form">
                        <div className="form-group">
                            <label>Frequency:</label>
                            <select 
                                value={scheduleConfig.frequency} 
                                onChange={handleFrequencyChange}
                            >
                                <option value="daily">Daily</option>
                                <option value="weekly">Weekly</option>
                            </select>
                        </div>
                        
                        <div className="form-group">
                            <label>Time:</label>
                            <input 
                                type="time" 
                                value={scheduleConfig.time} 
                                onChange={handleTimeChange}
                            />
                        </div>
                        
                        {scheduleConfig.frequency === 'weekly' && (
                            <div className="form-group days-selection">
                                <label>Days:</label>
                                <div className="days-checkboxes">
                                    {['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'].map(day => (
                                        <div key={day} className="day-checkbox">
                                            <input 
                                                type="checkbox" 
                                                id={day} 
                                                checked={scheduleConfig.days.includes(day)} 
                                                onChange={() => handleDayToggle(day)}
                                            />
                                            <label htmlFor={day}>{day.charAt(0).toUpperCase() + day.slice(1)}</label>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                </div>
                
                <div className="schedule-modal-footer">
                    <button className="cancel-button" onClick={onClose}>Cancel</button>
                    <button className="schedule-button" onClick={handleSubmit}>Schedule</button>
                </div>
            </div>
        </div>
    );
};

export default ScheduleModal;