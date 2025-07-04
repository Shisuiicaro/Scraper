import React, { useState, useEffect, useRef } from 'react';
import './App.css';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faPlay, faStop, faEye, faTrash, faPlus, faClock, faSave, faCalendarAlt, faToggleOn, faToggleOff, faTimes } from '@fortawesome/free-solid-svg-icons';
        
const API_URL = process.env.REACT_APP_API_URL || 'http://161.97.78.253:5000/api';

function App() {
    const [availableScripts, setAvailableScripts] = useState([]);
    const [selectedScripts, setSelectedScripts] = useState([]);
    const [runningTasks, setRunningTasks] = useState([]);
    const [activeTaskDetail, setActiveTaskDetail] = useState(null);
    const [scheduleModalOpen, setScheduleModalOpen] = useState(false);
    const [scheduleConfig, setScheduleConfig] = useState({
        enabled: false,
        frequency: 'daily',
        time: '12:00',
        days: ['monday', 'wednesday', 'friday']
    });
    const [notification, setNotification] = useState(null);
    const [scheduledTasks, setScheduledTasks] = useState([]);
    const [schedulesVisible, setSchedulesVisible] = useState(false);
    const outputRef = useRef(null);

    // Fetch initial data
    useEffect(() => {
        fetchAvailableScripts();
        fetchRunningTasks();
        fetchSchedules();

        // Poll for task status
        const interval = setInterval(fetchRunningTasks, 2000);
        // Poll for schedule updates every 30 seconds
        const scheduleInterval = setInterval(fetchSchedules, 30000);
        
        return () => {
            clearInterval(interval);
            clearInterval(scheduleInterval);
        };
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
    
    const fetchSchedules = async () => {
        try {
            const response = await fetch(`${API_URL}/schedules`);
            const data = await response.json();
            setScheduledTasks(data);
        } catch (error) {
            console.error('Error fetching schedules:', error);
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

    // Show notification message
    const showNotification = (message, type = 'info') => {
        setNotification({ message, type });
        setTimeout(() => setNotification(null), 3000);
    };

    const handleScriptSelection = (script) => {
        // Check if script is already in the sequence
        if (selectedScripts.includes(script)) {
            showNotification(`"${script}" já está na sequência!`, 'warning');
            return;
        }
        setSelectedScripts(prev => [...prev, script]);
        showNotification(`"${script}" adicionado à sequência`, 'success');
    };

    const handleRemoveScript = (index) => {
        setSelectedScripts(prev => prev.filter((_, i) => i !== index));
    };

    const handleRunSequence = async () => {
        if (selectedScripts.length === 0) {
            showNotification('Selecione pelo menos um script para executar', 'error');
            return;
        }
        try {
            const payload = { 
                scripts: selectedScripts,
                schedule: scheduleConfig.enabled ? scheduleConfig : null
            };
            
            const response = await fetch(`${API_URL}/run-task`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            
            const data = await response.json();
            
            if (scheduleConfig.enabled) {
                showNotification(
                    `Sequência agendada com sucesso! ${scheduleConfig.frequency === 'daily' ? 'Diariamente' : 'Nos dias selecionados'} às ${scheduleConfig.time}`, 
                    'success'
                );
            } else {
                showNotification('Sequência iniciada com sucesso!', 'success');
                fetchTaskDetail(data.task_id);
            }
            
            setSelectedScripts([]); // Clear selection after running
        } catch (error) {
            console.error('Error running task sequence:', error);
            showNotification('Erro ao executar a sequência de scripts', 'error');
        }
    };

    const handleStopTask = async (taskId) => {
        try {
            await fetch(`${API_URL}/stop-task/${taskId}`, { method: 'POST' });
            fetchRunningTasks(); // Refresh list
            showNotification('Tarefa interrompida com sucesso', 'info');
        } catch (error) {
            console.error('Error stopping task:', error);
            showNotification('Erro ao interromper a tarefa', 'error');
        }
    };

    const handleViewTask = (taskId) => {
        fetchTaskDetail(taskId);
    };
    
    const toggleScheduleModal = () => {
        setScheduleModalOpen(!scheduleModalOpen);
    };
    
    const handleScheduleChange = (e) => {
        const { name, value, type, checked } = e.target;
        
        if (type === 'checkbox') {
            setScheduleConfig(prev => ({
                ...prev,
                [name]: checked
            }));
        } else if (name === 'days') {
            // Handle multiple select for days
            const options = e.target.options;
            const selectedDays = [];
            for (let i = 0; i < options.length; i++) {
                if (options[i].selected) {
                    selectedDays.push(options[i].value);
                }
            }
            setScheduleConfig(prev => ({
                ...prev,
                days: selectedDays
            }));
        } else {
            setScheduleConfig(prev => ({
                ...prev,
                [name]: value
            }));
        }
    };
    
    const saveScheduleConfig = () => {
        showNotification('Configuração de agendamento salva', 'success');
        toggleScheduleModal();
    };
    
    const toggleScheduleVisibility = () => {
        setSchedulesVisible(!schedulesVisible);
    };
    
    const formatDate = (dateString) => {
        if (!dateString) return 'Não agendado';
        
        try {
            const date = new Date(dateString);
            return date.toLocaleString('pt-BR', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        } catch (error) {
            console.error('Error formatting date:', error);
            return dateString;
        }
    };
    
    const handleToggleSchedule = async (scheduleId) => {
        try {
            const response = await fetch(`${API_URL}/schedules/${scheduleId}/toggle`, {
                method: 'POST'
            });
            const data = await response.json();
            
            if (response.ok) {
                showNotification(
                    `Agendamento ${data.enabled ? 'ativado' : 'desativado'} com sucesso`, 
                    'success'
                );
                fetchSchedules(); // Refresh list
            } else {
                throw new Error(data.error || 'Erro ao alterar o agendamento');
            }
        } catch (error) {
            console.error('Error toggling schedule:', error);
            showNotification('Erro ao alterar o agendamento', 'error');
        }
    };
    
    const handleDeleteSchedule = async (scheduleId) => {
        if (!window.confirm('Tem certeza que deseja excluir este agendamento?')) {
            return;
        }
        
        try {
            const response = await fetch(`${API_URL}/schedules/${scheduleId}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                showNotification('Agendamento excluído com sucesso', 'success');
                fetchSchedules(); // Refresh list
            } else {
                const data = await response.json();
                throw new Error(data.error || 'Erro ao excluir o agendamento');
            }
        } catch (error) {
            console.error('Error deleting schedule:', error);
            showNotification('Erro ao excluir o agendamento', 'error');
        }
    };

    return (
        <div className="App">
            <header className="App-header">
                <h1>Script Automation Dashboard</h1>
            </header>
            
            {/* Notification component */}
            {notification && (
                <div className={`notification ${notification.type}`}>
                    {notification.message}
                </div>
            )}
            
            <main className="container">
                <div className="control-panel">
                    <div className="card script-selector">
                        <h2>Criar Sequência</h2>
                        <p>Selecione scripts da lista para adicionar à sequência de execução.</p>
                        <div className="script-list">
                            {availableScripts.map(script => (
                                <button key={script} onClick={() => handleScriptSelection(script)} className="script-item">
                                    {script}
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="card sequence-display">
                        <h2>Sequência de Execução</h2>
                        {selectedScripts.length === 0 ? (
                            <p className="empty-message">Nenhum script selecionado.</p>
                        ) : (
                            <ol className="sequence-list">
                                {selectedScripts.map((script, index) => (
                                    <li key={index}>
                                        <span>{script}</span>
                                        <button onClick={() => handleRemoveScript(index)} className="remove-btn" title="Remover script">×</button>
                                    </li>
                                ))}
                            </ol>
                        )}
                        <div className="sequence-actions">
                            <button 
                                onClick={toggleScheduleModal} 
                                className={`schedule-btn ${scheduleConfig.enabled ? 'scheduled' : ''}`}
                                title="Configurar agendamento"
                            >
                                {scheduleConfig.enabled ? 'Agendado' : 'Agendar'}
                            </button>
                            <button 
                                onClick={handleRunSequence} 
                                disabled={selectedScripts.length === 0} 
                                className="run-btn"
                            >
                                {scheduleConfig.enabled ? 'Salvar e Agendar' : 'Executar Agora'}
                            </button>
                        </div>
                    </div>
                </div>

                <div className="card task-monitor">
                    <h2>Tarefas Ativas e Recentes</h2>
                    {runningTasks.length === 0 ? (
                        <p className="empty-message">Nenhuma tarefa em execução.</p>
                    ) : (
                        <ul>
                            {runningTasks.map(task => (
                                <li key={task.id} className={`task-item-status ${task.status}`}>
                                    <span><strong>ID:</strong> {task.id.substring(0, 8)}...</span>
                                    <span><strong>Scripts:</strong> {task.scripts.join(', ')}</span>
                                    <span className="status">
                                        {task.status === 'running' ? 'Em execução' : 
                                         task.status === 'finished' ? 'Concluída' : 
                                         task.status === 'error' ? 'Erro' : 
                                         task.status === 'stopped' ? 'Interrompida' : task.status}
                                    </span>
                                    <div className="task-actions">
                                        <button onClick={() => handleViewTask(task.id)} className="view-btn" title="Ver logs">
                                            <FontAwesomeIcon icon={faEye} /> Ver Logs
                                        </button>
                                        {task.status === 'running' && 
                                            <button onClick={() => handleStopTask(task.id)} className="stop-btn" title="Interromper tarefa">
                                                <FontAwesomeIcon icon={faStop} /> Parar
                                            </button>
                                        }
                                    </div>
                                </li>
                            ))}
                        </ul>
                    )}
                </div>
                
                <div className="card schedules-panel">
                    <div className="schedules-header" onClick={toggleScheduleVisibility}>
                        <h2>
                            <FontAwesomeIcon icon={faCalendarAlt} /> Agendamentos
                            <span className="toggle-icon">
                                {schedulesVisible ? '▼' : '►'}
                            </span>
                        </h2>
                    </div>
                    
                    {schedulesVisible && (
                        <div className="schedules-content">
                            {scheduledTasks.length === 0 ? (
                                <p className="empty-message">Nenhum agendamento configurado.</p>
                            ) : (
                                <ul className="schedules-list">
                                    {scheduledTasks.map(schedule => (
                                        <li key={schedule.id} className={`schedule-item ${schedule.enabled ? 'enabled' : 'disabled'}`}>
                                            <div className="schedule-info">
                                                <h3>Scripts: {schedule.scripts.join(', ')}</h3>
                                                <p>
                                                    <strong>Frequência:</strong> {schedule.frequency === 'daily' ? 'Diariamente' : 'Semanal'}
                                                    {schedule.frequency === 'weekly' && (
                                                        <span className="schedule-days">
                                                            {schedule.days.map(day => {
                                                                const dayMap = {
                                                                    'monday': 'Seg',
                                                                    'tuesday': 'Ter',
                                                                    'wednesday': 'Qua',
                                                                    'thursday': 'Qui',
                                                                    'friday': 'Sex',
                                                                    'saturday': 'Sáb',
                                                                    'sunday': 'Dom'
                                                                };
                                                                return dayMap[day] || day;
                                                            }).join(', ')}
                                                        </span>
                                                    )}
                                                </p>
                                                <p><strong>Horário:</strong> {schedule.time}</p>
                                                <p>
                                                    <strong>Próxima execução:</strong> {formatDate(schedule.next_run)}
                                                </p>
                                                {schedule.last_run && (
                                                    <p>
                                                        <strong>Última execução:</strong> {formatDate(schedule.last_run)}
                                                    </p>
                                                )}
                                            </div>
                                            <div className="schedule-actions">
                                                <button 
                                                    onClick={() => handleToggleSchedule(schedule.id)} 
                                                    className={`toggle-btn ${schedule.enabled ? 'enabled' : 'disabled'}`}
                                                    title={schedule.enabled ? 'Desativar agendamento' : 'Ativar agendamento'}
                                                >
                                                    <FontAwesomeIcon icon={schedule.enabled ? faToggleOn : faToggleOff} />
                                                    {schedule.enabled ? 'Ativo' : 'Inativo'}
                                                </button>
                                                <button 
                                                    onClick={() => handleDeleteSchedule(schedule.id)} 
                                                    className="delete-btn"
                                                    title="Excluir agendamento"
                                                >
                                                    <FontAwesomeIcon icon={faTrash} />
                                                    Excluir
                                                </button>
                                            </div>
                                        </li>
                                    ))}
                                </ul>
                            )}
                        </div>
                    )}
                </div>

                {activeTaskDetail && (
                    <div className="card output-viewer">
                        <div className="output-header">
                            <h2>Logs da Tarefa: {activeTaskDetail.id.substring(0, 8)}...</h2>
                            <span className={`status ${activeTaskDetail.status}`}>
                                {activeTaskDetail.status === 'running' ? 'Em execução' : 
                                 activeTaskDetail.status === 'finished' ? 'Concluída' : 
                                 activeTaskDetail.status === 'error' ? 'Erro' : 
                                 activeTaskDetail.status === 'stopped' ? 'Interrompida' : activeTaskDetail.status}
                            </span>
                        </div>
                        <pre ref={outputRef} className="output-log">
                            {activeTaskDetail.output.join('')}
                        </pre>
                        <button onClick={() => setActiveTaskDetail(null)} className="close-btn">Fechar</button>
                    </div>
                )}
                
                {/* Schedule Modal */}
                {scheduleModalOpen && (
                    <div className="modal-overlay">
                        <div className="modal schedule-modal">
                            <div className="modal-header">
                                <h2>Configurar Agendamento</h2>
                                <button onClick={toggleScheduleModal} className="close-btn">×</button>
                            </div>
                            <div className="modal-body">
                                <div className="form-group">
                                    <label>
                                        <input 
                                            type="checkbox" 
                                            name="enabled" 
                                            checked={scheduleConfig.enabled}
                                            onChange={handleScheduleChange}
                                        />
                                        Ativar agendamento
                                    </label>
                                </div>
                                
                                {scheduleConfig.enabled && (
                                    <>
                                        <div className="form-group">
                                            <label>Frequência:</label>
                                            <select 
                                                name="frequency" 
                                                value={scheduleConfig.frequency}
                                                onChange={handleScheduleChange}
                                            >
                                                <option value="daily">Diariamente</option>
                                                <option value="weekly">Dias específicos</option>
                                            </select>
                                        </div>
                                        
                                        {scheduleConfig.frequency === 'weekly' && (
                                            <div className="form-group">
                                                <label>Dias da semana:</label>
                                                <select 
                                                    name="days" 
                                                    multiple 
                                                    value={scheduleConfig.days}
                                                    onChange={handleScheduleChange}
                                                    className="days-select"
                                                >
                                                    <option value="monday">Segunda-feira</option>
                                                    <option value="tuesday">Terça-feira</option>
                                                    <option value="wednesday">Quarta-feira</option>
                                                    <option value="thursday">Quinta-feira</option>
                                                    <option value="friday">Sexta-feira</option>
                                                    <option value="saturday">Sábado</option>
                                                    <option value="sunday">Domingo</option>
                                                </select>
                                                <small>Segure Ctrl para selecionar múltiplos dias</small>
                                            </div>
                                        )}
                                        
                                        <div className="form-group">
                                            <label>Horário:</label>
                                            <input 
                                                type="time" 
                                                name="time" 
                                                value={scheduleConfig.time}
                                                onChange={handleScheduleChange}
                                            />
                                        </div>
                                    </>
                                )}
                            </div>
                            <div className="modal-footer">
                                <button onClick={toggleScheduleModal} className="cancel-btn">Cancelar</button>
                                <button onClick={saveScheduleConfig} className="save-btn">Salvar</button>
                            </div>
                        </div>
                    </div>
                )}
            </main>
        </div>
    );
}

export default App;
