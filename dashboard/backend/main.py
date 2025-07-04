import os
import subprocess
import threading
from threading import Thread
import time
from datetime import datetime
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import uuid

app = Flask(__name__)
CORS(app, origins="*")

# Use relative path to scripts directory
SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))

# --- State Management ---
running_tasks = {}
scheduled_tasks = {}

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()
scheduler.start()

# Helper function for scheduled jobs
def run_sequence_job(scripts, task_id=None):
    """Run a sequence of scripts as a scheduled job"""
    if task_id is None:
        task_id = str(uuid.uuid4())
        
    # Create a new task
    task = {
        'id': task_id,
        'scripts': scripts,
        'status': 'starting',
        'output': [],
        'start_time': datetime.now().isoformat(),
        'scheduled': True
    }
    running_tasks[task_id] = task
    
    # Update the last_run time for the scheduled task
    for schedule_id, schedule_data in scheduled_tasks.items():
        if schedule_data.get('scripts') == scripts:
            schedule_data['last_run'] = datetime.now().isoformat()
    
    # Run the task in a separate thread
    thread = Thread(target=execute_script_sequence, args=(scripts, task_id))
    thread.daemon = True
    thread.start()
    
    return task_id

# --- Helper Functions ---
def stream_output(process, task_id, stream_type):
    stream = process.stdout if stream_type == 'stdout' else process.stderr
    for line in iter(stream.readline, ''):
        if task_id in running_tasks:
            running_tasks[task_id]['output'].append(line)
    stream.close()

def run_script_and_wait(script_name, task_id):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        running_tasks[task_id]['status'] = 'error'
        running_tasks[task_id]['output'].append(f"Error: Script not found at {script_path}")
        return

    try:
        # Configurar os argumentos do Popen com base no sistema operacional
        popen_args = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'text': True,
            'bufsize': 1,
            'cwd': os.path.dirname(SCRIPTS_DIR)  # Define o diretório de trabalho como o diretório raiz do projeto
        }
        
        # Adicionar creationflags apenas no Windows
        if os.name == 'nt':  # Windows
            popen_args['creationflags'] = subprocess.CREATE_NO_WINDOW
            
        process = subprocess.Popen(
            ['python', '-u', script_path],
            **popen_args
        )
        running_tasks[task_id]['process'] = process
        running_tasks[task_id]['pid'] = process.pid

        stdout_thread = threading.Thread(target=stream_output, args=(process, task_id, 'stdout'))
        stderr_thread = threading.Thread(target=stream_output, args=(process, task_id, 'stderr'))
        stdout_thread.start()
        stderr_thread.start()

        process.wait()
        stdout_thread.join()
        stderr_thread.join()

        if task_id in running_tasks:
            running_tasks[task_id]['status'] = 'finished' if process.returncode == 0 else 'error'
            running_tasks[task_id]['returncode'] = process.returncode

    except Exception as e:
        if task_id in running_tasks:
            running_tasks[task_id]['status'] = 'error'
            running_tasks[task_id]['output'].append(f"Failed to run script '{script_name}': {e}")

def execute_script_sequence(scripts, task_id):
    running_tasks[task_id]['status'] = 'running'
    for i, script_name in enumerate(scripts):
        if task_id not in running_tasks or running_tasks[task_id].get('stopped'):
            running_tasks[task_id]['status'] = 'stopped'
            break
        
        running_tasks[task_id]['output'].append(f"--- Running script {i+1}/{len(scripts)}: {script_name} ---")
        run_script_and_wait(script_name, task_id)
        if running_tasks[task_id]['status'] == 'error':
            running_tasks[task_id]['output'].append(f"--- Sequence stopped due to error in {script_name} ---")
            break
    else:
        if running_tasks.get(task_id) and not running_tasks[task_id].get('stopped'):
            running_tasks[task_id]['status'] = 'finished'

# --- API Endpoints ---

@app.route('/api/scripts', methods=['GET'])
def get_scripts():
    try:
        print(f"Looking for scripts in: {SCRIPTS_DIR}")
        if not os.path.exists(SCRIPTS_DIR):
            print(f"Directory does not exist: {SCRIPTS_DIR}")
            return jsonify({'error': 'Scripts directory not found'}), 404
        
        files = [f for f in os.listdir(SCRIPTS_DIR) if f.endswith('.py')]
        print(f"Found scripts: {files}")
        return jsonify(sorted(files))
    except Exception as e:
        print(f"Error in get_scripts: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/run-task', methods=['POST'])
def run_task():
    data = request.json
    scripts = data.get('scripts') # Expect a list of scripts
    schedule_config = data.get('schedule') # Optional schedule configuration
    
    if not scripts or not isinstance(scripts, list):
        return jsonify({'error': 'A list of script names is required'}), 400

    task_id = str(uuid.uuid4())
    task_data = {
        'id': task_id,
        'scripts': scripts,
        'status': 'starting',
        'output': [],
        'pid': None,
        'process': None
    }
    
    # Handle scheduling if provided
    if schedule_config and schedule_config.get('enabled'):
        schedule_id = str(uuid.uuid4())
        frequency = schedule_config.get('frequency', 'daily')
        time_parts = schedule_config.get('time', '12:00').split(':')
        hour = int(time_parts[0])
        minute = int(time_parts[1])
        
        # Create the scheduled task
        scheduled_tasks[schedule_id] = {
            'id': schedule_id,
            'scripts': scripts,
            'frequency': frequency,
            'time': schedule_config.get('time'),
            'days': schedule_config.get('days', []),
            'enabled': True,
            'last_run': None,
            'next_run': None
        }
        
        # Schedule the job based on frequency
        if frequency == 'daily':
            job = scheduler.add_job(
                run_sequence_job,
                'cron',
                hour=hour,
                minute=minute,
                args=[scripts, task_id],
                id=schedule_id
            )
            scheduled_tasks[schedule_id]['next_run'] = job.next_run_time
        else:  # weekly with specific days
            days_of_week = ','.join(schedule_config.get('days', []))
            if days_of_week:
                job = scheduler.add_job(
                    run_sequence_job,
                    'cron',
                    day_of_week=days_of_week,
                    hour=hour,
                    minute=minute,
                    args=[scripts, task_id],
                    id=schedule_id
                )
                scheduled_tasks[schedule_id]['next_run'] = job.next_run_time
        
        return jsonify({
            'message': 'Task scheduled',
            'schedule_id': schedule_id,
            'next_run': scheduled_tasks[schedule_id]['next_run'].isoformat() if scheduled_tasks[schedule_id]['next_run'] else None
        }), 200
    else:
        # Run immediately if no schedule
        running_tasks[task_id] = task_data
        thread = threading.Thread(target=execute_script_sequence, args=(scripts, task_id))
        thread.start()
        return jsonify({'message': 'Task started', 'task_id': task_id}), 200

@app.route('/api/task-status', methods=['GET'])
def get_all_task_status():
    status_list = []
    for task_id, task_data in running_tasks.items():
        status_list.append({
            'id': task_id,
            'scripts': task_data['scripts'],
            'status': task_data['status'],
            'output_lines': len(task_data['output'])
        })
    return jsonify(status_list)

@app.route('/api/task-status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    if task_id not in running_tasks:
        return jsonify({'error': 'Task not found'}), 404
    
    task = running_tasks[task_id]
    return jsonify({
        'id': task_id,
        'scripts': task['scripts'],
        'status': task['status'],
        'output': task['output']
    })

@app.route('/api/stop-task/<task_id>', methods=['POST'])
def stop_task(task_id):
    if task_id not in running_tasks:
        return jsonify({'error': 'Task not found'}), 404

    task = running_tasks[task_id]
    task['stopped'] = True
    if task.get('process'):
        try:
            task['process'].terminate() # or kill()
            task['status'] = 'stopped'
            return jsonify({'message': f'Task {task_id} stopped.'}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({'message': 'Task marked for stopping.'}), 200

# --- Scheduling Endpoints ---

@app.route('/api/schedules', methods=['GET'])
def get_schedules():
    """Get all scheduled tasks"""
    schedule_list = []
    for schedule_id, schedule_data in scheduled_tasks.items():
        # Format next_run as ISO string if it exists
        next_run = None
        if schedule_data.get('next_run'):
            next_run = schedule_data['next_run'].isoformat()
            
        schedule_list.append({
            'id': schedule_id,
            'scripts': schedule_data['scripts'],
            'frequency': schedule_data['frequency'],
            'time': schedule_data['time'],
            'days': schedule_data['days'],
            'enabled': schedule_data['enabled'],
            'last_run': schedule_data['last_run'],
            'next_run': next_run
        })
    return jsonify(schedule_list)

@app.route('/api/schedules/<schedule_id>', methods=['GET'])
def get_schedule(schedule_id):
    """Get a specific scheduled task"""
    if schedule_id not in scheduled_tasks:
        return jsonify({'error': 'Schedule not found'}), 404
        
    schedule_data = scheduled_tasks[schedule_id]
    # Format next_run as ISO string if it exists
    next_run = None
    if schedule_data.get('next_run'):
        next_run = schedule_data['next_run'].isoformat()
        
    return jsonify({
        'id': schedule_id,
        'scripts': schedule_data['scripts'],
        'frequency': schedule_data['frequency'],
        'time': schedule_data['time'],
        'days': schedule_data['days'],
        'enabled': schedule_data['enabled'],
        'last_run': schedule_data['last_run'],
        'next_run': next_run
    })

@app.route('/api/schedules/<schedule_id>', methods=['DELETE'])
def delete_schedule(schedule_id):
    """Delete a scheduled task"""
    if schedule_id not in scheduled_tasks:
        return jsonify({'error': 'Schedule not found'}), 404
        
    try:
        # Remove from scheduler
        scheduler.remove_job(schedule_id)
        # Remove from our tracking
        del scheduled_tasks[schedule_id]
        return jsonify({'message': f'Schedule {schedule_id} deleted.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/schedules/<schedule_id>/toggle', methods=['POST'])
def toggle_schedule(schedule_id):
    """Enable or disable a scheduled task"""
    if schedule_id not in scheduled_tasks:
        return jsonify({'error': 'Schedule not found'}), 404
        
    try:
        current_state = scheduled_tasks[schedule_id]['enabled']
        new_state = not current_state
        scheduled_tasks[schedule_id]['enabled'] = new_state
        
        # Pause or resume in the scheduler
        if new_state:
            # Re-add the job if it was disabled
            schedule_data = scheduled_tasks[schedule_id]
            time_parts = schedule_data['time'].split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            
            if schedule_data['frequency'] == 'daily':
                job = scheduler.add_job(
                    run_sequence_job,
                    'cron',
                    hour=hour,
                    minute=minute,
                    args=[schedule_data['scripts'], str(uuid.uuid4())],
                    id=schedule_id,
                    replace_existing=True
                )
            else:  # weekly
                days_of_week = ','.join(schedule_data['days'])
                if days_of_week:
                    job = scheduler.add_job(
                        run_sequence_job,
                        'cron',
                        day_of_week=days_of_week,
                        hour=hour,
                        minute=minute,
                        args=[schedule_data['scripts'], str(uuid.uuid4())],
                        id=schedule_id,
                        replace_existing=True
                    )
            
            scheduled_tasks[schedule_id]['next_run'] = job.next_run_time
        else:
            # Remove the job if it's being disabled
            scheduler.remove_job(schedule_id)
            scheduled_tasks[schedule_id]['next_run'] = None
            
        return jsonify({
            'message': f'Schedule {schedule_id} is now {"enabled" if new_state else "disabled"}.',
            'enabled': new_state
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print(f"Starting server with scripts directory: {SCRIPTS_DIR}")
    print(f"Scripts directory exists: {os.path.exists(SCRIPTS_DIR)}")
    if os.path.exists(SCRIPTS_DIR):
        print(f"Scripts in directory: {[f for f in os.listdir(SCRIPTS_DIR) if f.endswith('.py')]}")
    app.run(host='0.0.0.0', port=5000, debug=True)