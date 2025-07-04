import os
import subprocess
import threading
import time
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import uuid

app = Flask(__name__)
CORS(app)

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts')

# --- State Management ---
running_tasks = {}

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()
scheduler.start()

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
        process = subprocess.Popen(
            ['python', '-u', script_path], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            bufsize=1,
            creationflags=subprocess.CREATE_NO_WINDOW
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

def run_sequence_job(scripts, task_id):
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
        files = [f for f in os.listdir(SCRIPTS_DIR) if f.endswith('.py')]
        return jsonify(sorted(files))
    except FileNotFoundError:
        return jsonify({'error': 'Scripts directory not found'}), 404

@app.route('/api/run-task', methods=['POST'])
def run_task():
    data = request.json
    scripts = data.get('scripts') # Expect a list of scripts
    if not scripts or not isinstance(scripts, list):
        return jsonify({'error': 'A list of script names is required'}), 400

    task_id = str(uuid.uuid4())
    running_tasks[task_id] = {
        'id': task_id,
        'scripts': scripts,
        'status': 'starting',
        'output': [],
        'pid': None,
        'process': None
    }

    thread = threading.Thread(target=run_sequence_job, args=(scripts, task_id))
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

# Note: Scheduling endpoints would need to be adapted to the new task-based system.
# For simplicity, they are omitted in this refactoring but can be added back.

if __name__ == '__main__':
    app.run(debug=True, port=5001)