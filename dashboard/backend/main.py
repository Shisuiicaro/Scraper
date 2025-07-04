import os
import subprocess
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts')

# --- State for running processes ---
processes = {}

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()
scheduler.start()

# --- Helper Functions ---
def run_script_job(script_name):
    """Function to be executed by the scheduler."""
    try:
        script_path = os.path.join(SCRIPTS_DIR, script_name)
        if not os.path.exists(script_path):
            print(f"Error: Script not found at {script_path}")
            return

        process = subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        print(f"Started script '{script_name}' with PID {process.pid}")
    except Exception as e:
        print(f"Failed to start script '{script_name}': {e}")

# --- API Endpoints ---

@app.route('/api/scripts', methods=['GET'])
def get_scripts():
    """Returns a list of available python scripts in the scripts directory."""
    try:
        files = [f for f in os.listdir(SCRIPTS_DIR) if f.endswith('.py')]
        return jsonify(files)
    except FileNotFoundError:
        return jsonify({'error': 'Scripts directory not found'}), 404

@app.route('/api/run-script', methods=['POST'])
def run_script():
    """Runs a specified script."""
    data = request.json
    script_name = data.get('script')
    if not script_name:
        return jsonify({'error': 'Script name is required'}), 400

    script_path = os.path.join(SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        return jsonify({'error': 'Script not found'}), 404

    try:
        # Using Popen to run the script in the background
        process = subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        processes[script_name] = process
        return jsonify({'message': f'Script {script_name} started successfully.', 'pid': process.pid}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/script-status', methods=['GET'])
def get_script_status():
    """Checks the status of a running script."""
    script_name = request.args.get('script')
    if not script_name or script_name not in processes:
        return jsonify({'status': 'not_running'})

    process = processes[script_name]
    if process.poll() is None:
        return jsonify({'status': 'running'})
    else:
        # Script has finished, remove from tracking
        del processes[script_name]
        return jsonify({'status': 'finished', 'returncode': process.returncode})

@app.route('/api/schedule-script', methods=['POST'])
def schedule_script():
    """Schedules a script to run at a given interval using a cron-like syntax."""
    data = request.json
    script_name = data.get('script')
    cron = data.get('cron') # e.g., {'hour': 1, 'minute': 30}

    if not script_name or not cron:
        return jsonify({'error': 'Script name and cron schedule are required'}), 400

    try:
        # Remove existing job for this script if it exists
        if scheduler.get_job(script_name):
            scheduler.remove_job(script_name)

        scheduler.add_job(run_script_job, 'cron', id=script_name, args=[script_name], **cron)
        return jsonify({'message': f'Script {script_name} scheduled successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduled-jobs', methods=['GET'])
def get_scheduled_jobs():
    """Returns a list of all scheduled jobs."""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({'id': job.id, 'next_run_time': str(job.next_run_time)})
    return jsonify(jobs)

@app.route('/api/cancel-job', methods=['POST'])
def cancel_scheduled_job():
    """Cancels a scheduled job."""
    data = request.json
    job_id = data.get('job_id')
    if not job_id:
        return jsonify({'error': 'Job ID is required'}), 400

    try:
        scheduler.remove_job(job_id)
        return jsonify({'message': f'Job {job_id} cancelled successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)