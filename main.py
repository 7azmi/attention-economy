# main.py
import subprocess
import sys
import time
import logging
import os

# Basic logging for the main orchestrator
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [MAIN] - %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("main_launcher")

# --- Configuration ---
BOT_WORKER_SCRIPT = "bot_worker.py"
BOT_INSTANCES = [
    "grammar",  # This must match one of the choices in bot_worker.py's argparse
    "english"   # This must match one of the choices in bot_worker.py's argparse
]
# --- End Configuration ---

if __name__ == "__main__":
    log.info("Starting bot launcher...")

    if not os.path.exists(BOT_WORKER_SCRIPT):
        log.critical(f"Error: Worker script '{BOT_WORKER_SCRIPT}' not found.")
        sys.exit(1)

    processes = []
    for bot_id in BOT_INSTANCES:
        # Construct the command to run the worker script with the specific ID
        # sys.executable ensures we use the same Python interpreter that's running main.py
        cmd = [sys.executable, BOT_WORKER_SCRIPT, bot_id]
        log.info(f"Launching worker process: {' '.join(cmd)}")
        try:
            # Use Popen to start the process in the background without waiting
            process = subprocess.Popen(cmd)
            processes.append({"id": bot_id, "process": process})
            log.info(f"Launched {bot_id.upper()} worker (PID: {process.pid})")
            time.sleep(5) # Stagger launches slightly to avoid resource contention / API rate limits on init
        except Exception as e:
            log.error(f"Failed to launch worker '{bot_id}': {e}", exc_info=True)

    log.info(f"Launched {len(processes)} worker processes.")
    log.info("Main launcher process will now wait for workers to complete (or run indefinitely if workers loop forever).")
    log.info("Use Ctrl+C here to stop the launcher, but worker processes might continue.")
    log.info("To stop everything, you may need to terminate the worker processes manually.")

    # Optional: Wait for processes to finish (useful if they have a defined end)
    # If the workers run in an infinite loop, this wait() will never return unless they crash.
    try:
        for p_info in processes:
            p_info["process"].wait()
            log.info(f"Worker process {p_info['id']} (PID: {p_info['process'].pid}) has exited with code {p_info['process'].returncode}.")
    except KeyboardInterrupt:
        log.info("Launcher received KeyboardInterrupt. Attempting to terminate workers...")
        for p_info in processes:
            try:
                log.warning(f"Terminating worker {p_info['id']} (PID: {p_info['process'].pid})...")
                p_info["process"].terminate() # Send SIGTERM
                p_info["process"].wait(timeout=5) # Wait briefly for termination
            except subprocess.TimeoutExpired:
                log.error(f"Worker {p_info['id']} did not terminate gracefully, killing.")
                p_info["process"].kill() # Send SIGKILL
            except Exception as e:
                 log.error(f"Error terminating worker {p_info['id']}: {e}")
        log.info("Launcher exiting after attempting worker termination.")
    except Exception as e:
        log.critical(f"An error occurred while waiting for processes: {e}", exc_info=True)

    log.info("Main launcher process finished.")