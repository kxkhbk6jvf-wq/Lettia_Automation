#!/usr/bin/env python3
"""
Koyeb Entrypoint Script
Executes scheduled tasks for Lettia Automation on Koyeb platform.
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def health_check():
    """Simple health check endpoint for Koyeb."""
    try:
        # Basic validation - check if we can import core modules
        from core.scheduler import load_default_tasks
        load_default_tasks()
        logger.info("Health check: OK - All modules loaded successfully")
        return True
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return False


def run_scheduled_task(task_name: str):
    """
    Run a specific scheduled task.
    
    Args:
        task_name: Name of the task to execute
    """
    try:
        from core.scheduler import load_default_tasks, run_task
        
        logger.info(f"Loading tasks and executing: {task_name}")
        load_default_tasks()
        
        success = run_task(task_name)
        
        if success:
            logger.info(f"Task '{task_name}' completed successfully")
            return 0
        else:
            logger.error(f"Task '{task_name}' failed")
            return 1
            
    except Exception as e:
        logger.error(f"Error executing task '{task_name}': {str(e)}", exc_info=True)
        return 1


def main():
    """
    Main entrypoint for Koyeb deployment.
    
    This script can run in different modes:
    1. Health check: KOYEB_MODE=health
    2. Single task: KOYEB_TASK=<task_name>
    3. Scheduled mode: KOYEB_MODE=scheduled (runs tasks on schedule)
    """
    mode = os.getenv('KOYEB_MODE', 'task')
    task_name = os.getenv('KOYEB_TASK', 'process_sef')
    schedule_interval = int(os.getenv('KOYEB_SCHEDULE_INTERVAL', '3600'))  # Default: 1 hour
    
    logger.info("=" * 70)
    logger.info("Lettia Automation - Koyeb Entrypoint")
    logger.info(f"Mode: {mode}")
    logger.info(f"Task: {task_name}")
    logger.info("=" * 70)
    
    # Health check mode
    if mode == 'health':
        health_check()
        return 0
    
    # Single task mode (default)
    if mode == 'task':
        return run_scheduled_task(task_name)
    
    # Scheduled mode (runs tasks continuously)
    if mode == 'scheduled':
        logger.info(f"Starting scheduled mode (interval: {schedule_interval}s)")
        while True:
            try:
                logger.info(f"Running scheduled task: {task_name} at {datetime.now()}")
                result = run_scheduled_task(task_name)
                
                if result != 0:
                    logger.warning(f"Task failed with exit code {result}")
                
                logger.info(f"Waiting {schedule_interval} seconds until next run...")
                time.sleep(schedule_interval)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in scheduled mode: {str(e)}", exc_info=True)
                logger.info(f"Waiting {schedule_interval} seconds before retry...")
                time.sleep(schedule_interval)
        
        return 0
    
    logger.error(f"Unknown mode: {mode}. Use 'health', 'task', or 'scheduled'")
    return 1


if __name__ == "__main__":
    sys.exit(main())

