"""
Task Scheduler Module.
Executes individual tasks from the Orchestrator via CLI using a task registry pattern.
"""

import logging
from typing import Callable, Dict, Optional

# Configure logging (matching orchestrator style)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global task registry mapping task names to callable functions
TASK_REGISTRY: Dict[str, Callable[[], bool]] = {}


def register_task(name: str) -> Callable:
    """
    Decorator function to register a task in the TASK_REGISTRY.
    
    Usage:
        @register_task("process_sef")
        def run_sef():
            orchestrator.process_sef()
    
    Args:
        name: Task name to register
        
    Returns:
        Decorator function that registers the task
    """
    def decorator(func: Callable[[], bool]) -> Callable[[], bool]:
        if name in TASK_REGISTRY:
            logger.warning(f"Task '{name}' is already registered. Overwriting previous registration.")
        TASK_REGISTRY[name] = func
        logger.debug(f"Registered task: {name}")
        return func
    return decorator


def run_task(task_name: str) -> bool:
    """
    Execute a registered task by name.
    
    Looks up the task in the registry, logs execution start/end,
    executes with error handling, and returns success status.
    
    Args:
        task_name: Name of the task to execute (must be in TASK_REGISTRY)
        
    Returns:
        True if task executed successfully, False otherwise
        
    Raises:
        ValueError: If task name is not found in registry
    """
    if task_name not in TASK_REGISTRY:
        available_tasks = ', '.join(sorted(TASK_REGISTRY.keys()))
        error_msg = (
            f"Task '{task_name}' not found in registry. "
            f"Available tasks: {available_tasks if available_tasks else 'none'}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    task_func = TASK_REGISTRY[task_name]
    
    logger.info("=" * 70)
    logger.info(f"Executing task: {task_name}")
    logger.info("=" * 70)
    
    try:
        result = task_func()
        
        if result:
            logger.info(f"Task '{task_name}' completed successfully")
        else:
            logger.warning(f"Task '{task_name}' completed with errors")
        
        logger.info("=" * 70)
        return result
        
    except Exception as e:
        logger.error(f"Task '{task_name}' failed with exception: {str(e)}", exc_info=True)
        logger.info("=" * 70)
        return False


def load_default_tasks() -> None:
    """
    Load default tasks from the Orchestrator into the task registry.
    
    Instantiates an Orchestrator and registers all available tasks:
    - process_sef
    - sync_lodgify
    - send_whatsapp_messages
    - update_financials
    - export_faturas
    - create_notion_tasks
    - generate_kpis
    - full_cycle
    """
    logger.info("Loading default tasks from Orchestrator...")
    
    try:
        # Import orchestrator here to avoid circular imports
        from .orchestrator import Orchestrator
        
        # Instantiate orchestrator
        orchestrator = Orchestrator()
        
        # Register all orchestrator methods as tasks
        @register_task("process_sef")
        def run_process_sef() -> bool:
            """Execute SEF form processing task."""
            return orchestrator.process_sef()
        
        @register_task("sync_lodgify")
        def run_sync_lodgify() -> bool:
            """Execute Lodgify synchronization task."""
            return orchestrator.sync_lodgify()
        
        @register_task("send_whatsapp_messages")
        def run_send_whatsapp_messages() -> bool:
            """Execute WhatsApp message sending task."""
            return orchestrator.send_whatsapp_messages()
        
        @register_task("update_financials")
        def run_update_financials() -> bool:
            """Execute financial data update task."""
            return orchestrator.update_financials()
        
        @register_task("export_faturas")
        def run_export_faturas() -> bool:
            """Execute faturas export task."""
            return orchestrator.export_faturas()
        
        @register_task("create_notion_tasks")
        def run_create_notion_tasks() -> bool:
            """Execute Notion task creation."""
            return orchestrator.create_notion_tasks()
        
        @register_task("generate_kpis")
        def run_generate_kpis() -> bool:
            """Execute KPI generation task."""
            return orchestrator.generate_kpis()
        
        @register_task("full_cycle")
        def run_full_cycle() -> bool:
            """Execute complete automation cycle."""
            result = orchestrator.full_cycle()
            return result.get('overall_success', False) if isinstance(result, dict) else result
        
        @register_task("sync_reservations_csv")
        def run_sync_reservations_csv() -> bool:
            """Execute CSV reservations synchronization task."""
            return orchestrator.sync_reservations_csv()
        
        @register_task("generate_invoices")
        def run_generate_invoices() -> bool:
            """Execute invoice generation task."""
            return orchestrator.generate_invoices()
        
        logger.info(f"Loaded {len(TASK_REGISTRY)} default tasks: {', '.join(sorted(TASK_REGISTRY.keys()))}")
        
    except Exception as e:
        logger.error(f"Failed to load default tasks: {str(e)}", exc_info=True)
        raise


def list_tasks() -> list:
    """
    List all registered tasks.
    
    Returns:
        List of task names currently in the registry
    """
    return sorted(TASK_REGISTRY.keys())


if __name__ == "__main__":
    """
    CLI entry point for task execution.
    
    Usage:
        python -m core.scheduler --task process_sef
        python core/scheduler.py --task full_cycle
        
    Note: When running as a script directly, ensure PYTHONPATH includes the project root.
    """
    import sys
    import argparse
    from pathlib import Path
    
    # Add project root to Python path for direct script execution
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    parser = argparse.ArgumentParser(
        description="Lettia Automation Scheduler - Execute automation tasks"
    )
    parser.add_argument(
        "--task",
        required=False,
        help="Task name to execute (use --list to see available tasks)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available tasks and exit"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug/dry-run mode (no writes, preview only)"
    )
    
    args = parser.parse_args()
    
    # Load default tasks
    load_default_tasks()
    
    # Handle --list flag
    if args.list:
        tasks = list_tasks()
        print("\nAvailable tasks:")
        print("=" * 70)
        for task in tasks:
            print(f"  - {task}")
        print("=" * 70)
        exit(0)
    
    # Validate --task is provided if not listing
    if not args.task:
        parser.error("--task is required (use --list to see available tasks)")
    
    # Execute requested task
    try:
        # Pass debug flag to task if it's generate_invoices
        if args.task == "generate_invoices":
            from .orchestrator import Orchestrator
            orchestrator = Orchestrator()
            success = orchestrator.generate_invoices(debug=args.debug)
        else:
            success = run_task(args.task)
        exit(0 if success else 1)
    except ValueError as e:
        logger.error(str(e))
        exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        exit(1)

