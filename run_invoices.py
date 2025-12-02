#!/usr/bin/env python3
"""
Helper script to run invoice generation easily.
This script ensures the correct PYTHONPATH is set.
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import and run the scheduler
if __name__ == "__main__":
    import argparse
    from core.scheduler import load_default_tasks, run_task, list_tasks
    from core.orchestrator import Orchestrator
    
    parser = argparse.ArgumentParser(
        description="Lettia Automation - Generate Invoices"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug/dry-run mode (no writes, preview only)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available tasks"
    )
    parser.add_argument(
        "--task",
        default="generate_invoices",
        help="Task name to execute (default: generate_invoices)"
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
        sys.exit(0)
    
    # Execute the task
    try:
        if args.task == "generate_invoices":
            orchestrator = Orchestrator()
            success = orchestrator.generate_invoices(debug=args.debug)
        else:
            success = run_task(args.task)
        
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

