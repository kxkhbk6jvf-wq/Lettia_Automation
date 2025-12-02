"""
Database connection module.
Note: Google Sheets is the initial data store for this project.
This SQLite connection is a placeholder for future database needs.
"""

import sqlite3
from pathlib import Path
from typing import Optional


def get_database_connection(db_path: Optional[Path] = None):
    """
    Get a database connection.
    
    Args:
        db_path: Optional path to SQLite database file.
                 Defaults to 'lettia.db' in project root.
    
    Returns:
        SQLite connection object
    
    Note:
        Google Sheets is currently being used as the primary data store.
        This SQLite connection is provided as a placeholder for future
        database requirements or local data caching needs.
    
    Planned behavior:
        - Initialize SQLite database if it doesn't exist
        - Return connection for database operations
        - Handle connection errors gracefully
    """
    if db_path is None:
        # Default to project root
        project_root = Path(__file__).parent.parent.parent
        db_path = project_root / 'lettia.db'
    
    # Ensure directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create and return connection
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
    
    # TODO: Initialize database schema if needed
    # TODO: Set up connection pooling if required
    
    return conn

