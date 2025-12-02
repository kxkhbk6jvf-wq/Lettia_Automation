"""
Database module for Lettia automation.
Currently using Google Sheets as the primary data store.
"""

from .connection import get_database_connection

__all__ = ['get_database_connection']

