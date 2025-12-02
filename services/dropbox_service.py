"""
Dropbox integration service.
Handles file uploads and folder management in Dropbox App Folder.
"""

from typing import Dict, Optional
from pathlib import Path
import dropbox
from dropbox.exceptions import ApiError, AuthError
from config.settings import get_dropbox_access_token, get_dropbox_sef_folder


class DropboxService:
    """Service class for interacting with Dropbox API v2."""
    
    def __init__(self):
        """Initialize Dropbox client with access token from settings."""
        self.access_token = get_dropbox_access_token()
        self.sef_folder = get_dropbox_sef_folder()
        self.dbx = dropbox.Dropbox(self.access_token)
    
    def folder_exists(self, folder_path: str) -> bool:
        """
        Check if folder exists inside App Folder.
        
        Args:
            folder_path: Path to folder (relative to App Folder, e.g., "SEF/2024")
            
        Returns:
            True if folder exists, False otherwise
        """
        try:
            # Ensure path starts with / for Dropbox API
            dbx_path = f"/{folder_path.strip('/')}" if not folder_path.startswith('/') else folder_path
            
            # Try to get metadata for the folder
            self.dbx.files_get_metadata(dbx_path)
            return True
        except ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                return False
            # Re-raise other API errors
            raise
    
    def ensure_folder_exists(self, folder_path: str) -> None:
        """
        Create folder if it doesn't exist.
        
        Args:
            folder_path: Path to folder (relative to App Folder)
        """
        if not self.folder_exists(folder_path):
            try:
                # Ensure path starts with / for Dropbox API
                dbx_path = f"/{folder_path.strip('/')}" if not folder_path.startswith('/') else folder_path
                self.dbx.files_create_folder_v2(dbx_path)
            except ApiError as e:
                # If folder was created by another process between check and create
                if e.error.is_path() and e.error.get_path().is_conflict():
                    # Folder exists now, which is fine
                    pass
                else:
                    raise
    
    def upload_file(self, local_path: str, dropbox_folder: str) -> Dict[str, str]:
        """
        Upload a file into the App folder.
        Ensures folder exists before uploading.
        Automatically handles overwrites.
        
        Args:
            local_path: Path to local file to upload
            dropbox_folder: Folder path in App Folder (e.g., "SEF/2024")
            
        Returns:
            Dictionary with metadata:
            - 'path': Dropbox path of uploaded file
            - 'id': Dropbox file ID
            - 'link': Sharing link (if available)
            - 'size': File size in bytes
            - 'name': File name
            
        Raises:
            ValueError: If local file doesn't exist
            AuthError: If access token is invalid
            ApiError: For other Dropbox API errors
        """
        local_file = Path(local_path)
        
        # Validate local file exists
        if not local_file.exists():
            raise ValueError(f"Local file not found: {local_path}")
        
        if not local_file.is_file():
            raise ValueError(f"Path is not a file: {local_path}")
        
        # Ensure folder exists
        self.ensure_folder_exists(dropbox_folder)
        
        # Prepare Dropbox paths
        file_name = local_file.name
        dbx_folder_path = f"/{dropbox_folder.strip('/')}" if not dropbox_folder.startswith('/') else dropbox_folder
        dbx_file_path = f"{dbx_folder_path}/{file_name}"
        
        try:
            # Read file content
            with open(local_file, 'rb') as f:
                file_content = f.read()
            
            # Upload file with overwrite mode
            file_size = len(file_content)
            
            result = self.dbx.files_upload(
                file_content,
                dbx_file_path,
                mode=dropbox.files.WriteMode.overwrite
            )
            
            # Get sharing link (optional, continue without it if fails)
            sharing_link = None
            try:
                shared_link = self.dbx.sharing_create_shared_link_with_settings(dbx_file_path)
                sharing_link = shared_link.url
            except ApiError:
                # If sharing link creation fails, try to get existing link
                try:
                    shared_links = self.dbx.sharing_list_shared_links(path=dbx_file_path)
                    if shared_links.links:
                        sharing_link = shared_links.links[0].url
                except ApiError:
                    # If we can't get a sharing link, continue without it
                    pass
            
            return {
                'path': result.path_display,
                'id': result.id,
                'link': sharing_link or 'N/A',
                'size': file_size,
                'name': file_name,
                'client_modified': result.client_modified.isoformat() if result.client_modified else None,
                'server_modified': result.server_modified.isoformat() if result.server_modified else None,
            }
            
        except AuthError as e:
            raise ValueError(
                f"Dropbox authentication failed. Please check your access token. "
                f"Error: {str(e)}"
            ) from e
        except ApiError as e:
            error_msg = e.error.get_path() if e.error.is_path() else str(e.error)
            raise ValueError(
                f"Dropbox API error while uploading file: {error_msg}. "
                f"File: {local_path}, Dropbox path: {dbx_file_path}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"Unexpected error uploading file to Dropbox: {str(e)}. "
                f"File: {local_path}"
            ) from e

