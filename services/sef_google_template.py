"""
SEF Google Template Service.
Generates SEF PDF documents by filling a Google Sheets template and exporting to PDF.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO

from config.settings import (
    get_google_service_account_json,
    get_google_sheet_sef_id,
    get_google_sheet_sef_template_id,
    get_dropbox_access_token,
)
from services.date_utils import normalize_date_safe
import dropbox
from dropbox.exceptions import ApiError


# Field mapping: guest data field → cell reference in Google Sheets template
FIELD_MAP = {
    "check_in": "B7",
    "check_in_time": "E7",
    "check_out": "H7",
    "full_name": "B10",
    "date_of_birth": "B13",
    "nationality": "H13",
    "id_type": "B16",
    "id_number": "H16",
    "country_issue": "B20",
    "country_residence": "H19",
    "tourist_tax": "E22",
    "allergies": "H22",
}

TEMPLATE_SHEET_NAME = "Template"


class SEFTemplateFiller:
    """
    Service for filling SEF Google Sheets template and exporting to PDF.
    
    This class loads a Google Sheets template, fills specific cells with guest data,
    and exports the filled template as a PDF file.
    
    Usage Example:
        >>> from services.tourist_tax import calculate_tourist_tax
        >>> f = SEFTemplateFiller()
        >>> data = {
        ...     "check_in": "2025-06-01",
        ...     "check_out": "2025-06-05",
        ...     "full_name": "Test Example",
        ...     "date_of_birth": "2010-06-03",
        ...     "nationality": "British",
        ...     "id_type": "Passport",
        ...     "id_number": "AB123456",
        ...     "country_issue": "United Kingdom",
        ...     "country_residence": "United Kingdom",
        ...     "tourist_tax": calculate_tourist_tax("2025-06-01","2025-06-05","2010-06-03")
        ... }
        >>> f.fill_template(data)
        >>> pdf_path = f.export_pdf("example.pdf")
        >>> print("Generated:", pdf_path)
    """
    
    def __init__(self, template_sheet_id: Optional[str] = None):
        """
        Initialize SEF Template Filler with Google Sheets and Drive API clients.
        
        Loads service account credentials from environment variables and initializes
        Google Sheets and Drive API clients with required scopes.
        
        Args:
            template_sheet_id: Optional explicit template sheet ID. If not provided,
                             uses GOOGLE_SHEET_SEF_TEMPLATE_ID from settings, or falls
                             back to GOOGLE_SHEET_SEF_ID for backwards compatibility.
        
        Raises:
            ValueError: If service account JSON cannot be loaded or parsed
            Exception: If API clients cannot be initialized
        """
        try:
            # Load service account credentials
            service_account_json_str = get_google_service_account_json()
            
            # Handle both file path and JSON string
            service_account_json_path = Path(service_account_json_str)
            if service_account_json_path.exists():
                with open(service_account_json_path, 'r') as f:
                    service_account_info = json.load(f)
            else:
                # Parse as JSON string
                service_account_info = json.loads(service_account_json_str)
            
            # Create credentials with required scopes
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            
            # Initialize Google Sheets API client
            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            
            # Initialize Google Drive API client
            self.drive_service = build('drive', 'v3', credentials=credentials)
            
            # Initialize Dropbox client
            try:
                dropbox_token = get_dropbox_access_token()
                self.dbx = dropbox.Dropbox(dropbox_token)
            except Exception as e:
                print(f"[SEF Template] ⚠ Warning: Could not initialize Dropbox client: {str(e)}")
                self.dbx = None
            
            # Determine template sheet ID (priority: parameter > template ID > fallback to SEF ID)
            if template_sheet_id:
                self.sheet_id = template_sheet_id
            else:
                template_id = get_google_sheet_sef_template_id()
                if template_id:
                    self.sheet_id = template_id
                else:
                    # Backwards compatibility: fall back to GOOGLE_SHEET_SEF_ID
                    self.sheet_id = get_google_sheet_sef_id()
            
            # Validate that Template tab exists
            self._validate_template_tab()
            
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse Google service account JSON: {str(e)}. "
                f"Please check your GOOGLE_SERVICE_ACCOUNT_JSON environment variable."
            ) from e
        except Exception as e:
            raise Exception(
                f"Failed to initialize SEF Template Filler: {str(e)}. "
                f"Please verify your Google service account credentials and sheet ID."
            ) from e
    
    def _validate_template_tab(self) -> None:
        """
        Validate that the Template tab exists in the sheet.
        
        Raises:
            Exception: If Template tab does not exist
        """
        try:
            # Try to get sheet metadata
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=self.sheet_id
            ).execute()
            
            sheets = spreadsheet.get('sheets', [])
            sheet_names = [sheet['properties']['title'] for sheet in sheets]
            
            if TEMPLATE_SHEET_NAME not in sheet_names:
                available_tabs = ', '.join(sheet_names) if sheet_names else 'none'
                raise Exception(
                    f"Template tab '{TEMPLATE_SHEET_NAME}' not found in sheet. "
                    f"Available tabs: {available_tabs}. "
                    f"Please ensure the template sheet has a tab named '{TEMPLATE_SHEET_NAME}'."
                )
        except HttpError as e:
            raise Exception(
                f"Cannot access template sheet (ID: {self.sheet_id}): {str(e)}. "
                f"Please verify the sheet ID and that the service account has access."
            ) from e
    
    def fill_template(self, data: Dict) -> None:
        """
        Fill the Google Sheets template with guest data.
        
        Writes guest data to specific cells in the template sheet based on FIELD_MAP.
        Uses batch update with USER_ENTERED value input option for proper formatting.
        
        Args:
            data: Dictionary with guest data. Keys should match FIELD_MAP keys:
                - check_in: Check-in date
                - check_out: Check-out date
                - full_name: Guest full name
                - date_of_birth: Date of birth
                - nationality: Guest nationality
                - id_type: ID document type (e.g., "Passport")
                - id_number: ID document number
                - country_issue: Country where ID was issued
                - country_residence: Country of residence
                - tourist_tax: Tourist tax payment status
        
        Raises:
            ValueError: If required fields are missing or sheet/tab not found
            Exception: If Google Sheets API call fails
        """
        try:
            # Prepare batch update data
            data_updates = []
            
            for field_name, cell_ref in FIELD_MAP.items():
                if field_name in data:
                    value = data[field_name]
                    # Normalize date fields before writing to template
                    # Note: check_in_time is NOT normalized (it's a time, not a date)
                    if field_name in ["check_in", "check_out", "date_of_birth"]:
                        # Normalize date to YYYY-MM-DD format
                        cell_value = normalize_date_safe(value) if value else ""
                        if value and not cell_value:
                            print(f"[SEF Template] ⚠ Warning: Could not normalize {field_name} value '{value}', leaving cell blank")
                    else:
                        # Convert to string if not already (for all other fields including check_in_time and allergies)
                        if value is not None:
                            cell_value = str(value)
                        else:
                            cell_value = ""
                    
                    # Create range in A1 notation (e.g., "Template!B7")
                    range_name = f"{TEMPLATE_SHEET_NAME}!{cell_ref}"
                    
                    data_updates.append({
                        'range': range_name,
                        'values': [[cell_value]]
                    })
            
            if not data_updates:
                raise ValueError(
                    "No valid data fields provided. Expected fields: " +
                    ", ".join(FIELD_MAP.keys())
                )
            
            # Perform batch update
            body = {
                'valueInputOption': 'USER_ENTERED',
                'data': data_updates
            }
            
            result = self.sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.sheet_id,
                body=body
            ).execute()
            
            updated_cells = result.get('totalUpdatedCells', 0)
            print(f"[SEF Template] Filled {updated_cells} cells in template")
            
        except HttpError as e:
            error_msg = (
                f"Failed to fill template in sheet (ID: {self.sheet_id}): {str(e)}. "
                f"Please verify the sheet ID, that the '{TEMPLATE_SHEET_NAME}' tab exists, "
                f"and that the service account has write permissions."
            )
            raise Exception(error_msg) from e
        except Exception as e:
            error_msg = (
                f"Failed to fill template: {str(e)}. "
                f"Please verify the sheet ID and that the '{TEMPLATE_SHEET_NAME}' tab exists."
            )
            raise Exception(error_msg) from e
    
    def export_pdf(self, output_name: str, output_dir: Path = None) -> Path:
        """
        Export the filled Google Sheets template to PDF.
        
        Uses Google Drive API to export the spreadsheet as PDF and saves it locally.
        
        Args:
            output_name: Name for the output PDF file (e.g., "SEF_DavidBrown.pdf")
            output_dir: Optional directory path to save PDF. Defaults to current directory.
        
        Returns:
            Path object pointing to the generated PDF file
        
        Raises:
            ValueError: If output_name is invalid or export fails
            Exception: If Google Drive API call fails or file cannot be saved
        """
        try:
            if not output_name:
                raise ValueError("Output file name cannot be empty")
            
            # Ensure .pdf extension
            if not output_name.lower().endswith('.pdf'):
                output_name = f"{output_name}.pdf"
            
            # Set output directory
            if output_dir is None:
                output_dir = Path.cwd() / "sef_pdfs"
            else:
                output_dir = Path(output_dir)
            
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_path = output_dir / output_name
            
            # Export spreadsheet as PDF using Drive API
            print(f"[SEF Template] Exporting spreadsheet to PDF: {output_name}")
            
            request = self.drive_service.files().export_media(
                fileId=self.sheet_id,
                mimeType='application/pdf'
            )
            
            # Download PDF content
            file_handle = BytesIO()
            downloader = MediaIoBaseDownload(file_handle, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"[SEF Template] Download progress: {int(status.progress() * 100)}%")
            
            # Save PDF to local file
            file_handle.seek(0)
            with open(output_path, 'wb') as f:
                f.write(file_handle.read())
            
            file_size = output_path.stat().st_size
            print(f"[SEF Template] ✓ PDF exported successfully: {output_path}")
            print(f"[SEF Template]   File size: {file_size} bytes")
            
            # Clear template after PDF generation
            self.clear_template()
            
            return output_path
            
        except Exception as e:
            error_msg = (
                f"Failed to export PDF: {str(e)}. "
                f"Please verify the sheet ID and Drive API permissions."
            )
            raise Exception(error_msg) from e
    
    def clear_template(self) -> None:
        """
        Clears all template fields after PDF generation while preserving formatting.
        
        Clears the following cells:
        - B7, E7, H7 (check-in date, check-in time, check-out date)
        - B10 (full name)
        - B13, H13 (date of birth, nationality)
        - B16, H16 (id type, id number)
        - B20, H19 (country of issue, country of residence)
        - H22, E22 (allergies, tourist tax)
        
        Uses batchUpdate with USER_ENTERED to preserve cell formatting.
        """
        try:
            # Define all cells to clear
            cells_to_clear = [
                "B7",   # check_in
                "E7",   # check_in_time
                "H7",   # check_out
                "B10",  # full_name
                "B13",  # date_of_birth
                "H13",  # nationality
                "B16",  # id_type
                "H16",  # id_number
                "B20",  # country_issue
                "H19",  # country_residence
                "H22",  # allergies
                "E22",  # tourist_tax
            ]
            
            # Prepare batch update data
            data_updates = []
            for cell_ref in cells_to_clear:
                range_name = f"{TEMPLATE_SHEET_NAME}!{cell_ref}"
                data_updates.append({
                    'range': range_name,
                    'values': [[""]]
                })
            
            # Perform batch update
            body = {
                'valueInputOption': 'USER_ENTERED',
                'data': data_updates
            }
            
            result = self.sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.sheet_id,
                body=body
            ).execute()
            
            cleared_cells = result.get('totalUpdatedCells', 0)
            print(f"[SEF Template] Cleared {cleared_cells} cells in template")
            
        except HttpError as e:
            print(f"[SEF Template] ⚠ Warning: Failed to clear template: {str(e)}")
            # Don't raise - clearing is not critical
        except Exception as e:
            print(f"[SEF Template] ⚠ Warning: Failed to clear template: {str(e)}")
            # Don't raise - clearing is not critical
    
    def upload_to_dropbox(self, pdf_path: str, dropbox_folder: str) -> str:
        """
        Upload PDF to Dropbox in a structured folder hierarchy.
        
        Creates the destination folder if it doesn't exist, then uploads the PDF.
        Uses overwrite mode to replace existing files with the same name.
        
        Args:
            pdf_path: Local path to the PDF file
            dropbox_folder: Dropbox folder path (e.g., "/SEF/2025-12-05")
        
        Returns:
            Final Dropbox path of the uploaded file (e.g., "/SEF/2025-12-05/SEF_JohnDoe.pdf")
        
        Raises:
            ValueError: If Dropbox client is not initialized or PDF file doesn't exist
            Exception: If upload fails
        """
        if self.dbx is None:
            raise ValueError("Dropbox client is not initialized. Check DROPBOX_ACCESS_TOKEN environment variable.")
        
        try:
            pdf_file = Path(pdf_path)
            if not pdf_file.exists():
                raise ValueError(f"PDF file not found: {pdf_path}")
            
            # Ensure dropbox_folder starts with /
            if not dropbox_folder.startswith('/'):
                dropbox_folder = f"/{dropbox_folder}"
            
            # Ensure folder exists (ignore error if it already exists)
            # First, ensure parent folder (/SEF) exists
            parent_folder = '/SEF'
            if dropbox_folder != parent_folder:
                try:
                    self.dbx.files_create_folder_v2(parent_folder)
                except ApiError as e:
                    if not (e.error.is_path() and e.error.get_path().is_conflict()):
                        # If it's not a conflict (already exists), log but continue
                        pass
            
            # Now create the date-specific folder
            try:
                self.dbx.files_create_folder_v2(dropbox_folder)
            except ApiError as e:
                if e.error.is_path() and e.error.get_path().is_conflict():
                    # Folder already exists, which is fine
                    pass
                else:
                    # Other errors, log but continue (folder might still work for upload)
                    print(f"[SEF Template] ⚠ Warning: Could not create folder {dropbox_folder}: {str(e)}")
            
            # Build final Dropbox path
            filename = pdf_file.name
            dropbox_file_path = f"{dropbox_folder.rstrip('/')}/{filename}"
            
            # Upload file with overwrite mode
            with open(pdf_file, 'rb') as f:
                file_data = f.read()
                self.dbx.files_upload(
                    file_data,
                    dropbox_file_path,
                    mode=dropbox.files.WriteMode.overwrite
                )
            
            print(f"[SEF Template] ✓ PDF uploaded to Dropbox: {dropbox_file_path}")
            return dropbox_file_path
            
        except Exception as e:
            error_msg = (
                f"Failed to upload PDF to Dropbox: {str(e)}. "
                f"File: {pdf_path}, Folder: {dropbox_folder}"
            )
            raise Exception(error_msg) from e
