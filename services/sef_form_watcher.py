"""
SEF Form Watcher Service.
Monitors Google Sheets form responses and automatically generates SEF PDFs for new entries.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.settings import (
    get_google_service_account_json,
    get_google_sheet_sef_id,
    get_google_sheet_sef_template_id,
)
from services.sef_google_template import SEFTemplateFiller
from services.tourist_tax import calculate_tourist_tax
from services.date_utils import normalize_date_safe

FORM_SHEET_NAME = "Form_Responses"
FULL_NAME_COLUMN = "Full Name"
STATE_FILE_PATH = Path(__file__).parent.parent / "database" / "sef_state.json"

# Column aliases matching Google Form field names
# Matching is case-insensitive and trims whitespace
COLUMN_ALIASES = {
    "check_in": ["check-in date", "check in date", "arrival date"],
    "check_in_time": ["what time do you would like to do the check-in?", "what time do you would like to do the check in?", "check-in time", "check in time"],
    "check_out": ["check-out date", "checkout date", "departure date"],
    "full_name": ["full name", "name"],
    "date_of_birth": ["date of birth", "dob"],
    "nationality": ["nationality"],
    "id_type": ["type of identification card", "document type"],
    "id_number": ["identification card number", "document number"],
    "country_issue": ["country of issue of the identification card"],
    "country_residence": ["country of residence"],
    "tourist_tax": ["tourist tax"],
    "allergies": ["do you have any allergies?", "allergies"],
}


class SEFFormWatcher:
    """
    Service for monitoring Google Sheets form responses and generating SEF PDFs.
    
    Watches the "Form_Responses" tab in the SEF Google Sheet and automatically
    generates PDF documents for new entries using the SEFTemplateFiller service.
    
    Usage Example:
        >>> watcher = SEFFormWatcher()
        >>> watcher.check_for_new_entries()
        [SEF Watcher] Checking for new form responses...
        [SEF Watcher] New guest detected: John Doe — PDF generated: /path/to/SEF_JohnDoe.pdf
    """
    
    def __init__(self):
        """
        Initialize SEF Form Watcher with Google Sheets API client.
        
        Loads service account credentials from environment variables and initializes
        Google Sheets API client. Also initializes SEFTemplateFiller for PDF generation.
        
        Raises:
            ValueError: If service account JSON cannot be loaded or parsed
            Exception: If API clients cannot be initialized
        """
        try:
            # Load service account credentials (same strategy as sef_google_template.py)
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
            
            # Get Sheet A ID (Form Responses) - where we read from
            self.form_sheet_id = get_google_sheet_sef_id()
            
            # Get Sheet B ID (Template Sheet) - where we write template and export PDF
            template_sheet_id = get_google_sheet_sef_template_id()
            if not template_sheet_id:
                # Fallback to form sheet if template ID not set (backwards compatibility)
                template_sheet_id = self.form_sheet_id
                print("[SEF Watcher] ⚠ GOOGLE_SHEET_SEF_TEMPLATE_ID not set, using form sheet for template")
            
            self.template_sheet_id = template_sheet_id
            
            # Initialize PDF generator with explicit template sheet ID
            self.template_filler = SEFTemplateFiller(template_sheet_id=template_sheet_id)
            
            # Ensure state file directory exists
            STATE_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
            
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse Google service account JSON: {str(e)}. "
                f"Please check your GOOGLE_SERVICE_ACCOUNT_JSON environment variable."
            ) from e
        except Exception as e:
            raise Exception(
                f"Failed to initialize SEF Form Watcher: {str(e)}. "
                f"Please verify your Google service account credentials and sheet ID."
            ) from e
    
    def _load_state(self) -> Dict[str, Any]:
        """
        Load the last processed row state from JSON file.
        
        Returns:
            Dictionary with state information, defaulting to {'last_processed_row': 0}
        """
        try:
            if STATE_FILE_PATH.exists():
                with open(STATE_FILE_PATH, 'r') as f:
                    state = json.load(f)
                    return state
            return {'last_processed_row': 0}
        except (json.JSONDecodeError, IOError) as e:
            print(f"[SEF Watcher] ⚠ Warning: Could not load state file: {e}. Starting from row 0.")
            return {'last_processed_row': 0}
    
    def _save_state(self, last_processed_row: int) -> None:
        """
        Save the last processed row state to JSON file.
        
        Args:
            last_processed_row: Row number that was last processed
        """
        try:
            state = {'last_processed_row': last_processed_row}
            with open(STATE_FILE_PATH, 'w') as f:
                json.dump(state, f, indent=2)
        except IOError as e:
            print(f"[SEF Watcher] ⚠ Warning: Could not save state file: {e}")
    
    def _get_column_index(self, header_row: List[str], column_name: str) -> Optional[int]:
        """
        Find the column index for a given column name (case-insensitive, trimmed).
        
        Args:
            header_row: List of header values from the first row
            column_name: Name of the column to find (will be matched case-insensitively)
        
        Returns:
            Column index (0-based) or None if not found
        """
        column_name_normalized = column_name.strip().lower()
        for idx, header in enumerate(header_row):
            if header and str(header).strip().lower() == column_name_normalized:
                return idx
        return None
    
    def _find_column_by_aliases(self, header_row: List[str], aliases: List[str]) -> Optional[int]:
        """
        Find column index by checking multiple possible aliases.
        
        Args:
            header_row: List of header values from the first row
            aliases: List of possible column names to match
        
        Returns:
            Column index (0-based) or None if not found
        """
        for alias in aliases:
            idx = self._get_column_index(header_row, alias)
            if idx is not None:
                return idx
        return None
    
    def _get_header_row(self) -> List[str]:
        """
        Get the header row (first row) from the Form_Responses sheet.
        
        Returns:
            List of header values
        
        Raises:
            Exception: If sheet or tab cannot be accessed
        """
        try:
            range_name = f"{FORM_SHEET_NAME}!1:1"
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.form_sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                raise Exception(f"No header row found in {FORM_SHEET_NAME} tab")
            
            return values[0]
        except HttpError as e:
            raise Exception(
                f"Failed to read header row from {FORM_SHEET_NAME}: {str(e)}. "
                f"Please verify the sheet ID and that the '{FORM_SHEET_NAME}' tab exists."
            ) from e
    
    def _find_full_name_column(self, header_row: List[str]) -> Optional[int]:
        """
        Find the "Full Name" column index using case-insensitive matching.
        
        Args:
            header_row: List of header values
        
        Returns:
            Column index (0-based) or None if not found
        """
        # Use aliases for full_name field
        aliases = COLUMN_ALIASES.get("full_name", ["full name", "name"])
        return self._find_column_by_aliases(header_row, aliases)
    
    def _get_last_filled_row(self, header_row: List[str]) -> int:
        """
        Find the last row with data in the "Full Name" column.
        
        Args:
            header_row: List of header values from the first row
        
        Returns:
            Row number (1-based) of the last filled row, or 1 if no data found
        """
        try:
            full_name_col_idx = self._find_full_name_column(header_row)
            if full_name_col_idx is None:
                raise Exception("Column for 'full name' not found in sheet")
            
            # Convert 0-based index to column letter (A=0, B=1, ..., Z=25, AA=26, etc.)
            def index_to_column_letter(idx: int) -> str:
                result = ""
                idx += 1  # Convert to 1-based
                while idx > 0:
                    idx -= 1
                    result = chr(ord('A') + (idx % 26)) + result
                    idx //= 26
                return result
            
            col_letter = index_to_column_letter(full_name_col_idx)
            
            # Read all values in the Full Name column (excluding header) from Sheet A
            range_name = f"{FORM_SHEET_NAME}!{col_letter}2:{col_letter}"
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.form_sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return 1  # Only header row exists
            
            # Find the last non-empty row
            last_row = 1  # Header is row 1
            for idx, row in enumerate(values):
                if row and len(row) > 0 and row[0] and str(row[0]).strip():
                    last_row = idx + 2  # +2 because header is row 1 and index is 0-based
            
            return last_row
            
        except HttpError as e:
            raise Exception(
                f"Failed to find last filled row: {str(e)}"
            ) from e
    
    def _extract_row_data(self, header_row: List[str], data_row: List[str]) -> Optional[Dict[str, str]]:
        """
        Extract guest data from a row using column aliases (case-insensitive matching).
        
        Args:
            header_row: List of header values (column names)
            data_row: List of data values from the row
        
        Returns:
            Dictionary with guest data containing all required fields, or None if full_name is missing
        """
        guest_data = {}
        
        # Build column index mapping using case-insensitive alias matching
        column_indices = {}
        for field, aliases in COLUMN_ALIASES.items():
            idx = self._find_column_by_aliases(header_row, aliases)
            if idx is not None:
                column_indices[field] = idx
        
        # Extract values for all expected fields
        expected_fields = [
            "check_in", "check_in_time", "check_out",
            "full_name", "date_of_birth", "nationality",
            "id_type", "id_number",
            "country_issue", "country_residence",
            "allergies"
        ]
        
        for field in expected_fields:
            if field in column_indices:
                col_idx = column_indices[field]
                if col_idx < len(data_row):
                    value = data_row[col_idx]
                    raw_value = str(value).strip() if value else ""
                    
                    # Normalize date fields (but NOT check_in_time - it's a time, not a date)
                    if field in ["check_in", "check_out", "date_of_birth"]:
                        guest_data[field] = normalize_date_safe(raw_value)
                        if raw_value and not guest_data[field]:
                            print(f"[SEF Watcher] ⚠ Warning: Could not normalize {field} value '{raw_value}', leaving empty")
                    else:
                        # For non-date fields (including check_in_time and allergies), use raw value
                        guest_data[field] = raw_value
                else:
                    guest_data[field] = ""
            else:
                # Field not found in sheet, set empty
                guest_data[field] = ""
        
        # Validate that at least full_name is present
        if not guest_data.get('full_name'):
            return None
        
        return guest_data
    
    def _process_new_row(self, row_num: int, header_row: List[str]) -> Optional[Tuple[str, str]]:
        """
        Process a new row: extract data, generate PDF, return guest name and PDF path.
        
        Args:
            row_num: Row number to process (1-based)
            header_row: List of header values
        
        Returns:
            Tuple of (full_name, pdf_path) if processing succeeded, None otherwise
        """
        try:
            # Read the row data from Sheet A (Form Responses)
            range_name = f"{FORM_SHEET_NAME}!{row_num}:{row_num}"
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.form_sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values or not values[0]:
                return None
            
            data_row = values[0]
            
            # Extract guest data
            guest_data = self._extract_row_data(header_row, data_row)
            if not guest_data:
                print(f"[SEF Watcher] ⚠ Row {row_num}: No valid guest data found (missing full_name), skipping")
                return None
            
            # Validate required fields are present
            full_name = guest_data.get('full_name', '').strip()
            if not full_name:
                print(f"[SEF Watcher] ⚠ Row {row_num}: Missing full_name, skipping")
                return None
            
            # Calculate tourist tax after extracting dates
            # Dates are already normalized in _extract_row_data()
            check_in = guest_data.get('check_in', '').strip()
            check_out = guest_data.get('check_out', '').strip()
            date_of_birth = guest_data.get('date_of_birth', '').strip()
            
            if check_in and check_out and date_of_birth:
                try:
                    # Dates are already normalized, but ensure they're in correct format
                    tax_amount = calculate_tourist_tax(check_in, check_out, date_of_birth)
                    guest_data['tourist_tax'] = f"{tax_amount} EUR"
                    print(f"[SEF Watcher] Tourist tax calculated: {guest_data['tourist_tax']} (dates: check_in={check_in}, check_out={check_out}, dob={date_of_birth})")
                except ValueError as tax_error:
                    print(f"[SEF Watcher] ⚠ Row {row_num}: Tourist tax calculation failed: {tax_error}")
                    # Continue with empty tax value if calculation fails
                    guest_data['tourist_tax'] = ""
            else:
                # Missing required dates for tax calculation
                print(f"[SEF Watcher] ⚠ Row {row_num}: Missing dates for tourist tax calculation (check_in={check_in}, check_out={check_out}, date_of_birth={date_of_birth})")
                guest_data['tourist_tax'] = ""
            
            # Ensure all fields required by template are present
            # Add any missing fields with empty values
            required_template_fields = [
                "check_in", "check_in_time", "check_out",
                "full_name", "date_of_birth", "nationality",
                "id_type", "id_number",
                "country_issue", "country_residence",
                "tourist_tax", "allergies"
            ]
            for field in required_template_fields:
                if field not in guest_data:
                    guest_data[field] = ""
            
            # Generate PDF filename
            safe_name = full_name.replace(' ', '_').replace('/', '_')[:50]  # Sanitize filename
            pdf_filename = f"SEF_{safe_name}.pdf"
            
            # Fill template in Sheet B and export PDF
            print(f"[SEF Watcher] Processing row {row_num}: {full_name}")
            print(f"[SEF Watcher] Filling SEF template in Sheet B...")
            self.template_filler.fill_template(guest_data)
            pdf_path = self.template_filler.export_pdf(pdf_filename)
            
            # Upload PDF to Dropbox in date-based folder structure
            check_in_date = guest_data.get('check_in', '').strip()
            if check_in_date:
                try:
                    # Build folder name from check-in date (already normalized to YYYY-MM-DD)
                    dropbox_folder = f"/SEF/{check_in_date}"
                    dropbox_path = self.template_filler.upload_to_dropbox(str(pdf_path), dropbox_folder)
                    print(f"[SEF Watcher] PDF uploaded to Dropbox → {dropbox_path}")
                except Exception as upload_error:
                    print(f"[SEF Watcher] ⚠ Warning: Failed to upload PDF to Dropbox: {upload_error}")
                    # Continue even if upload fails
            else:
                print(f"[SEF Watcher] ⚠ Warning: Cannot upload to Dropbox - missing check_in date")
            
            # Log tourist tax if it was calculated
            if guest_data.get('tourist_tax') and 'EUR' in str(guest_data.get('tourist_tax', '')):
                tax_value = guest_data['tourist_tax'].replace(' EUR', '')
                print(f"[SEF Watcher] ✓ Tourist tax applied: {tax_value} EUR")
            
            return full_name, str(pdf_path)
            
        except Exception as e:
            print(f"[SEF Watcher] ✗ Error processing row {row_num}: {str(e)}")
            return None
    
    def check_for_new_entries(self) -> None:
        """
        Check the sheet for new responses and generate PDFs if needed.
        
        This method is non-blocking and performs a single check per call.
        It compares the current last filled row with the last processed row
        stored in the state file, processes any new rows, and updates the state.
        
        Raises:
            Exception: If sheet cannot be accessed or processing fails
        """
        try:
            print("[SEF Watcher] Checking for new form responses...")
            print(f"[SEF Watcher] Reading new form responses from Sheet A...")
            
            # Load last processed row
            state = self._load_state()
            last_processed = state.get('last_processed_row', 0)
            
            # Get header row from Sheet A
            header_row = self._get_header_row()
            
            # Find current last filled row in Sheet A
            last_filled_row = self._get_last_filled_row(header_row)
            
            print(f"[SEF Watcher] Last processed row: {last_processed}, Last filled row: {last_filled_row}")
            
            # Check if there are new rows to process
            if last_filled_row <= last_processed:
                print("[SEF Watcher] No new entries found")
                return
            
            # Process new rows (from last_processed + 1 to last_filled_row)
            new_row_count = last_filled_row - last_processed
            print(f"[SEF Watcher] Found {new_row_count} new row(s) to process")
            
            processed_count = 0
            for row_num in range(last_processed + 1, last_filled_row + 1):
                result = self._process_new_row(row_num, header_row)
                if result:
                    full_name, pdf_path = result
                    print(f"[SEF Watcher] New guest detected: {full_name} — PDF generated: {pdf_path}")
                    print(f"[SEF Watcher] PDF generated -> {pdf_path}")
                    processed_count += 1
                    # Update state after each successful processing
                    self._save_state(row_num)
                else:
                    # Skip incomplete rows but update state to avoid reprocessing
                    print(f"[SEF Watcher] Row {row_num}: Incomplete data, skipping but marking as processed")
                    self._save_state(row_num)
            
            print(f"[SEF Watcher] ✓ Processed {processed_count} new entry/entries")
            
        except Exception as e:
            error_msg = (
                f"Failed to check for new entries: {str(e)}. "
                f"Please verify Sheet A (Form Responses) ID and that the '{FORM_SHEET_NAME}' tab exists, "
                f"and that Sheet B (Template) is accessible."
            )
            print(f"[SEF Watcher] ✗ {error_msg}")
            raise Exception(error_msg) from e

