"""
Google Sheets integration service.
Handles reading from and writing to Google Sheets for data storage.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.settings import (
    get_google_service_account_json,
    get_google_sheet_reservations_id,
    get_google_sheet_sef_id
)

# Configure logging
logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Service class for interacting with Google Sheets API."""
    
    # Expected headers for reservations sheet
    RESERVATION_HEADERS = [
        "reservation_id", "origin", "lodgify_id", "airbnb_id", "guest_name", "guest_email",
        "guest_phone", "guests_count", "check_in", "check_out", "nights", "currency",
        "total_price", "country", "reservation_date", "status", "airbnb_fee", "lodgify_fee",
        "stripe_fee", "vat_amount", "net_revenue", "anticipation_days",
        "price_per_night", "price_per_guest_per_night", "welcome_sent"
    ]
    
    def __init__(self):
        """
        Initialize Google Sheets client with service account credentials.
        
        Handles both file paths and JSON strings for service account credentials:
        - If the value ends with '.json', treats it as a file path and loads from file
        - Otherwise, treats it as a JSON string and parses it directly
        """
        try:
            import os
            
            # Get the service account JSON value from environment variable
            service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
            if not service_account_json:
                # Fallback to config.settings function if env var not directly available
                service_account_json = get_google_service_account_json()
            
            # Determine if it's a file path or JSON string
            # CASE A: If it ends with ".json", treat as file path
            if service_account_json.strip().endswith('.json'):
                service_account_path = Path(service_account_json)
                
                # Handle relative paths (relative to project root)
                if not service_account_path.is_absolute():
                    # Get project root (parent of services directory)
                    project_root = Path(__file__).parent.parent
                    service_account_path = project_root / service_account_path
                
                # Check if file exists
                if not service_account_path.exists():
                    error_msg = f"Service account JSON file not found: {service_account_path}"
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)
                
                # Load from file
                logger.debug(f"Loading service account from file: {service_account_path}")
                try:
                    with open(service_account_path, 'r', encoding='utf-8') as f:
                        self.service_account_info = json.load(f)
                    logger.info(f"Successfully loaded service account from file: {service_account_path}")
                except json.JSONDecodeError as e:
                    error_msg = f"Failed to parse JSON from file {service_account_path}: {str(e)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg) from e
                except Exception as e:
                    error_msg = f"Error reading service account file {service_account_path}: {str(e)}"
                    logger.error(error_msg)
                    raise
            else:
                # CASE B: Treat as JSON string
                logger.debug("Parsing service account as JSON string")
                try:
                    self.service_account_info = json.loads(service_account_json)
                    logger.info("Successfully parsed service account JSON string")
                except json.JSONDecodeError as e:
                    error_msg = f"Failed to parse service account JSON string: {str(e)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg) from e
            
            # Load sheet IDs
            self.reservations_sheet_id = get_google_sheet_reservations_id()
            self.sef_sheet_id = get_google_sheet_sef_id()
            
            # Create credentials with required scopes
            credentials = service_account.Credentials.from_service_account_info(
                self.service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive.readonly'
                ]
            )
            
            # Build Google Sheets API service
            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            
            logger.info("GoogleSheetsService initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize GoogleSheetsService: {str(e)}", exc_info=True)
            raise
    
    def read_range(self, sheet_id: str, range_name: str) -> List[List]:
        """
        Read data from a specific range in a Google Sheet.
        
        Args:
            sheet_id: Google Sheet ID
            range_name: A1 notation range (e.g., 'Sheet1!A1:C10')
            
        Returns:
            List of rows, where each row is a list of cell values
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            return values
            
        except HttpError as e:
            logger.error(f"Error reading range {range_name} from sheet {sheet_id}: {str(e)}")
            raise
    
    def write_range(self, sheet_id: str, range_name: str, values: List[List]) -> None:
        """
        Write data to a specific range in a Google Sheet.
        
        Args:
            sheet_id: Google Sheet ID
            range_name: A1 notation range (e.g., 'Sheet1!A1:C10')
            values: List of rows to write, where each row is a list of cell values
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            body = {
                'values': values
            }
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.debug(f"Successfully wrote {len(values)} rows to {range_name}")
            
        except HttpError as e:
            logger.error(f"Error writing to range {range_name} in sheet {sheet_id}: {str(e)}")
            raise
    
    def append_row(self, sheet_id: str, sheet_name: str, values: List) -> None:
        """
        Append a new row to a Google Sheet.
        
        Args:
            sheet_id: Google Sheet ID
            sheet_name: Name of the sheet/tab
            values: List of cell values for the new row
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            range_name = f"{sheet_name}!A:A"  # Append to column A to auto-detect end
            body = {
                'values': [values]
            }
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            logger.debug(f"Successfully appended row to {sheet_name}")
            
        except HttpError as e:
            logger.error(f"Error appending row to {sheet_name} in sheet {sheet_id}: {str(e)}")
            raise
    
    def get_reservations_data(self) -> List[Dict]:
        """
        Get all reservations data from the reservations Google Sheet.
        
        Returns:
            List of reservation dictionaries
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            range_name = "reservations!A:Z"  # Read all columns
            values = self.read_range(self.reservations_sheet_id, range_name)
            
            if not values:
                return []
            
            # First row is headers
            headers = values[0]
            reservations = []
            
            # Map rows to dictionaries
            for row in values[1:]:
                reservation = {}
                for i, header in enumerate(headers):
                    reservation[header] = row[i] if i < len(row) else ""
                reservations.append(reservation)
            
            return reservations
            
        except HttpError as e:
            logger.error(f"Error getting reservations data: {str(e)}")
            raise
    
    def get_sef_data(self) -> List[Dict]:
        """
        Get all SEF data from the Form_Responses tab in the SEF Google Sheet.
        
        Returns:
            List of SEF form dictionaries
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            range_name = "Form_Responses!A:Z"  # Read all columns from Form_Responses tab
            values = self.read_range(self.sef_sheet_id, range_name)
            
            if not values:
                return []
            
            # First row is headers
            headers = values[0]
            sef_forms = []
            
            # Map rows to dictionaries
            for row in values[1:]:
                sef_form = {}
                for i, header in enumerate(headers):
                    sef_form[header] = row[i] if i < len(row) else ""
                sef_forms.append(sef_form)
            
            return sef_forms
            
        except HttpError as e:
            logger.error(f"Error getting SEF data: {str(e)}")
            raise
    
    def get_invoices_data(self, sheet_id: str) -> List[Dict]:
        """
        Get all invoice data from the Invoices_Lettia sheet.
        
        Args:
            sheet_id: Google Sheet ID containing the Invoices_Lettia sheet
            
        Returns:
            List of invoice dictionaries
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            range_name = "Invoices_Lettia!A:Z"  # Read all columns
            values = self.read_range(sheet_id, range_name)
            
            if not values:
                return []
            
            # First row is headers
            headers = values[0]
            invoices = []
            
            # Map rows to dictionaries
            for row in values[1:]:
                invoice = {}
                for i, header in enumerate(headers):
                    invoice[header] = row[i] if i < len(row) else ""
                invoices.append(invoice)
            
            return invoices
            
        except HttpError as e:
            logger.error(f"Error getting invoices data: {str(e)}")
            raise
    
    def get_headers(self, sheet_name: str, sheet_id: Optional[str] = None) -> List[str]:
        """
        Get headers from a specific sheet.
        
        Args:
            sheet_name: Name of the sheet/tab
            sheet_id: Optional Google Sheet ID (defaults to reservations_sheet_id)
        
        Returns:
            List of header names
            
        Raises:
            HttpError: If Google Sheets API request fails
            ValueError: If sheet is empty or headers not found
        """
        try:
            target_sheet_id = sheet_id if sheet_id else self.reservations_sheet_id
            header_range = f"{sheet_name}!1:1"
            header_result = self.read_range(target_sheet_id, header_range)
            
            if not header_result or not header_result[0]:
                raise ValueError(f"Could not read header row from {sheet_name} sheet")
            
            headers = header_result[0]
            logger.debug(f"Retrieved {len(headers)} headers from {sheet_name} sheet")
            return headers
            
        except HttpError as e:
            logger.error(f"Error reading headers from {sheet_name} sheet: {str(e)}")
            raise
    
    def get_sheet_gid(self, sheet_id: str, sheet_name: str) -> int:
        """
        Get the sheet ID (GID) for a given sheet name.
        
        Args:
            sheet_id: Google Sheet ID
            sheet_name: Name of the sheet/tab
            
        Returns:
            Sheet ID (GID) as integer
            
        Raises:
            HttpError: If Google Sheets API request fails
            ValueError: If sheet name not found
        """
        try:
            metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            
            for sheet in metadata.get("sheets", []):
                if sheet.get("properties", {}).get("title") == sheet_name:
                    return sheet["properties"]["sheetId"]
            
            raise ValueError(f"Sheet '{sheet_name}' not found")
            
        except HttpError as e:
            logger.error(f"Error getting sheet GID for {sheet_name}: {str(e)}")
            raise
    
    def set_cell_note(self, sheet_id: str, row_index: int, col_index: int, note_text: str, sheet_name: str = "reservations") -> None:
        """
        Set a note on a specific cell in Google Sheets.
        
        Args:
            sheet_id: Google Sheet ID
            row_index: Row index (0-based)
            col_index: Column index (0-based)
            note_text: Text content for the note
            sheet_name: Name of the sheet/tab
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            sheet_gid = self.get_sheet_gid(sheet_id, sheet_name)
            
            body = {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_gid,
                                "startRowIndex": row_index,
                                "endRowIndex": row_index + 1,
                                "startColumnIndex": col_index,
                                "endColumnIndex": col_index + 1,
                            },
                            "cell": {
                                "note": note_text
                            },
                            "fields": "note"
                        }
                    }
                ]
            }
            
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body=body
            ).execute()
            
            logger.debug(f"Set note on cell at row {row_index + 1}, col {col_index + 1}")
            
        except HttpError as e:
            logger.error(f"Error setting cell note: {str(e)}")
            raise
    
    def set_cell_notes_batch(self, sheet_id: str, row_index: int, notes_map: Dict[str, str], headers: List[str], sheet_name: str = "reservations") -> None:
        """
        Set multiple cell notes in a single batch operation.
        
        Args:
            sheet_id: Google Sheet ID
            row_index: Row index (0-based)
            notes_map: Dictionary mapping field names to note text
            headers: List of header names (used to find column indices)
            sheet_name: Name of the sheet/tab
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        if not notes_map:
            return
        
        try:
            sheet_gid = self.get_sheet_gid(sheet_id, sheet_name)
            
            requests = []
            for field, note_text in notes_map.items():
                if field in headers:
                    try:
                        col_idx = headers.index(field)
                        requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_gid,
                                    "startRowIndex": row_index,
                                    "endRowIndex": row_index + 1,
                                    "startColumnIndex": col_idx,
                                    "endColumnIndex": col_idx + 1,
                                },
                                "cell": {
                                    "note": note_text
                                },
                                "fields": "note"
                            }
                        })
                    except ValueError:
                        logger.warning(f"Field '{field}' not found in headers, skipping note")
                        continue
            
            if requests:
                body = {"requests": requests}
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body=body
                ).execute()
                logger.debug(f"Set {len(requests)} notes on row {row_index + 1}")
            
        except HttpError as e:
            logger.error(f"Error setting cell notes in batch: {str(e)}")
            raise
    
    def delete_row(self, sheet_id: str, row_number: int) -> None:
        """
        Delete a row from a Google Sheet.
        
        Args:
            sheet_id: Google Sheet ID
            row_number: Row number to delete (1-indexed)
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        try:
            sheet_gid = self.get_sheet_gid(sheet_id, "reservations")
            
            body = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_gid,
                                "dimension": "ROWS",
                                "startIndex": row_number - 1,
                                "endIndex": row_number
                            }
                        }
                    }
                ]
            }
            
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body=body
            ).execute()
            
            logger.debug(f"Deleted row {row_number} from reservations sheet")
            
        except HttpError as e:
            logger.error(f"Error deleting row {row_number}: {str(e)}")
            raise
    
    def delete_rows_batch(self, sheet_id: str, sheet_name: str, rows_to_delete: List[int]) -> None:
        """
        Delete multiple rows from a Google Sheet in a single batch operation.
        
        Args:
            sheet_id: Google Sheet ID
            sheet_name: Name of the sheet/tab
            rows_to_delete: List of row numbers to delete (1-indexed, must be sorted descending)
            
        Raises:
            HttpError: If Google Sheets API request fails
        """
        if not rows_to_delete:
            return
        
        try:
            sheet_gid = self.get_sheet_gid(sheet_id, sheet_name)
            
            # rows_to_delete must be sorted descending
            requests = []
            for r in rows_to_delete:
                requests.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_gid,
                            "dimension": "ROWS",
                            "startIndex": r - 1,
                            "endIndex": r
                        }
                    }
                })
            
            body = {"requests": requests}
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id, body=body
            ).execute()
            
            logger.info(f"Deleted {len(rows_to_delete)} rows in batch from {sheet_name} sheet")
            
        except HttpError as e:
            logger.error(f"Error deleting rows in batch: {str(e)}")
            raise
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from the 'config' sheet.
        
        Reads the following fields:
        - VAT_RATE
        - AIRBNB_FEE_PERCENT
        - LODGIFY_FEE_PERCENT
        - STRIPE_FEE_TABLE (JSON string → converted to Python dict)
        
        Expected sheet format:
        Row 1 (headers): ["VAT_RATE", "AIRBNB_FEE_PERCENT", "LODGIFY_FEE_PERCENT", "STRIPE_FEE_TABLE"]
        Row 2 (data): ["0.06", "0.15", "0.03", '{"PT":0.014,"FR":0.016,...}']
        
        Returns:
            Dictionary with configuration values:
            {
                "VAT_RATE": 0.06,
                "AIRBNB_FEE_PERCENT": 0.15,
                "LODGIFY_FEE_PERCENT": 0.03,
                "STRIPE_FEE_TABLE": {"PT": 0.014, "FR": 0.016, ...}
            }
            
        Raises:
            HttpError: If Google Sheets API request fails
            ValueError: If required config values are missing or invalid, or sheet format is incorrect
        """
        try:
            # Read config sheet (read enough columns to get all config fields)
            range_name = "config!A:D"  # Read first 4 columns (should be enough for all config fields)
            values = self.read_range(self.reservations_sheet_id, range_name)
            
            # Validate that sheet has at least 2 rows (header + data)
            if not values:
                raise ValueError("Config sheet is empty")
            
            if len(values) < 2:
                raise ValueError(
                    f"Config sheet must have at least 2 rows (header + data). "
                    f"Found {len(values)} row(s)"
                )
            
            # Extract headers from first row (row index 0)
            headers = values[0]
            
            # Extract data from second row (row index 1)
            data = values[1]
            
            logger.debug(f"Config headers: {headers}")
            logger.debug(f"Config data: {data}")
            
            # Build config dictionary by pairing headers[i] with data[i]
            config = {}
            
            for i, header in enumerate(headers):
                # Skip empty headers
                if not header or not str(header).strip():
                    continue
                
                header_name = str(header).strip().upper()
                
                # Get corresponding value from data row
                if i < len(data):
                    value = str(data[i]).strip() if data[i] is not None else ""
                else:
                    value = ""
                
                # Check if column is empty
                if not value:
                    logger.warning(f"Config field '{header_name}' is empty in config sheet")
                    continue
                
                # Convert values based on field type
                if header_name == "VAT_RATE":
                    try:
                        config["VAT_RATE"] = float(value)
                    except ValueError as e:
                        raise ValueError(f"Invalid VAT_RATE value '{value}': cannot convert to float. {str(e)}")
                
                elif header_name == "AIRBNB_FEE_PERCENT":
                    try:
                        config["AIRBNB_FEE_PERCENT"] = float(value)
                    except ValueError as e:
                        raise ValueError(f"Invalid AIRBNB_FEE_PERCENT value '{value}': cannot convert to float. {str(e)}")
                
                elif header_name == "LODGIFY_FEE_PERCENT":
                    try:
                        config["LODGIFY_FEE_PERCENT"] = float(value)
                    except ValueError as e:
                        raise ValueError(f"Invalid LODGIFY_FEE_PERCENT value '{value}': cannot convert to float. {str(e)}")
                
                elif header_name == "STRIPE_FEE_TABLE":
                    try:
                        config["STRIPE_FEE_TABLE"] = json.loads(value)
                        # Validate that it's a dictionary
                        if not isinstance(config["STRIPE_FEE_TABLE"], dict):
                            raise ValueError(f"STRIPE_FEE_TABLE must be a JSON object (dict), got {type(config['STRIPE_FEE_TABLE'])}")
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON in STRIPE_FEE_TABLE: {str(e)}. Value: {value[:100]}{'...' if len(value) > 100 else ''}"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
            
            # Validate all required fields are present
            required_fields = ["VAT_RATE", "AIRBNB_FEE_PERCENT", "LODGIFY_FEE_PERCENT", "STRIPE_FEE_TABLE"]
            missing_fields = [field for field in required_fields if field not in config]
            
            if missing_fields:
                raise ValueError(
                    f"Missing required config fields: {', '.join(missing_fields)}. "
                    f"Available headers found: {headers}"
                )
            
            logger.info("Configuration loaded successfully from config sheet")
            return config
            
        except HttpError as e:
            logger.error(f"Error loading config from sheet: {str(e)}")
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Error parsing config: {str(e)}")
            raise
    
    def _empty_if_zero(self, value: Any) -> Any:
        """
        Helper method to replace zero values with empty string.
        
        Used to hide zeros in fee columns in Google Sheets.
        
        Args:
            value: Value to check (can be int, float, string, etc.)
            
        Returns:
            Empty string if value is zero (or effectively zero), otherwise original value
        """
        try:
            v = float(str(value).replace(",", "."))
            return "" if abs(v) < 0.0001 else v
        except (ValueError, TypeError):
            return value
    
    def upsert_reservation(self, reservation_dict: Dict[str, Any], headers: Optional[List[str]] = None, notes: Optional[Dict[str, str]] = None) -> None:
        """
        Upsert (insert or update) a reservation in the reservations sheet.
        
        This method:
        - Reads the header row and preserves the exact column order (or uses provided headers)
        - Looks for existing row where "reservation_id" matches
        - If found: merges with existing data (preserves financial fields)
        - If not found: appends new row
        - Fills missing keys with empty strings ""
        - Is fully idempotent and safe to run many times
        - Never overwrites financial fields (preserved from existing row)
        
        Args:
            reservation_dict: Dictionary containing reservation data with keys matching
                            the expected headers (reservation_id, origin, lodgify_id, etc.)
            headers: Optional list of headers. If not provided, reads from sheet.
                    Pass headers to avoid repeated API calls (prevents rate limit 429)
        
        Raises:
            HttpError: If Google Sheets API request fails
            ValueError: If reservation_id is missing
        """
        sheet_name = "reservations"
        
        if "reservation_id" not in reservation_dict or not reservation_dict["reservation_id"]:
            raise ValueError("reservation_dict must contain a non-empty 'reservation_id' field")
        
        reservation_id = str(reservation_dict["reservation_id"])
        
        # Financial fields that should be preserved from existing row
        financial_fields = {
            "airbnb_fee", "lodgify_fee", "stripe_fee", "vat_amount",
            "net_revenue", "price_per_night", "price_per_guest_per_night"
        }
        
        try:
            # Use provided headers or read from sheet
            if headers is None:
                headers = self.get_headers(sheet_name)
            
            # Read all existing data to find matching reservation_id
            data_range = f"{sheet_name}!A:Z"
            all_data = self.read_range(self.reservations_sheet_id, data_range)
            
            existing_row_data = None
            row_index = None
            
            if all_data:
                # Find column index for reservation_id
                try:
                    reservation_id_col_index = headers.index("reservation_id")
                except ValueError:
                    raise ValueError("Header 'reservation_id' not found in sheet")
                
                # Search for matching reservation_id and load existing row data
                for i, row in enumerate(all_data[1:], start=2):  # Start at row 2 (1-indexed)
                    if reservation_id_col_index < len(row) and str(row[reservation_id_col_index]).strip() == reservation_id:
                        row_index = i
                        # Build existing row as dictionary
                        existing_row_data = {}
                        for j, header in enumerate(headers):
                            value = row[j] if j < len(row) else ""
                            existing_row_data[header] = value
                        break
            
            # If row exists, merge with existing data (preserve financial fields)
            if existing_row_data is not None:
                # Import merger for merging
                from services.reservation_merger import ReservationMerger
                merger = ReservationMerger()
                
                # Merge new data into existing (preserves financial fields)
                merged_dict = merger.merge(existing_row_data, reservation_dict)
                
                # Build row values from merged dict
                row_values = []
                # Fields that should hide zeros (fee columns)
                fee_fields = {"airbnb_fee", "lodgify_fee", "stripe_fee", "dynamic_fee"}
                
                # Apply zero hiding to fee fields before processing
                for field in fee_fields:
                    if field in merged_dict:
                        merged_dict[field] = self._empty_if_zero(merged_dict[field])
                
                # Numeric fields - must be sent as real numbers (floats), not strings
                numeric_fields = {
                    "total_price", "vat_amount", "airbnb_fee", "lodgify_fee",
                    "stripe_fee", "dynamic_fee", "total_fees",
                    "net_revenue", "payout_expected", "price_per_night",
                    "price_per_guest_per_night", "anticipation_days"
                }
                for header in headers:
                    value = merged_dict.get(header, "")
                    
                    # Handle numeric fields - send as floats, not strings
                    # Exception: fee fields that are empty string (hidden zeros) stay as empty string
                    if header in numeric_fields:
                        # If it's a fee field that was set to "" (hidden zero), keep it as ""
                        if header in fee_fields and value == "":
                            pass  # Keep as empty string
                        elif value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                            # Use existing value if available, otherwise 0.0
                            existing_value = existing_row_data.get(header, "")
                            if existing_value and str(existing_value).strip():
                                try:
                                    value = float(existing_value)
                                except (ValueError, TypeError):
                                    value = 0.0
                            else:
                                value = 0.0
                        else:
                            # Convert to float (do not stringify)
                            try:
                                value = float(value)
                            except (ValueError, TypeError):
                                # If can't convert, use existing value or 0.0
                                existing_value = existing_row_data.get(header, "")
                                if existing_value and str(existing_value).strip():
                                    try:
                                        value = float(existing_value)
                                    except (ValueError, TypeError):
                                        value = 0.0
                                else:
                                    value = 0.0
                    
                    # Handle phone numbers - must have leading quote to force text
                    elif header == "guest_phone":
                        if value is None or value == "":
                            value = ""
                        else:
                            value = str(value)
                            # Ensure it has leading quote if not empty
                            if value and not value.startswith("'"):
                                value = "'" + value
                    
                    # All other fields as strings
                    else:
                        if value is None:
                            value = ""
                        else:
                            value = str(value)
                    
                    row_values.append(value)
                
                # Update existing row
                range_name = f"{sheet_name}!{row_index}:{row_index}"
                self.write_range(self.reservations_sheet_id, range_name, [row_values])
                logger.info(f"Updated reservation {reservation_id} at row {row_index}")
                
                # Apply financial field notes if provided (using batch update)
                if notes:
                    financial_fields = {"airbnb_fee", "lodgify_fee", "stripe_fee", "dynamic_fee",
                                       "total_fees", "vat_amount", "net_revenue", "payout_expected",
                                       "price_per_night", "price_per_guest_per_night"}
                    
                    # Filter notes to only include financial fields that exist in headers
                    filtered_notes = {
                        field: note_text
                        for field, note_text in notes.items()
                        if field in financial_fields and field in headers
                    }
                    
                    if filtered_notes:
                        # row_index is 1-indexed from sheet, convert to 0-based for API
                        self.set_cell_notes_batch(
                            self.reservations_sheet_id,
                            row_index - 1,  # Convert to 0-based (header row is 0)
                            filtered_notes,
                            headers,
                            sheet_name
                        )
            else:
                # Build row values for new row
                row_values = []
                # Fields that should hide zeros (fee columns)
                fee_fields = {"airbnb_fee", "lodgify_fee", "stripe_fee", "dynamic_fee"}
                
                # Apply zero hiding to fee fields before processing
                for field in fee_fields:
                    if field in reservation_dict:
                        reservation_dict[field] = self._empty_if_zero(reservation_dict[field])
                
                # Numeric fields - must be sent as real numbers (floats), not strings
                numeric_fields = {
                    "total_price", "vat_amount", "airbnb_fee", "lodgify_fee",
                    "stripe_fee", "dynamic_fee", "total_fees",
                    "net_revenue", "payout_expected", "price_per_night",
                    "price_per_guest_per_night", "anticipation_days"
                }
                for header in headers:
                    value = reservation_dict.get(header, "")
                    
                    # Handle numeric fields - send as floats, not strings
                    # Exception: fee fields that are empty string (hidden zeros) stay as empty string
                    if header in numeric_fields:
                        # If it's a fee field that was set to "" (hidden zero), keep it as ""
                        if header in fee_fields and value == "":
                            pass  # Keep as empty string
                        elif value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                            value = 0.0
                        else:
                            try:
                                value = float(value)
                            except (ValueError, TypeError):
                                value = 0.0
                    
                    # Handle phone numbers - must have leading quote to force text
                    elif header == "guest_phone":
                        if value is None or value == "":
                            value = ""
                        else:
                            value = str(value)
                            # Ensure it has leading quote if not empty
                            if value and not value.startswith("'"):
                                value = "'" + value
                    
                    # All other fields as strings
                    else:
                        if value is None:
                            value = ""
                        else:
                            value = str(value)
                    
                    row_values.append(value)
                
                # Append new row
                self.append_row(self.reservations_sheet_id, sheet_name, row_values)
                logger.info(f"Inserted new reservation {reservation_id}")
                
                # Get the row index of the newly appended row
                # Read back to find where it was inserted
                data_range = f"{sheet_name}!A:Z"
                all_data = self.read_range(self.reservations_sheet_id, data_range)
                
                appended_row_index = None
                if all_data:
                    # Find the row with matching reservation_id (should be the last row)
                    try:
                        reservation_id_col_index = headers.index("reservation_id")
                        for i, row in enumerate(all_data[1:], start=2):  # Start at row 2 (1-indexed)
                            if reservation_id_col_index < len(row) and str(row[reservation_id_col_index]).strip() == reservation_id:
                                appended_row_index = i
                                break
                    except ValueError:
                        logger.warning("Could not find reservation_id column for note placement")
                
                # Apply financial field notes if provided (using batch update)
                if notes and appended_row_index:
                    financial_fields = {"airbnb_fee", "lodgify_fee", "stripe_fee", "dynamic_fee",
                                       "total_fees", "vat_amount", "net_revenue", "payout_expected",
                                       "price_per_night", "price_per_guest_per_night"}
                    
                    # Filter notes to only include financial fields that exist in headers
                    filtered_notes = {
                        field: note_text
                        for field, note_text in notes.items()
                        if field in financial_fields and field in headers
                    }
                    
                    if filtered_notes:
                        # appended_row_index is 1-indexed, convert to 0-based for API
                        self.set_cell_notes_batch(
                            self.reservations_sheet_id,
                            appended_row_index - 1,  # Convert to 0-based (header row is 0)
                            filtered_notes,
                            headers,
                            sheet_name
                        )
            
            # Small delay to avoid rate limit 429
            import time
            time.sleep(0.15)
            
        except HttpError as e:
            logger.error(f"Error upserting reservation {reservation_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error upserting reservation {reservation_id}: {str(e)}", exc_info=True)
            raise


# Alias for backward compatibility
GoogleSheets = GoogleSheetsService
