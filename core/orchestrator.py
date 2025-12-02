"""
Core Orchestrator Module.
Central controller for coordinating all automation subsystems.
"""

import logging
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Central orchestrator for coordinating all automation subsystems.
    
    This class acts as the main controller for:
    - Lodgify synchronization
    - SEF form processing and PDF generation
    - WhatsApp message sending
    - Financial data updates
    - Fatura export
    - Notion task creation
    - KPI generation
    
    Usage Example:
        >>> orchestrator = Orchestrator()
        >>> orchestrator.full_cycle()  # Run all subsystems
        >>> orchestrator.process_sef()  # Run only SEF processing
    """
    
    def __init__(self):
        """
        Initialize the orchestrator.
        
        Sets up dependencies and prepares for subsystem coordination.
        """
        logger.info("Initializing Orchestrator...")
        self._initialized = True
        logger.info("Orchestrator initialized successfully")
    
    def sync_lodgify(self) -> bool:
        """
        Synchronize data with Lodgify platform.
        
        This method:
        - Fetches ALL reservations from Lodgify API (since beginning of time)
        - Normalizes reservation data
        - Calculates financial fields
        - Updates Google Sheets reservations sheet (upsert by reservation_id)
        - Handles conflicts and duplicates (idempotent)
        
        Flow:
        Lodgify → Normalize → Financials → Google Sheets (reservations sheet)
        
        Returns:
            True if synchronization succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during synchronization
        """
        logger.info("Starting Lodgify synchronization...")
        
        try:
            # Import services
            from services.lodgify_service import LodgifyService
            from services.google_sheets import GoogleSheetsService
            from services.finance import calculate_financials
            
            # Initialize services
            lodgify = LodgifyService()
            sheets = GoogleSheetsService()
            
            logger.info("Services initialized successfully")
            
            # Load configuration from Google Sheets
            logger.info("Loading configuration from config sheet...")
            try:
                config = sheets.load_config()
                logger.info("Configuration loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load config from sheet: {str(e)}", exc_info=True)
                logger.warning("Falling back to environment variables for config...")
                # Fallback to environment variables
                from config.settings import (
                    get_vat_rate,
                    get_airbnb_fee_percent,
                    get_lodgify_fee_percent,
                    get_stripe_fee_table
                )
                config = {
                    "VAT_RATE": get_vat_rate(),
                    "AIRBNB_FEE_PERCENT": get_airbnb_fee_percent(),
                    "LODGIFY_FEE_PERCENT": get_lodgify_fee_percent(),
                    "STRIPE_FEE_TABLE": get_stripe_fee_table()
                }
                logger.info("Using environment variable configuration")
            
            # Fetch ALL reservations from Lodgify (no date filter = all reservations)
            logger.info("Fetching all reservations from Lodgify...")
            try:
                reservations = lodgify.get_reservations()
                logger.info(f"Retrieved {len(reservations)} reservations from Lodgify")
            except Exception as e:
                logger.error(f"Failed to fetch reservations from Lodgify: {str(e)}", exc_info=True)
                return False
            
            if not reservations:
                logger.info("No reservations found in Lodgify")
                return True
            
            # Process each reservation
            processed_count = 0
            updated_count = 0
            inserted_count = 0
            error_count = 0
            
            logger.info(f"Processing {len(reservations)} reservations...")
            
            for i, raw_reservation in enumerate(reservations, 1):
                try:
                    reservation_id = raw_reservation.get("id", f"unknown_{i}")
                    logger.debug(f"Processing reservation {i}/{len(reservations)}: {reservation_id}")
                    
                    # Normalize reservation data
                    normalized = lodgify.normalize_reservation(raw_reservation)
                    
                    # Calculate financials
                    financials = calculate_financials(normalized, config)
                    
                    # Merge normalized reservation with financials
                    final_dict = {**normalized, **financials}
                    
                    # Map fields to expected sheet column names
                    # Ensure reservation_id is present (use Lodgify ID as fallback)
                    if "reservation_id" not in final_dict or not final_dict["reservation_id"]:
                        lodgify_id = normalized.get("id", normalized.get("lodgify_id", reservation_id))
                        final_dict["reservation_id"] = str(lodgify_id)
                    
                    # Map guest info fields
                    if "guest_name" not in final_dict:
                        final_dict["guest_name"] = normalized.get("guest_name", "")
                    if "guest_email" not in final_dict:
                        final_dict["guest_email"] = normalized.get("guest_email", "")
                    if "guest_phone" not in final_dict:
                        final_dict["guest_phone"] = normalized.get("guest_phone", "")
                    if "guests_count" not in final_dict:
                        final_dict["guests_count"] = normalized.get("guest_count", normalized.get("guests_count", ""))
                    
                    # Ensure lodgify_id is set
                    if "lodgify_id" not in final_dict:
                        final_dict["lodgify_id"] = str(normalized.get("id", reservation_id))
                    
                    # Map channel to origin
                    channel = normalized.get("channel", "").lower().strip()
                    if channel:
                        final_dict["origin"] = channel
                    elif "origin" not in final_dict or not final_dict["origin"]:
                        final_dict["origin"] = "lodgify"
                    
                    # Ensure nights is calculated if missing
                    if "nights" not in final_dict or not final_dict["nights"]:
                        check_in = final_dict.get("check_in", "")
                        check_out = final_dict.get("check_out", "")
                        if check_in and check_out:
                            try:
                                from datetime import datetime
                                check_in_date = datetime.strptime(check_in.split()[0], "%Y-%m-%d").date()
                                check_out_date = datetime.strptime(check_out.split()[0], "%Y-%m-%d").date()
                                nights = (check_out_date - check_in_date).days
                                final_dict["nights"] = nights if nights > 0 else ""
                            except (ValueError, AttributeError):
                                final_dict["nights"] = ""
                        else:
                            final_dict["nights"] = ""
                    
                    # Ensure reservation_date is set if missing (use created_at)
                    if "reservation_date" not in final_dict or not final_dict["reservation_date"]:
                        final_dict["reservation_date"] = normalized.get("created_at", "")
                    
                    # Ensure status is set
                    if "status" not in final_dict:
                        final_dict["status"] = normalized.get("status", "")
                    
                    # Ensure welcome_sent is set (default to empty)
                    if "welcome_sent" not in final_dict:
                        final_dict["welcome_sent"] = ""
                    
                    # Ensure airbnb_id is set (empty if not Airbnb)
                    if "airbnb_id" not in final_dict:
                        final_dict["airbnb_id"] = ""
                    
                    # Upsert to Google Sheets
                    # Note: upsert_reservation() will determine if it's an insert or update
                    sheets.upsert_reservation(final_dict)
                    
                    processed_count += 1
                    
                    # Log progress every 10 reservations
                    if processed_count % 10 == 0:
                        logger.info(f"Processed {processed_count}/{len(reservations)} reservations...")
                
                except Exception as e:
                    error_count += 1
                    reservation_id = raw_reservation.get("id", f"unknown_{i}")
                    logger.error(
                        f"Error processing reservation {reservation_id}: {str(e)}",
                        exc_info=True
                    )
                    # Continue processing other reservations even if one fails
                    continue
            
            # Log summary
            logger.info("=" * 70)
            logger.info("Lodgify synchronization completed")
            logger.info("=" * 70)
            logger.info(f"Total reservations processed: {processed_count}")
            logger.info(f"Errors encountered: {error_count}")
            logger.info(f"Successfully synced: {processed_count - error_count}")
            logger.info("=" * 70)
            
            # Return success if at least some reservations were processed
            return processed_count > 0 or len(reservations) == 0
            
        except Exception as e:
            logger.error(f"Failed to sync Lodgify: {str(e)}", exc_info=True)
            return False
    
    def process_sef(self) -> bool:
        """
        Process SEF form responses and generate PDFs.
        
        This method calls the existing SEF Form Watcher service to:
        - Read new form responses from Google Sheets
        - Extract guest information
        - Calculate tourist tax
        - Generate SEF PDF documents
        - Upload PDFs to Dropbox in date-organized folders
        
        Returns:
            True if processing succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during SEF processing
        """
        logger.info("Starting SEF form processing...")
        try:
            # Import SEF watcher service
            from services.sef_form_watcher import SEFFormWatcher
            
            # Initialize and run SEF watcher
            watcher = SEFFormWatcher()
            watcher.check_for_new_entries()
            
            logger.info("SEF form processing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process SEF forms: {str(e)}", exc_info=True)
            return False
    
    def sync_reservations_csv(self) -> bool:
        """
        Synchronize reservations from CSV files.
        
        This method:
        - Finds the latest CSV file in the Lodgify_CSV directory
        - Loads and validates CSV data
        - Maps CSV rows to internal reservation schema
        - Merges with existing reservations in Google Sheets
        - Calculates financial fields
        - Upserts to Google Sheets reservations sheet
        - Uses state file to skip already imported reservations (idempotent)
        
        Flow:
        CSV File → Import → Map → Merge → Financials → Google Sheets
        
        Returns:
            True if synchronization succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during synchronization
        """
        logger.info("Starting CSV reservations synchronization...")
        
        try:
            # Import services
            from services.csv_importer import CSVImporter
            from services.reservation_mapper import ReservationMapper
            from services.reservation_merger import ReservationMerger
            from services.google_sheets import GoogleSheetsService
            from services.finance import calculate_financials
            from services.state_manager import StateManager
            
            # Initialize state manager
            state = StateManager()
            
            # Initialize services
            csv_directory = "/Users/goncalotelesdeabreu/Developer/Lettia_Automation/Lodgify_CSV"
            importer = CSVImporter(csv_directory)
            mapper = ReservationMapper()
            merger = ReservationMerger()
            sheets = GoogleSheetsService()
            
            logger.info("Services initialized successfully")
            
            # Find latest CSV file
            latest_csv = importer.get_latest_csv()
            if not latest_csv:
                logger.warning("No CSV file found in directory. Skipping CSV sync.")
                return False
            
            logger.info(f"Processing CSV file: {latest_csv.name}")
            
            # Load CSV data
            try:
                rows = importer.load_csv(latest_csv)
            except Exception as e:
                logger.error(f"Failed to load CSV file: {str(e)}", exc_info=True)
                return False
            
            if not rows:
                logger.warning("CSV file is empty. No reservations to process.")
                return True
            
            # Validate required columns (exact case-sensitive match)
            required_columns = [
                "Id", "Source", "SourceText", "Name", "DateArrival", "DateDeparture",
                "Nights", "People", "DateCreated", "TotalAmount", "Currency",
                "Status", "Email", "Phone", "CountryName", "IncludedVatTotal"
            ]
            
            try:
                importer.validate_columns(rows, required_columns)
            except ValueError as e:
                logger.error(f"CSV validation failed: {str(e)}")
                return False
            
            # Load configuration from Google Sheets
            logger.info("Loading configuration from config sheet...")
            try:
                config = sheets.load_config()
                logger.info("Configuration loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load config from sheet: {str(e)}", exc_info=True)
                logger.warning("Falling back to environment variables for config...")
                # Fallback to environment variables
                from config.settings import (
                    get_vat_rate,
                    get_airbnb_fee_percent,
                    get_lodgify_fee_percent,
                    get_stripe_fee_table
                )
                config = {
                    "VAT_RATE": get_vat_rate(),
                    "AIRBNB_FEE_PERCENT": get_airbnb_fee_percent(),
                    "LODGIFY_FEE_PERCENT": get_lodgify_fee_percent(),
                    "STRIPE_FEE_TABLE": get_stripe_fee_table()
                }
                logger.info("Using environment variable configuration")
            
            # Get headers once to avoid rate limit 429
            logger.info("Loading sheet headers...")
            try:
                sheet_headers = sheets.get_headers("reservations")
                logger.info(f"Loaded {len(sheet_headers)} headers from reservations sheet")
            except Exception as e:
                logger.error(f"Failed to load headers: {str(e)}", exc_info=True)
                return False
            
            # Get existing reservations from Google Sheets for merging
            logger.info("Loading existing reservations from Google Sheets...")
            try:
                existing_reservations = sheets.get_reservations_data()
                # Create a lookup dictionary by reservation_id
                existing_lookup = {}
                for res in existing_reservations:
                    res_id = str(res.get("reservation_id", "")).strip()
                    if res_id:
                        existing_lookup[res_id] = res
                logger.info(f"Loaded {len(existing_lookup)} existing reservations for merging")
            except Exception as e:
                logger.warning(f"Could not load existing reservations: {str(e)}. Proceeding without merge.")
                existing_lookup = {}
            
            # Process each CSV row
            processed_count = 0
            updated_count = 0
            inserted_count = 0
            error_count = 0
            ignored_count = 0
            
            logger.info(f"Processing {len(rows)} reservations from CSV...")
            
            for i, csv_row in enumerate(rows, 1):
                try:
                    # Map CSV row to internal schema (returns None if Status != "Booked")
                    mapped = mapper.map_csv_row(csv_row)
                    
                    # Skip if mapping returned None (status not "Booked")
                    if mapped is None:
                        ignored_count += 1
                        continue
                    
                    reservation_id = mapped.get("reservation_id", f"unknown_{i}")
                    
                    # Skip if already imported (idempotency)
                    if state.already_imported(reservation_id):
                        logger.debug(f"Skipping reservation {reservation_id} - already imported")
                        continue
                    
                    # Get existing reservation if it exists
                    existing = existing_lookup.get(reservation_id, {})
                    
                    # Merge with existing data
                    if existing:
                        merged = merger.merge(existing, mapped)
                        updated_count += 1
                    else:
                        merged = mapped
                        inserted_count += 1
                    
                    # Calculate financials
                    financials = calculate_financials(merged, config)
                    
                    # Merge financials into final dict
                    final_dict = {**merged, **financials}
                    
                    # Ensure all required fields are present
                    if "reservation_id" not in final_dict or not final_dict["reservation_id"]:
                        final_dict["reservation_id"] = reservation_id
                    
                    # Generate financial field notes (only if notes not already filled)
                    notes = None
                    if not state.notes_already_filled(reservation_id):
                        from services.finance import generate_financial_notes
                        notes = generate_financial_notes(final_dict, financials, config)
                    
                    # Upsert to Google Sheets (pass headers and notes)
                    sheets.upsert_reservation(final_dict, headers=sheet_headers, notes=notes)
                    
                    # Mark as imported and notes filled
                    state.mark_imported(reservation_id)
                    if notes:
                        state.mark_notes_filled(reservation_id)
                    
                    processed_count += 1
                    
                    # Log progress every 10 reservations
                    if processed_count % 10 == 0:
                        logger.info(f"Processed {processed_count} new reservations...")
                
                except Exception as e:
                    error_count += 1
                    reservation_id = csv_row.get("Id", f"unknown_{i}")
                    logger.error(
                        f"Error processing reservation {reservation_id}: {str(e)}",
                        exc_info=True
                    )
                    # Continue processing other reservations even if one fails
                    continue
            
            # Log summary
            logger.info("=" * 70)
            logger.info("CSV reservations synchronization completed")
            logger.info("=" * 70)
            logger.info(f"Total reservations in CSV: {len(rows)}")
            logger.info(f"  - Imported bookings: {processed_count}")
            logger.info(f"  - Ignored non-booked bookings: {ignored_count}")
            logger.info(f"  - New reservations: {inserted_count}")
            logger.info(f"  - Updated reservations: {updated_count}")
            logger.info(f"  - Errors encountered: {error_count}")
            logger.info("=" * 70)
            
            # Remove rows with status != "booked"
            logger.info("Removing reservations with status != 'booked'...")
            try:
                headers = sheets.get_headers("reservations")
                status_col = headers.index("status")
                
                all_rows = sheets.read_range(sheets.reservations_sheet_id, "reservations!A:Z")
                
                rows_to_delete = []
                for idx, row in enumerate(all_rows[1:], start=2):  # start=2 → skip header
                    status = row[status_col] if status_col < len(row) else ""
                    if status.lower() != "booked":
                        rows_to_delete.append(idx)
                
                if rows_to_delete:
                    logger.info(f"Found {len(rows_to_delete)} rows to delete (status != 'booked')")
                    rows_to_delete.sort(reverse=True)
                    sheets.delete_rows_batch(
                        sheets.reservations_sheet_id,
                        "reservations",
                        rows_to_delete
                    )
                    logger.info(f"Deleted {len(rows_to_delete)} rows with status != 'booked'")
                else:
                    logger.info("No rows to delete (all reservations have status 'booked')")
                    
            except Exception as e:
                logger.warning(f"Could not remove non-booked reservations: {str(e)}. Continuing...")
            
            # Fill notes for existing rows that don't have them yet
            logger.info("Checking for existing reservations missing financial notes...")
            notes_added_count = 0
            try:
                # Use already loaded existing_reservations to avoid duplicate API calls
                for res in existing_reservations:
                    reservation_id = str(res.get("reservation_id", "")).strip()
                    if not reservation_id:
                        continue
                    
                    # Skip if notes already filled
                    if state.notes_already_filled(reservation_id):
                        continue
                    
                    # Skip if not imported (shouldn't happen, but safety check)
                    if not state.already_imported(reservation_id):
                        continue
                    
                    # Generate and add notes for this existing row
                    try:
                        # Recalculate financials for notes generation
                        merged = res  # Use existing row data
                        financials = calculate_financials(merged, config)
                        final_dict = {**merged, **financials}
                        
                        from services.finance import generate_financial_notes
                        notes = generate_financial_notes(final_dict, financials, config)
                        
                        if notes:
                            # Get row index for this reservation
                            # We'll need to find the row and update notes only
                            # For now, we'll use upsert which will update notes
                            sheets.upsert_reservation(final_dict, headers=sheet_headers, notes=notes)
                            state.mark_notes_filled(reservation_id)
                            notes_added_count += 1
                            logger.debug(f"Added notes to existing reservation {reservation_id}")
                    except Exception as e:
                        logger.warning(f"Could not add notes to reservation {reservation_id}: {str(e)}")
                        continue
                
                if notes_added_count > 0:
                    logger.info(f"Added notes to {notes_added_count} existing reservations")
            
            except Exception as e:
                logger.warning(f"Could not fill notes for existing reservations: {str(e)}. Continuing...")
            
            # Return success if at least some reservations were processed
            return processed_count > 0 or len(rows) == 0
            
        except Exception as e:
            logger.error(f"Failed to sync CSV reservations: {str(e)}", exc_info=True)
            return False
    
    def generate_invoices(self, debug: bool = False) -> bool:
        """
        Generate invoice-ready data from reservations and SEF forms.
        
        This method:
        - Loads reservations from Google Sheets
        - Loads SEF forms from Google Sheets
        - Matches reservations with SEF forms using fuzzy matching
        - Generates invoice lines (1 for Airbnb, 3 for Website bookings)
        - Writes to Invoices_Lettia sheet (only new reservations)
        - Uses state file for idempotency
        
        Args:
            debug: If True, run in dry-run mode (no writes, preview only)
        
        Returns:
            True if generation succeeded, False otherwise
        """
        if debug:
            logger.info("Starting invoice generation in DEBUG/DRY-RUN mode (no writes will be performed)...")
        else:
            logger.info("Starting invoice generation...")
        
        try:
            from services.invoice_service import InvoiceService
            from services.google_sheets import GoogleSheetsService
            
            # Initialize services
            sheets = GoogleSheetsService()
            invoice_service = InvoiceService(sheets)
            
            # Generate all invoices
            return invoice_service.generate_all_invoices(debug=debug)
            
        except Exception as e:
            logger.error(f"Failed to generate invoices: {str(e)}", exc_info=True)
            return False
    
    def send_whatsapp_messages(self) -> bool:
        """
        Send WhatsApp notifications and messages.
        
        This method:
        - Loads the latest CSV file
        - Loads all existing reservations from Google Sheets
        - Identifies incomplete rows (missing any financial field)
        - For each incomplete row, re-processes from CSV and updates only that row
        
        Returns:
            True if retry completed, False otherwise
        """
        logger.info("Starting CSV reservations retry (missing financial fields only)...")
        
        try:
            import time
            from services.csv_importer import CSVImporter
            from services.reservation_mapper import ReservationMapper
            from services.reservation_merger import ReservationMerger
            from services.google_sheets import GoogleSheetsService
            from services.finance import calculate_financials, generate_financial_notes
            
            # Initialize services
            csv_directory = "/Users/goncalotelesdeabreu/Developer/Lettia_Automation/Lodgify_CSV"
            importer = CSVImporter(csv_directory)
            mapper = ReservationMapper()
            merger = ReservationMerger()
            sheets = GoogleSheetsService()
            
            logger.info("Services initialized successfully")
            
            # Find latest CSV file
            latest_csv = importer.get_latest_csv()
            if not latest_csv:
                logger.warning("No CSV file found in directory. Skipping retry.")
                return False
            
            logger.info(f"Processing CSV file: {latest_csv.name}")
            
            # Load CSV data and create lookup by reservation_id
            try:
                csv_rows = importer.load_csv(latest_csv)
            except Exception as e:
                logger.error(f"Failed to load CSV file: {str(e)}", exc_info=True)
                return False
            
            if not csv_rows:
                logger.warning("CSV file is empty. Nothing to retry.")
                return True
            
            # Build CSV lookup by Id (reservation_id source)
            csv_lookup = {}
            for csv_row in csv_rows:
                csv_id = str(csv_row.get("Id", "")).strip()
                if csv_id:
                    csv_lookup[csv_id] = csv_row
            
            logger.info(f"Loaded {len(csv_lookup)} reservations from CSV")
            
            # Load configuration
            logger.info("Loading configuration from config sheet...")
            try:
                config = sheets.load_config()
                logger.info("Configuration loaded successfully")
            except Exception as e:
                logger.error(f"Failed to load config from sheet: {str(e)}", exc_info=True)
                logger.warning("Falling back to environment variables for config...")
                from config.settings import (
                    get_vat_rate,
                    get_airbnb_fee_percent,
                    get_lodgify_fee_percent,
                    get_stripe_fee_table
                )
                config = {
                    "VAT_RATE": get_vat_rate(),
                    "AIRBNB_FEE_PERCENT": get_airbnb_fee_percent(),
                    "LODGIFY_FEE_PERCENT": get_lodgify_fee_percent(),
                    "STRIPE_FEE_TABLE": get_stripe_fee_table()
                }
            
            # Load all existing reservations from Google Sheets
            logger.info("Loading existing reservations from Google Sheets...")
            try:
                existing_reservations = sheets.get_reservations_data()
                logger.info(f"Loaded {len(existing_reservations)} existing reservations")
            except Exception as e:
                logger.error(f"Failed to load existing reservations: {str(e)}", exc_info=True)
                return False
            
            # Get headers
            try:
                sheet_headers = sheets.get_headers("reservations")
            except Exception as e:
                logger.error(f"Failed to load headers: {str(e)}", exc_info=True)
                return False
            
            # Fields that must be checked for completeness
            required_financial_fields = {
                "country", "airbnb_fee", "lodgify_fee", "stripe_fee",
                "dynamic_fee", "total_fees", "vat_amount", "net_revenue",
                "price_per_night", "price_per_guest_per_night"
            }
            
            # Helper function to check if a field is truly empty
            def _is_field_empty(value):
                """
                Check if a field is empty (incomplete).
                Only None or empty string "" are considered incomplete.
                All other values (including "0", "0.00", numbers, etc.) are considered complete.
                """
                if value is None:
                    return True
                if isinstance(value, str) and value.strip() == "":
                    return True
                return False
            
            # Identify incomplete reservations
            incomplete_reservations = []
            incomplete_row_indexes = []
            incomplete_reservation_ids = []
            
            # We need to track row index for logging (1-indexed, row 1 is header)
            for row_index, res in enumerate(existing_reservations, start=2):  # start=2 because row 1 is header
                reservation_id = str(res.get("reservation_id", "")).strip()
                if not reservation_id:
                    continue
                
                # Check if any required financial field is empty (only None or "")
                is_incomplete = False
                for field in required_financial_fields:
                    value = res.get(field)
                    if _is_field_empty(value):
                        is_incomplete = True
                        break
                
                if is_incomplete:
                    incomplete_reservations.append(res)
                    incomplete_row_indexes.append(row_index)
                    incomplete_reservation_ids.append(reservation_id)
            
            if not incomplete_reservations:
                logger.info("No incomplete reservations found. All reservations are complete.")
                return True
            
            # Log details before processing
            logger.info(f"Found {len(incomplete_reservations)} incomplete rows")
            logger.info(f"Row indexes: {incomplete_row_indexes}")
            logger.info(f"Reservation IDs: {incomplete_reservation_ids}")
            
            logger.info(f"Processing {len(incomplete_reservations)} incomplete reservations to fix")
            
            # Process each incomplete reservation
            fixed_count = 0
            still_incomplete = []
            
            for incomplete_res in incomplete_reservations:
                reservation_id = str(incomplete_res.get("reservation_id", "")).strip()
                if not reservation_id:
                    continue
                
                try:
                    # Find corresponding CSV row
                    # reservation_id in Sheet = CSV Id, so we can look it up directly
                    csv_row = csv_lookup.get(reservation_id)
                    
                    if not csv_row:
                        logger.warning(f"Could not find CSV data for reservation {reservation_id} (Id={reservation_id}). Skipping.")
                        still_incomplete.append(reservation_id)
                        continue
                    
                    # Map CSV row to internal schema
                    mapped = mapper.map_csv_row(csv_row)
                    if mapped is None:
                        logger.warning(f"Reservation {reservation_id} mapped to None (status not Booked). Skipping.")
                        still_incomplete.append(reservation_id)
                        continue
                    
                    # Merge with existing data (preserve existing values)
                    merged = merger.merge(incomplete_res, mapped)
                    
                    # Calculate financials
                    financials = calculate_financials(merged, config)
                    
                    # Merge financials into final dict
                    final_dict = {**merged, **financials}
                    
                    # Ensure reservation_id is preserved
                    final_dict["reservation_id"] = reservation_id
                    
                    # Generate financial field notes
                    notes = generate_financial_notes(final_dict, financials, config)
                    
                    # Update only this row (it already exists, so upsert will update it)
                    sheets.upsert_reservation(final_dict, headers=sheet_headers, notes=notes)
                    
                    fixed_count += 1
                    logger.debug(f"Fixed reservation {reservation_id}")
                    
                    # Sleep to avoid rate limits
                    time.sleep(0.3)
                    
                    # Log progress every 10 fixes
                    if fixed_count % 10 == 0:
                        logger.info(f"Fixed {fixed_count}/{len(incomplete_reservations)} reservations...")
                
                except Exception as e:
                    logger.error(f"Error fixing reservation {reservation_id}: {str(e)}", exc_info=True)
                    still_incomplete.append(reservation_id)
                    continue
            
            # Log summary
            logger.info("=" * 70)
            logger.info("CSV reservations retry completed")
            logger.info("=" * 70)
            logger.info(f"Rows fixed: {fixed_count}")
            logger.info(f"Still incomplete: {len(still_incomplete)}")
            
            if still_incomplete:
                logger.warning("Still incomplete reservation IDs:")
                for res_id in sorted(still_incomplete):
                    logger.warning(f"  - {res_id}")
            else:
                logger.info("All incomplete reservations were successfully fixed!")
            
            logger.info("=" * 70)
            
            return fixed_count > 0 or len(incomplete_reservations) == 0
            
        except Exception as e:
            logger.error(f"Failed to retry missing reservations: {str(e)}", exc_info=True)
            return False
    
    def send_whatsapp_messages(self) -> bool:
        """
        Send WhatsApp notifications and messages.
        
        This method will:
        - Send guest arrival notifications to owner
        - Send check-in instructions to guests
        - Handle message templates and personalization
        - Track message delivery status
        
        Returns:
            True if messages sent successfully, False otherwise
            
        Raises:
            Exception: If critical errors occur during message sending
        """
        logger.info("Starting WhatsApp message sending...")
        try:
            # TODO: Implement WhatsApp message sending
            # - Connect to WhatsApp Business API
            # - Load message templates
            # - Send notifications to owner
            # - Send instructions to guests
            # - Handle delivery confirmations
            
            logger.info("WhatsApp message sending completed (placeholder)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send WhatsApp messages: {str(e)}", exc_info=True)
            return False
    
    def update_financials(self) -> bool:
        """
        Update financial records and calculations.
        
        This method will:
        - Calculate revenue from reservations
        - Update expense tracking
        - Compute profit margins
        - Generate financial reports
        - Sync with accounting systems
        
        Returns:
            True if financial update succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during financial updates
        """
        logger.info("Starting financial data update...")
        try:
            # TODO: Implement financial updates
            # - Fetch reservation data
            # - Calculate revenue, fees, taxes
            # - Update financial sheets
            # - Generate reports
            
            logger.info("Financial data update completed (placeholder)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update financials: {str(e)}", exc_info=True)
            return False
    
    def export_faturas(self) -> bool:
        """
        Export invoices (faturas) for accounting.
        
        This method will:
        - Generate invoices from reservations
        - Format according to Portuguese tax requirements
        - Export to CSV/PDF formats
        - Upload to accounting system
        - Track invoice numbers and sequences
        
        Returns:
            True if export succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during export
        """
        logger.info("Starting faturas export...")
        try:
            # TODO: Implement faturas export
            # - Generate invoice data
            # - Format according to requirements
            # - Export to CSV/PDF
            # - Upload to accounting system
            
            logger.info("Faturas export completed (placeholder)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export faturas: {str(e)}", exc_info=True)
            return False
    
    def create_notion_tasks(self) -> bool:
        """
        Create and update tasks in Notion workspace.
        
        This method will:
        - Create task entries for new reservations
        - Update existing tasks with guest information
        - Sync check-in/check-out reminders
        - Track task completion status
        - Link to related documents (PDFs, sheets)
        
        Returns:
            True if task creation succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during task creation
        """
        logger.info("Starting Notion task creation...")
        try:
            # TODO: Implement Notion task creation
            # - Connect to Notion API
            # - Create task database entries
            # - Link to reservations and PDFs
            # - Set reminders and due dates
            
            logger.info("Notion task creation completed (placeholder)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Notion tasks: {str(e)}", exc_info=True)
            return False
    
    def generate_kpis(self) -> bool:
        """
        Generate Key Performance Indicators (KPIs) and dashboard metrics.
        
        This method will:
        - Calculate occupancy rates
        - Compute revenue metrics
        - Analyze booking trends
        - Generate visualizations
        - Update dashboard data
        
        Returns:
            True if KPI generation succeeded, False otherwise
            
        Raises:
            Exception: If critical errors occur during KPI generation
        """
        logger.info("Starting KPI generation...")
        try:
            # TODO: Implement KPI generation
            # - Fetch data from all sources
            # - Calculate metrics (occupancy, revenue, etc.)
            # - Generate visualizations
            # - Update dashboard database
            
            logger.info("KPI generation completed (placeholder)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate KPIs: {str(e)}", exc_info=True)
            return False
    
    def full_cycle(self) -> dict:
        """
        Execute a complete automation cycle, running all subsystems.
        
        This method orchestrates the execution of all automation subsystems
        in the correct order, handling errors and aggregating results.
        
        Execution order:
        1. Sync Lodgify data
        2. Process SEF forms
        3. Send WhatsApp messages
        4. Update financial records
        5. Export faturas
        6. Create Notion tasks
        7. Generate KPIs
        
        Returns:
            Dictionary with execution results for each subsystem:
            {
                'sync_lodgify': True/False,
                'process_sef': True/False,
                'send_whatsapp_messages': True/False,
                'update_financials': True/False,
                'export_faturas': True/False,
                'create_notion_tasks': True/False,
                'generate_kpis': True/False,
                'overall_success': True/False
            }
            
        Raises:
            Exception: If critical errors occur that prevent cycle execution
        """
        logger.info("=" * 70)
        logger.info("Starting full automation cycle...")
        logger.info("=" * 70)
        
        results = {}
        
        try:
            # Execute all subsystems in order
            results['sync_lodgify'] = self.sync_lodgify()
            results['process_sef'] = self.process_sef()
            results['send_whatsapp_messages'] = self.send_whatsapp_messages()
            results['update_financials'] = self.update_financials()
            results['export_faturas'] = self.export_faturas()
            results['create_notion_tasks'] = self.create_notion_tasks()
            results['generate_kpis'] = self.generate_kpis()
            
            # Determine overall success
            results['overall_success'] = all([
                results['sync_lodgify'],
                results['process_sef'],
                results['send_whatsapp_messages'],
                results['update_financials'],
                results['export_faturas'],
                results['create_notion_tasks'],
                results['generate_kpis']
            ])
            
            # Log summary
            logger.info("=" * 70)
            logger.info("Automation cycle completed")
            logger.info("=" * 70)
            logger.info("Results summary:")
            for subsystem, success in results.items():
                if subsystem != 'overall_success':
                    status = "✓ SUCCESS" if success else "✗ FAILED"
                    logger.info(f"  {subsystem}: {status}")
            
            overall_status = "✓ SUCCESS" if results['overall_success'] else "✗ PARTIAL FAILURE"
            logger.info(f"  Overall: {overall_status}")
            logger.info("=" * 70)
            
            return results
            
        except Exception as e:
            logger.error(f"Critical error during full cycle execution: {str(e)}", exc_info=True)
            results['overall_success'] = False
            results['error'] = str(e)
            return results


if __name__ == "__main__":
    """
    Debug entry point for manual execution.
    """
    orch = Orchestrator()
    orch.full_cycle()

