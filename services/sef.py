"""
SEF (Serviço de Estrangeiros e Fronteiras) service.
Handles SEF registration and compliance operations.
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from pathlib import Path
from services.dropbox_service import DropboxService
from services.pdf_generator import PDFGenerator
from config.settings import get_dropbox_sef_folder


class SEFService:
    """Service class for SEF registration and compliance operations."""
    
    def __init__(self):
        """Initialize SEF service with Dropbox and PDF generator."""
        self.dropbox_service = DropboxService()
        self.pdf_generator = PDFGenerator()
        self.dropbox_folder = get_dropbox_sef_folder()
    
    def generate_sef_registration(self, guest_data: Dict) -> Dict:
        """
        Generate SEF registration data for a guest.
        
        Args:
            guest_data: Dictionary containing guest information (name, passport, dates, etc.)
            
        Returns:
            Dictionary with SEF registration data
            
        Planned behavior:
            - Format guest data according to SEF requirements
            - Validate required fields
            - Return structured registration data
        """
        # TODO: Implement SEF registration data generation
        pass
    
    def validate_guest_data(self, guest_data: Dict) -> tuple[bool, List[str]]:
        """
        Validate guest data for SEF compliance.
        
        Args:
            guest_data: Dictionary containing guest information
            
        Returns:
            Tuple of (is_valid, list_of_errors)
            - is_valid: True if all validation passes, False otherwise
            - errors: List of error messages (empty list if valid)
        """
        errors: List[str] = []
        required_fields = ['full_name', 'passport', 'country', 'dob', 'check_in', 'check_out']
        
        # Check for missing required fields
        for field in required_fields:
            if field not in guest_data or not guest_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate date format (YYYY-MM-DD) for date fields
        date_fields = ['dob', 'check_in', 'check_out']
        for field in date_fields:
            if field in guest_data and guest_data.get(field):
                date_value = str(guest_data[field]).strip()
                if not self._validate_date_format(date_value):
                    errors.append(f"Invalid date format for {field}: expected YYYY-MM-DD, got '{date_value}'")
        
        # Validate that check_out is after check_in
        if 'check_in' in guest_data and 'check_out' in guest_data:
            check_in = guest_data.get('check_in')
            check_out = guest_data.get('check_out')
            if check_in and check_out and self._validate_date_format(str(check_in)) and self._validate_date_format(str(check_out)):
                try:
                    check_in_date = datetime.strptime(str(check_in), '%Y-%m-%d')
                    check_out_date = datetime.strptime(str(check_out), '%Y-%m-%d')
                    if check_out_date <= check_in_date:
                        errors.append("check_out date must be after check_in date")
                except ValueError:
                    # Date parsing error already caught by format validation
                    pass
        
        # Return validation result
        if errors:
            return (False, errors)
        return (True, [])
    
    def _validate_date_format(self, date_string: str) -> bool:
        """
        Validate that a date string is in YYYY-MM-DD format.
        
        Args:
            date_string: Date string to validate
            
        Returns:
            True if format is valid, False otherwise
        """
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except (ValueError, TypeError):
            return False
    
    def generate_and_upload_sef_pdf(self, guest_data: Dict, output_path: Path) -> Dict[str, Any]:
        """
        Generate SEF PDF and upload to Dropbox automatically.
        
        Args:
            guest_data: Dictionary containing guest information (name, passport, dates, etc.)
            output_path: Local path where PDF should be saved before uploading
            
        Returns:
            Dictionary with:
            - 'local_path': Path object to local PDF file
            - 'dropbox_metadata': Dictionary with Dropbox upload metadata (path, id, link, size, etc.)
            - 'success': Boolean indicating if upload was successful
            
        Raises:
            ValueError: If guest data validation fails
            FileNotFoundError: If PDF generation fails or file doesn't exist
            Exception: For Dropbox upload errors (with clear error messages)
        """
        try:
            # Step 1: Generate SEF PDF locally
            guest_name = guest_data.get('full_name', guest_data.get('guest_name', 'Unknown'))
            print(f"[SEF] Generating SEF PDF for guest: {guest_name}")
            local_pdf_path = self.pdf_generator.generate_sef_form(guest_data, output_path)
            
            # Verify PDF was generated and exists
            if not local_pdf_path.exists():
                raise FileNotFoundError(
                    f"SEF PDF was not generated at expected path: {output_path}"
                )
            
            print(f"[SEF] PDF generated successfully: {local_pdf_path}")
            print(f"[SEF] File size: {local_pdf_path.stat().st_size} bytes")
            
            # Step 2: Upload to Dropbox
            print(f"[SEF] Uploading PDF to Dropbox folder: {self.dropbox_folder}")
            try:
                dropbox_metadata = self.dropbox_service.upload_file(
                    str(local_pdf_path),
                    self.dropbox_folder
                )
                
                print(f"[SEF] ✓ PDF uploaded to Dropbox successfully!")
                print(f"[SEF]   - Dropbox path: {dropbox_metadata.get('path', 'N/A')}")
                print(f"[SEF]   - File ID: {dropbox_metadata.get('id', 'N/A')}")
                print(f"[SEF]   - Sharing link: {dropbox_metadata.get('link', 'N/A')}")
                print(f"[SEF]   - File size: {dropbox_metadata.get('size', 'N/A')} bytes")
                
                return {
                    'local_path': local_pdf_path,
                    'dropbox_metadata': dropbox_metadata,
                    'success': True
                }
                
            except Exception as dropbox_error:
                error_msg = f"Dropbox upload failed: {str(dropbox_error)}"
                print(f"[SEF] ✗ {error_msg}")
                
                # Return result even if Dropbox upload fails, but mark as unsuccessful
                return {
                    'local_path': local_pdf_path,
                    'dropbox_metadata': None,
                    'success': False,
                    'error': error_msg
                }
                
        except Exception as e:
            error_msg = f"SEF PDF generation or upload failed: {str(e)}"
            print(f"[SEF] ✗ {error_msg}")
            raise
    
    def register_guest(self, guest_data: Dict, output_path: Optional[Path] = None) -> Dict:
        """
        Register a guest with SEF.
        After generating the SEF PDF, upload it to Dropbox automatically.
        
        Args:
            guest_data: Dictionary containing guest information
            output_path: Optional path where PDF should be saved. 
                        If not provided, generates a path based on guest data.
            
        Returns:
            Dictionary with registration confirmation:
            - 'registration_data': SEF registration information
            - 'pdf_local_path': Path to local PDF file
            - 'pdf_dropbox_path': Dropbox path of uploaded PDF
            - 'pdf_dropbox_link': Sharing link to PDF
            - 'pdf_dropbox_id': Dropbox file ID
            - 'upload_success': Boolean indicating Dropbox upload success
            
        Planned behavior:
            - Validate guest data
            - Generate SEF PDF form
            - Upload PDF to Dropbox automatically
            - Submit to SEF system if applicable
            - Return registration confirmation with PDF metadata
            - Handle errors gracefully
        """
        try:
            # Validate guest data first
            print("[SEF] Validating guest data...")
            is_valid, validation_errors = self.validate_guest_data(guest_data)
            if not is_valid:
                error_msg = f"Invalid guest data: {', '.join(validation_errors)}"
                print(f"[SEF] ✗ {error_msg}")
                raise ValueError(error_msg)
            
            print("[SEF] ✓ Guest data valid")
            
            # Generate output path if not provided
            if output_path is None:
                guest_name = guest_data.get('full_name', 'guest').replace(' ', '_')
                reservation_id = guest_data.get('reservation_id', 'unknown')
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"SEF_{guest_name}_{reservation_id}_{timestamp}.pdf"
                output_path = Path.cwd() / "sef_pdfs" / filename
                output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Generate PDF and upload to Dropbox
            pdf_result = self.generate_and_upload_sef_pdf(guest_data, output_path)
            
            # Prepare registration response
            registration_response = {
                'registration_data': self.generate_sef_registration(guest_data),
                'pdf_local_path': str(pdf_result['local_path']),
                'upload_success': pdf_result['success']
            }
            
            # Add Dropbox metadata if upload was successful
            if pdf_result.get('dropbox_metadata'):
                dropbox_meta = pdf_result['dropbox_metadata']
                registration_response.update({
                    'pdf_dropbox_path': dropbox_meta.get('path'),
                    'pdf_dropbox_link': dropbox_meta.get('link'),
                    'pdf_dropbox_id': dropbox_meta.get('id'),
                    'pdf_size': dropbox_meta.get('size')
                })
            else:
                registration_response.update({
                    'pdf_dropbox_path': None,
                    'pdf_dropbox_link': None,
                    'pdf_dropbox_id': None,
                    'upload_error': pdf_result.get('error')
                })
            
            print(f"[SEF] ✓ Guest registration completed")
            return registration_response
            
        except ValueError as e:
            # Re-raise validation errors as-is
            raise
        except Exception as e:
            error_msg = f"SEF registration failed: {str(e)}"
            print(f"[SEF] ✗ {error_msg}")
            raise Exception(error_msg) from e
    
    def get_registration_status(self, registration_id: str) -> Dict:
        """
        Get status of a SEF registration.
        
        Args:
            registration_id: SEF registration ID
            
        Returns:
            Dictionary with registration status and details
            
        Planned behavior:
            - Query SEF system for registration status
            - Return current status and any updates
        """
        # TODO: Implement registration status check
        pass
    
    def export_sef_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Export SEF registration data for a date range.
        
        Args:
            start_date: Start date for export
            end_date: End date for export
            
        Returns:
            List of registration dictionaries
            
        Planned behavior:
            - Fetch all registrations in date range
            - Format for export
            - Return structured data
        """
        # TODO: Implement SEF data export
        pass

