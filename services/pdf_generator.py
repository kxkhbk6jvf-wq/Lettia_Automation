"""
PDF generation service.
Creates PDF documents for various purposes (invoices, SEF forms, etc.).
"""

from typing import Dict, Optional, List
from pathlib import Path
from datetime import datetime
from config.settings import get_dropbox_sef_folder


class PDFGenerator:
    """Service class for generating PDF documents."""
    
    def __init__(self):
        """Initialize PDF generator service."""
        # TODO: Initialize PDF generation library (e.g., reportlab, weasyprint)
    
    def generate_invoice(self, invoice_data: Dict, output_path: Path) -> Path:
        """
        Generate an invoice PDF.
        
        Args:
            invoice_data: Dictionary with invoice information (guest, amount, dates, etc.)
            output_path: Path where PDF should be saved
            
        Returns:
            Path to generated PDF file
            
        Planned behavior:
            - Create invoice with company details
            - Include guest information
            - Include booking details and amounts
            - Apply proper formatting and styling
            - Save to specified path
        """
        # TODO: Implement invoice PDF generation
        pass
    
    def generate_sef_form(self, guest_data: Dict, output_path: Path, auto_upload: bool = False) -> Path:
        """
        Generate a SEF registration form PDF.
        
        Args:
            guest_data: Dictionary with guest and booking information
            output_path: Path where PDF should be saved locally
            auto_upload: If True, automatically upload to Dropbox after generation (default: False)
            
        Returns:
            Path to generated PDF file
            
        Planned behavior:
            - Create SEF form with required fields
            - Fill in guest information
            - Include booking dates and property details
            - Apply SEF form formatting requirements
            - Save to specified path
            - Optionally upload to Dropbox if auto_upload is True
        """
        # TODO: Implement SEF form PDF generation
        # For now, create empty PDF file as placeholder (will be replaced with actual implementation)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if not output_path.exists():
            # Create empty file as placeholder until PDF generation is implemented
            output_path.touch()
        
        # Upload to Dropbox only if explicitly requested
        if auto_upload and output_path.exists() and output_path.suffix.lower() == '.pdf':
            self._upload_sef_pdf_to_dropbox(output_path)
        
        return output_path
    
    def _upload_sef_pdf_to_dropbox(self, pdf_path: Path) -> None:
        """
        Upload SEF PDF to Dropbox after generation.
        Note: This is a helper method. Dropbox upload is typically handled by SEFService.
        
        Args:
            pdf_path: Path to the generated PDF file
        """
        try:
            from services.dropbox_service import DropboxService
            dropbox = DropboxService()
            dropbox_folder = get_dropbox_sef_folder()
            metadata = dropbox.upload_file(str(pdf_path), dropbox_folder)
            print(f"SEF PDF uploaded to Dropbox: {metadata.get('path', 'N/A')}")
        except Exception as e:
            # Log error but don't fail PDF generation if Dropbox upload fails
            print(f"Warning: Failed to upload SEF PDF to Dropbox: {e}")
    
    def generate_receipt(self, receipt_data: Dict, output_path: Path) -> Path:
        """
        Generate a payment receipt PDF.
        
        Args:
            receipt_data: Dictionary with receipt information
            output_path: Path where PDF should be saved
            
        Returns:
            Path to generated PDF file
            
        Planned behavior:
            - Create receipt with payment details
            - Include transaction information
            - Apply proper formatting
            - Save to specified path
        """
        # TODO: Implement receipt PDF generation
        pass
    
    def merge_pdfs(self, pdf_paths: List[Path], output_path: Path) -> Path:
        """
        Merge multiple PDF files into one.
        
        Args:
            pdf_paths: List of paths to PDF files to merge
            output_path: Path where merged PDF should be saved
            
        Returns:
            Path to merged PDF file
            
        Planned behavior:
            - Combine multiple PDFs in order
            - Preserve formatting
            - Save merged file
        """
        # TODO: Implement PDF merging
        pass

