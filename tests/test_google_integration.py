#!/usr/bin/env python3
"""
Integration test for Google Sheets and Google Drive APIs.
Tests authentication and basic read/write operations.
"""

import sys
import json
import traceback
from pathlib import Path
from io import BytesIO

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    get_google_service_account_json,
    get_google_sheet_reservations_id,
    get_google_sef_pdf_folder_id
)


def test_google_integration():
    """Test Google Sheets and Drive integration."""
    try:
        print("=" * 60)
        print("Google Integration Test")
        print("=" * 60)
        print()
        
        # Step 1: Load environment variables
        print("Step 1: Loading environment variables from .env...")
        try:
            service_account_json_str = get_google_service_account_json()
            sheet_id = get_google_sheet_reservations_id()
            folder_id = get_google_sef_pdf_folder_id()
            print("✓ Environment variables loaded successfully")
            print(f"  - Sheet ID: {sheet_id}")
            print(f"  - Folder ID: {folder_id}")
        except Exception as e:
            print(f"✗ Error loading environment variables:")
            traceback.print_exc()
            return False
        print()
        
        # Step 2: Authenticate with Google Service Account
        print("Step 2: Authenticating with Google Service Account...")
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaIoBaseUpload
            
            # Handle service account JSON - could be a file path or JSON string
            service_account_json_path = Path(__file__).parent.parent / service_account_json_str
            
            # Check if it's a file path
            if service_account_json_path.exists():
                print(f"  Loading service account from file: {service_account_json_path}")
                with open(service_account_json_path, 'r') as f:
                    service_account_info = json.load(f)
            else:
                # Try parsing as JSON string
                print("  Parsing service account as JSON string...")
                try:
                    service_account_info = json.loads(service_account_json_str)
                except json.JSONDecodeError:
                    # If it looks like a path but doesn't exist, try relative to project root
                    alt_path = Path(__file__).parent.parent / service_account_json_str
                    if alt_path.exists():
                        print(f"  Loading service account from file: {alt_path}")
                        with open(alt_path, 'r') as f:
                            service_account_info = json.load(f)
                    else:
                        raise ValueError(
                            f"Service account JSON is neither a valid JSON string nor a file path. "
                            f"Tried: {service_account_json_str}"
                        )
            
            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets.readonly',
                    'https://www.googleapis.com/auth/drive.file'
                ]
            )
            
            # Build service clients
            sheets_service = build('sheets', 'v4', credentials=credentials)
            drive_service = build('drive', 'v3', credentials=credentials)
            
            print("✓ Authentication successful")
            print(f"  - Service account email: {service_account_info.get('client_email', 'N/A')}")
        except Exception as e:
            print(f"✗ Error during authentication:")
            traceback.print_exc()
            return False
        print()
        
        # Step 3: Test Google Sheets API
        print("Step 3: Testing Google Sheets API...")
        try:
            # Read first row from "reservations" tab
            range_name = "reservations!A1:Z1"  # First row, columns A to Z
            
            print(f"  Reading first row from sheet: {sheet_id}")
            print(f"  Range: {range_name}")
            
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                print("  ⚠ Warning: No data found in first row (sheet might be empty)")
            else:
                first_row = values[0]
                print(f"✓ Successfully read {len(first_row)} columns from first row:")
                for i, cell_value in enumerate(first_row, start=1):
                    print(f"    Column {i}: {cell_value}")
        except Exception as e:
            print(f"✗ Error reading from Google Sheets:")
            traceback.print_exc()
            print("\n  Common issues:")
            print("    - Service account doesn't have access to the sheet")
            print("    - Sheet ID is incorrect")
            print("    - Sheet doesn't have a 'reservations' tab")
            print("    - Share the sheet with the service account email")
            return False
        print()
        
        # Step 4: Test Google Drive API
        print("Step 4: Testing Google Drive API...")
        try:
            # Create test file content
            test_content = "Lettia Automation – test successful"
            test_file_name = "integration_test.txt"
            
            print(f"  Creating test file: {test_file_name}")
            print(f"  Content: {test_content}")
            
            # Create file metadata
            file_metadata = {
                'name': test_file_name,
                'parents': [folder_id]
            }
            
            # Create media upload
            media = MediaIoBaseUpload(
                BytesIO(test_content.encode('utf-8')),
                mimetype='text/plain',
                resumable=True
            )
            
            # Upload file
            print(f"  Uploading to folder: {folder_id}")
            file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            
            file_id = file.get('id')
            file_name = file.get('name')
            file_link = file.get('webViewLink', 'N/A')
            
            print(f"✓ File uploaded successfully!")
            print(f"  - File ID: {file_id}")
            print(f"  - File Name: {file_name}")
            print(f"  - View Link: {file_link}")
        except Exception as e:
            print(f"✗ Error uploading to Google Drive:")
            error_msg = str(e)
            
            # Check for specific error types
            if "403" in error_msg or "storageQuotaExceeded" in error_msg:
                print("\n  Service Account Storage Error:")
                print("    Service accounts cannot create files in their own Drive.")
                print("    Solutions:")
                print("    1. Share the folder with the service account email:")
                print(f"       {service_account_info.get('client_email', 'service-account@email.com')}")
                print("    2. Or use a Google Shared Drive and upload there")
                print("    3. Or use domain-wide delegation with OAuth")
            elif "404" in error_msg or "not found" in error_msg.lower():
                print("\n  Folder Not Found Error:")
                print("    - Check that the folder ID is correct")
                print("    - Verify the folder exists in Google Drive")
            elif "403" in error_msg or "insufficient permissions" in error_msg.lower():
                print("\n  Permission Error:")
                print("    - Share the folder with the service account email:")
                print(f"       {service_account_info.get('client_email', 'service-account@email.com')}")
                print("    - Grant 'Editor' permissions to the service account")
            else:
                print("\n  Error details:")
            
            traceback.print_exc()
            print(f"\n  Service account email: {service_account_info.get('client_email', 'N/A')}")
            print(f"  Folder ID: {folder_id}")
            return False
        print()
        
        # Summary
        print("=" * 60)
        print("✓ All integration tests passed successfully!")
        print("=" * 60)
        print()
        print(f"Test file uploaded with ID: {file_id}")
        print("You can delete this file from Google Drive if needed.")
        
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("✗ Unexpected error during integration test:")
        print("=" * 60)
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_google_integration()
    sys.exit(0 if success else 1)

