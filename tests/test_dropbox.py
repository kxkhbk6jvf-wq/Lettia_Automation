"""
Tests for Dropbox integration service.
"""

import sys
import tempfile
import traceback
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.dropbox_service import DropboxService
from config.settings import get_dropbox_access_token, get_dropbox_sef_folder


def test_dropbox_integration():
    """Test Dropbox file upload integration."""
    try:
        print("=" * 60)
        print("Dropbox Integration Test")
        print("=" * 60)
        print()
        
        # Step 1: Load environment variables
        print("Step 1: Loading environment variables from .env...")
        try:
            access_token = get_dropbox_access_token()
            sef_folder = get_dropbox_sef_folder()
            print("✓ Environment variables loaded successfully")
            print(f"  - Access token: {access_token[:20]}..." if len(access_token) > 20 else f"  - Access token: {access_token}")
            print(f"  - SEF folder: {sef_folder}")
        except Exception as e:
            print(f"✗ Error loading environment variables:")
            traceback.print_exc()
            return False
        print()
        
        # Step 2: Instantiate DropboxService
        print("Step 2: Instantiating DropboxService...")
        try:
            dropbox = DropboxService()
            print("✓ DropboxService instantiated successfully")
        except Exception as e:
            print(f"✗ Error instantiating DropboxService:")
            traceback.print_exc()
            return False
        print()
        
        # Step 3: Create temporary test file
        print("Step 3: Creating temporary test file...")
        try:
            test_content = "Lettia Automation – Dropbox integration test successful"
            test_file_name = "dropbox_test.txt"
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, prefix='dropbox_test_') as temp_file:
                temp_file.write(test_content)
                temp_file_path = temp_file.name
            
            temp_file_path_obj = Path(temp_file_path)
            
            # Rename to desired name in same directory
            final_test_path = temp_file_path_obj.parent / test_file_name
            if final_test_path.exists():
                final_test_path.unlink()
            temp_file_path_obj.rename(final_test_path)
            temp_file_path = str(final_test_path)
            
            print(f"✓ Temporary file created: {temp_file_path}")
            print(f"  - Content: {test_content}")
            print(f"  - Size: {len(test_content)} bytes")
        except Exception as e:
            print(f"✗ Error creating temporary file:")
            traceback.print_exc()
            return False
        print()
        
        # Step 4: Upload file to Dropbox
        print("Step 4: Uploading file to Dropbox...")
        try:
            print(f"  Uploading to folder: {sef_folder}")
            metadata = dropbox.upload_file(temp_file_path, sef_folder)
            
            print(f"✓ File uploaded successfully!")
            print(f"  - Dropbox path: {metadata.get('path', 'N/A')}")
            print(f"  - File ID: {metadata.get('id', 'N/A')}")
            print(f"  - File name: {metadata.get('name', 'N/A')}")
            print(f"  - File size: {metadata.get('size', 'N/A')} bytes")
            print(f"  - Sharing link: {metadata.get('link', 'N/A')}")
            
            # Verify upload success
            if not metadata.get('path'):
                print("  ⚠ Warning: No path returned in metadata")
            if not metadata.get('id'):
                print("  ⚠ Warning: No ID returned in metadata")
                
        except Exception as e:
            print(f"✗ Error uploading file to Dropbox:")
            traceback.print_exc()
            print("\n  Common issues:")
            print("    - Invalid access token")
            print("    - Access token doesn't have required permissions")
            print("    - Network connectivity issues")
            # Clean up temp file before returning
            try:
                Path(temp_file_path).unlink()
            except:
                pass
            return False
        print()
        
        # Step 5: Clean up local temp file
        print("Step 5: Cleaning up local temporary file...")
        try:
            Path(temp_file_path).unlink()
            print("✓ Local temporary file deleted")
        except Exception as e:
            print(f"  ⚠ Warning: Could not delete temporary file: {e}")
        print()
        
        # Summary
        print("=" * 60)
        print("✓ All Dropbox integration tests passed successfully!")
        print("=" * 60)
        print()
        print(f"File uploaded to: {metadata.get('path', 'N/A')}")
        print("You can verify the file in your Dropbox App Folder.")
        print("The file will remain in Dropbox (not deleted by this test).")
        
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("✗ Unexpected error during Dropbox integration test:")
        print("=" * 60)
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_dropbox_integration()
    sys.exit(0 if success else 1)

