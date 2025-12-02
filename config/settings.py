"""
Settings and configuration management for Lettia automation.
Loads environment variables using python-dotenv.
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Load environment variables from .env file
# .env file is in the project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


def validate_required_env_var(var_name: str, value: Optional[str] = None) -> str:
    """
    Validate that a required environment variable is set.
    
    Args:
        var_name: Name of the environment variable to check
        value: Optional pre-fetched value (if None, will fetch from os.getenv)
        
    Returns:
        The environment variable value
        
    Raises:
        ValueError: If the environment variable is missing or empty, with helpful message
        
    Example:
        >>> api_key = validate_required_env_var('LODGIFY_API_KEY')
    """
    if value is None:
        value = os.getenv(var_name)
    
    if not value or value.strip() == '':
        env_file_path = Path(__file__).parent.parent / '.env'
        raise ValueError(
            f"Required environment variable '{var_name}' is not set or is empty.\n"
            f"Please set it in your .env file at: {env_file_path}\n"
            f"You can copy .env.example to .env and fill in the values."
        )
    
    return value


def get_missing_env_vars() -> List[str]:
    """
    Check which required environment variables are missing.
    
    Returns:
        List of missing environment variable names
        
    Example:
        >>> missing = get_missing_env_vars()
        >>> if missing:
        ...     print(f"Missing variables: {', '.join(missing)}")
    """
    required_vars = [
        'LODGIFY_API_KEY',
        'LODGIFY_PROPERTY_ID',
        'GOOGLE_SERVICE_ACCOUNT_JSON',
        'GOOGLE_SHEET_RESERVATIONS_ID',
        'GOOGLE_SHEET_SEF_ID',
        # Note: GOOGLE_SHEET_SEF_TEMPLATE_ID is optional (falls back to GOOGLE_SHEET_SEF_ID)
        'WHATSAPP_TOKEN',
        'WHATSAPP_PHONE_NUMBER_ID',
        'OWNER_PHONE',
        'STRIPE_FEE_TABLE',
        'DROPBOX_ACCESS_TOKEN',
        'DROPBOX_SEF_FOLDER',
    ]
    
    missing = []
    for var_name in required_vars:
        value = os.getenv(var_name)
        if not value or value.strip() == '':
            missing.append(var_name)
    
    return missing


def get_lodgify_api_key() -> str:
    """Get Lodgify API key from environment variables."""
    return validate_required_env_var('LODGIFY_API_KEY')


def get_lodgify_property_id() -> str:
    """Get Lodgify property ID from environment variables."""
    return validate_required_env_var('LODGIFY_PROPERTY_ID')


def get_google_service_account_json() -> str:
    """Get Google Service Account JSON content from environment variables."""
    return validate_required_env_var('GOOGLE_SERVICE_ACCOUNT_JSON')


def get_google_sheet_reservations_id() -> str:
    """Get Google Sheet ID for reservations from environment variables."""
    return validate_required_env_var('GOOGLE_SHEET_RESERVATIONS_ID')


def get_google_sheet_sef_id() -> str:
    """Get Google Sheet ID for SEF from environment variables."""
    return validate_required_env_var('GOOGLE_SHEET_SEF_ID')


def get_google_sheet_sef_template_id() -> Optional[str]:
    """
    Get Google Sheet ID for SEF template from environment variables.
    
    Returns:
        Template sheet ID if set, None otherwise (fallback to GOOGLE_SHEET_SEF_ID)
    """
    template_id = os.getenv('GOOGLE_SHEET_SEF_TEMPLATE_ID')
    if not template_id or template_id.strip() == '':
        return None
    return template_id.strip()


def get_dropbox_access_token() -> str:
    """Get Dropbox access token from environment variables."""
    return validate_required_env_var('DROPBOX_ACCESS_TOKEN')


def get_dropbox_sef_folder() -> str:
    """Get Dropbox SEF folder path from environment variables."""
    return validate_required_env_var('DROPBOX_SEF_FOLDER')


def get_whatsapp_token() -> str:
    """Get WhatsApp API token from environment variables."""
    return validate_required_env_var('WHATSAPP_TOKEN')


def get_whatsapp_phone_number_id() -> str:
    """Get WhatsApp phone number ID from environment variables."""
    return validate_required_env_var('WHATSAPP_PHONE_NUMBER_ID')


def get_owner_phone() -> str:
    """Get owner phone number from environment variables."""
    return validate_required_env_var('OWNER_PHONE')


def get_vat_rate() -> float:
    """Get VAT rate from environment variables. Defaults to 0.06 (6%)."""
    vat_rate_str = os.getenv('VAT_RATE', '0.06')
    if not vat_rate_str or vat_rate_str.strip() == '':
        vat_rate_str = '0.06'
    try:
        vat_rate = float(vat_rate_str)
        if vat_rate < 0 or vat_rate > 1:
            raise ValueError(f"VAT_RATE should be between 0 and 1, got: {vat_rate}")
        return vat_rate
    except ValueError as e:
        if "could not convert" in str(e):
            raise ValueError(
                f"VAT_RATE must be a valid number (e.g., 0.06 for 6%), got: '{vat_rate_str}'. "
                f"Please check your .env file."
            ) from e
        raise


def get_airbnb_fee_percent() -> float:
    """Get Airbnb fee percentage from environment variables. Defaults to 0.03 (3%)."""
    fee_str = os.getenv('AIRBNB_FEE_PERCENT', '0.03')
    if not fee_str or fee_str.strip() == '':
        fee_str = '0.03'
    try:
        fee = float(fee_str)
        if fee < 0 or fee > 1:
            raise ValueError(f"AIRBNB_FEE_PERCENT should be between 0 and 1, got: {fee}")
        return fee
    except ValueError as e:
        if "could not convert" in str(e):
            raise ValueError(
                f"AIRBNB_FEE_PERCENT must be a valid number (e.g., 0.03 for 3%), got: '{fee_str}'. "
                f"Please check your .env file."
            ) from e
        raise


def get_lodgify_fee_percent() -> float:
    """Get Lodgify fee percentage from environment variables. Defaults to 0.02 (2%)."""
    fee_str = os.getenv('LODGIFY_FEE_PERCENT', '0.02')
    if not fee_str or fee_str.strip() == '':
        fee_str = '0.02'
    try:
        fee = float(fee_str)
        if fee < 0 or fee > 1:
            raise ValueError(f"LODGIFY_FEE_PERCENT should be between 0 and 1, got: {fee}")
        return fee
    except ValueError as e:
        if "could not convert" in str(e):
            raise ValueError(
                f"LODGIFY_FEE_PERCENT must be a valid number (e.g., 0.02 for 2%), got: '{fee_str}'. "
                f"Please check your .env file."
            ) from e
        raise


def get_stripe_fee_table() -> Dict[str, Any]:
    """
    Get Stripe fee table from environment variables.
    Parses JSON string and returns as dictionary.
    
    Returns:
        Dictionary containing Stripe fee configuration
        
    Example:
        >>> fee_table = get_stripe_fee_table()
        >>> print(fee_table['rate'])  # Access fee rate
    """
    fee_table_str = validate_required_env_var('STRIPE_FEE_TABLE')
    
    try:
        fee_table = json.loads(fee_table_str)
        if not isinstance(fee_table, dict):
            raise ValueError(
                f"STRIPE_FEE_TABLE must be a valid JSON object (dict), "
                f"got type: {type(fee_table)}"
            )
        return fee_table
    except json.JSONDecodeError as e:
        raise ValueError(
            f"STRIPE_FEE_TABLE must be valid JSON. Error: {str(e)}\n"
            f"Got value: {fee_table_str[:100]}{'...' if len(fee_table_str) > 100 else ''}\n"
            f"Please check your .env file format. Example: "
            f'{{"fee_type":"percentage","rate":0.029,"fixed":0.30}}'
        ) from e

