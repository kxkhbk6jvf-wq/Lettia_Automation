"""
Configuration module for Lettia automation.
"""

from .settings import (
    validate_required_env_var,
    get_missing_env_vars,
    get_lodgify_api_key,
    get_lodgify_property_id,
    get_google_service_account_json,
    get_google_sheet_reservations_id,
    get_google_sheet_sef_id,
    get_google_sheet_sef_template_id,
    get_whatsapp_token,
    get_whatsapp_phone_number_id,
    get_owner_phone,
    get_vat_rate,
    get_airbnb_fee_percent,
    get_lodgify_fee_percent,
    get_stripe_fee_table,
    get_dropbox_access_token,
    get_dropbox_sef_folder,
)

__all__ = [
    'validate_required_env_var',
    'get_missing_env_vars',
    'get_lodgify_api_key',
    'get_lodgify_property_id',
    'get_google_service_account_json',
    'get_google_sheet_reservations_id',
    'get_google_sheet_sef_id',
    'get_google_sheet_sef_template_id',
    'get_whatsapp_token',
    'get_whatsapp_phone_number_id',
    'get_owner_phone',
    'get_vat_rate',
    'get_airbnb_fee_percent',
    'get_lodgify_fee_percent',
    'get_stripe_fee_table',
    'get_dropbox_access_token',
    'get_dropbox_sef_folder',
]

