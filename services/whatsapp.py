"""
WhatsApp integration service.
Handles sending messages via WhatsApp Business API.
"""

from typing import Optional, Dict
from config.settings import (
    get_whatsapp_token,
    get_whatsapp_phone_number_id,
    get_owner_phone
)


class WhatsAppService:
    """Service class for sending WhatsApp messages via Business API."""
    
    def __init__(self):
        """Initialize WhatsApp client with API credentials."""
        self.token = get_whatsapp_token()
        self.phone_number_id = get_whatsapp_phone_number_id()
        self.owner_phone = get_owner_phone()
        self.base_url = "https://graph.facebook.com/v18.0"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def send_message(self, to_phone: str, message: str) -> Dict:
        """
        Send a text message via WhatsApp.
        
        Args:
            to_phone: Recipient phone number in international format (e.g., +351XXXXXXXXX)
            message: Text message to send
            
        Returns:
            API response dictionary
            
        Planned behavior:
            - Send message via WhatsApp Business API
            - Handle rate limits
            - Return delivery status
        """
        # TODO: Implement WhatsApp API message sending
        pass
    
    def send_template_message(self, to_phone: str, template_name: str, parameters: List[str]) -> Dict:
        """
        Send a template message via WhatsApp.
        
        Args:
            to_phone: Recipient phone number in international format
            template_name: Name of the approved WhatsApp template
            parameters: List of parameter values for the template
            
        Returns:
            API response dictionary
            
        Planned behavior:
            - Send pre-approved template message
            - Fill in template parameters
            - Handle template validation errors
        """
        # TODO: Implement WhatsApp template message sending
        pass
    
    def send_owner_alert(self, message: str) -> Dict:
        """
        Send an alert message to the owner's phone number.
        
        Args:
            message: Alert message text
            
        Returns:
            API response dictionary
            
        Planned behavior:
            - Send message to owner phone number
            - Use for important notifications and alerts
        """
        # TODO: Implement owner alert using send_message
        pass

