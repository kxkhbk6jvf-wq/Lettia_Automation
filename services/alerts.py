"""
Alerts service for monitoring and notification.
Handles system alerts, errors, and important notifications.
"""

from typing import Optional, Dict, List
from datetime import datetime
from services.whatsapp import WhatsAppService
from config.settings import get_owner_phone


class AlertsService:
    """Service class for handling alerts and notifications."""
    
    def __init__(self):
        """Initialize alerts service with notification channels."""
        self.whatsapp = WhatsAppService()
        self.owner_phone = get_owner_phone()
    
    def send_alert(self, message: str, severity: str = "info", channel: str = "whatsapp") -> None:
        """
        Send an alert notification.
        
        Args:
            message: Alert message text
            severity: Alert severity level (info, warning, error, critical)
            channel: Notification channel (whatsapp, email, etc.)
            
        Planned behavior:
            - Format message with severity prefix
            - Send via specified channel
            - Log alert for tracking
        """
        formatted_message = f"[{severity.upper()}] {message}"
        
        if channel == "whatsapp":
            self.whatsapp.send_owner_alert(formatted_message)
        # TODO: Add other notification channels (email, Slack, etc.)
    
    def send_error_alert(self, error: Exception, context: Optional[str] = None) -> None:
        """
        Send an error alert notification.
        
        Args:
            error: Exception that occurred
            context: Optional context information about where error occurred
            
        Planned behavior:
            - Format error message with exception details
            - Include context if provided
            - Send via default alert channel
        """
        message = f"Error occurred: {str(error)}"
        if context:
            message = f"{context} - {message}"
        self.send_alert(message, severity="error")
    
    def send_critical_alert(self, message: str) -> None:
        """
        Send a critical alert notification.
        
        Args:
            message: Critical alert message
            
        Planned behavior:
            - Send immediately via all available channels
            - Use highest priority formatting
        """
        self.send_alert(message, severity="critical", channel="whatsapp")
        # TODO: Send to all notification channels for critical alerts
    
    def send_system_status(self, status: Dict) -> None:
        """
        Send system status update.
        
        Args:
            status: Dictionary with system status information
            
        Planned behavior:
            - Format status information
            - Send periodic status updates
            - Include health check results
        """
        # TODO: Implement system status reporting
        pass

