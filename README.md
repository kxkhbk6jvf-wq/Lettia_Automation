# Lettia Automation

Automation system for managing Lettia property operations, including reservations, SEF registrations, financial calculations, and notifications.

## Purpose

This project automates various aspects of property management for Lettia, including:
- **Reservation Management**: Integration with Lodgify API for booking data
- **SEF Compliance**: Automated guest registration with Portuguese immigration services
- **Financial Operations**: Fee calculations, VAT processing, and revenue tracking
- **Document Generation**: PDF creation for invoices, SEF forms, and receipts
- **Notifications**: WhatsApp alerts and owner communications
- **Data Management**: Google Sheets integration for data storage and retrieval

## Setup Instructions

### Prerequisites

- Python 3.11 or higher
- Virtual environment (recommended)
- Docker and Docker Compose (optional, for containerized deployment)

### Installation

1. **Clone the repository** (or navigate to the project directory):
   ```bash
   cd lettia_automation
   ```

2. **Create and activate virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   Create a `.env` file in the project root with the following variables:
   ```env
   # Lodgify Configuration
   LODGIFY_API_KEY=your_api_key_here
   LODGIFY_PROPERTY_ID=your_property_id_here
   
   # Google Services Configuration
   GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account",...}
   GOOGLE_SHEET_RESERVATIONS_ID=your_reservations_sheet_id
   GOOGLE_SHEET_SEF_ID=your_sef_sheet_id
   
   # Dropbox Configuration
   DROPBOX_ACCESS_TOKEN=your_dropbox_access_token
   DROPBOX_SEF_FOLDER=SEF/2024
   
   # WhatsApp Configuration
   WHATSAPP_TOKEN=your_whatsapp_token
   WHATSAPP_PHONE_NUMBER_ID=your_phone_number_id
   OWNER_PHONE=+351XXXXXXXXX
   
   # Financial Configuration
   VAT_RATE=0.06
   AIRBNB_FEE_PERCENT=0.03
   LODGIFY_FEE_PERCENT=0.02
   STRIPE_FEE_TABLE=your_stripe_fee_config
   ```

## How to Run

### CLI Commands

The application uses a simple CLI interface through `main.py`:

**Run the default command**:
```bash
python main.py
```

**Run with a specific command**:
```bash
python main.py <command>
```

Currently, the default command prints:
```
Lettia automation – skeleton ready
```

Additional commands will be implemented as the project develops.

### Running Tests

Run the test suite using pytest:
```bash
pytest
```

Run tests with coverage:
```bash
pytest --cov=. --cov-report=html
```

### Docker Deployment

Build and run using Docker Compose:
```bash
docker-compose up --build
```

Or build the Docker image manually:
```bash
docker build -t lettia-automation .
docker run --env-file .env lettia-automation
```

## Project Structure

```
lettia_automation/
├── main.py                 # CLI entry point
├── config/                 # Configuration management
│   ├── __init__.py
│   └── settings.py        # Environment variable loaders
├── services/              # Core service modules
│   ├── __init__.py
│   ├── lodgify_api.py    # Lodgify API integration
│   ├── google_sheets.py  # Google Sheets operations
│   ├── dropbox_service.py # Dropbox file storage operations
│   ├── whatsapp.py       # WhatsApp notifications
│   ├── finance.py        # Financial calculations
│   ├── sef.py            # SEF registration service
│   ├── pdf_generator.py  # PDF document generation
│   ├── faturacao_csv.py  # Billing CSV generation
│   ├── alerts.py         # Alert notifications
│   └── utils.py          # Utility functions
├── database/             # Database connections
│   ├── __init__.py
│   └── connection.py     # SQLite placeholder (Google Sheets is primary)
├── tests/                # Test suite
│   ├── test_finance.py
│   ├── test_lodgify.py
│   ├── test_sef.py
│   └── test_alerts.py
├── requirements.txt      # Python dependencies
├── Dockerfile           # Docker image configuration
├── docker-compose.yml   # Docker Compose configuration
└── README.md           # This file
```

## Future Roadmap

### Phase 1: Core Integrations
- [ ] Complete Lodgify API integration for reservation fetching
- [ ] Implement Google Sheets read/write operations
- [ ] Set up Dropbox file storage integration
- [ ] Configure WhatsApp Business API messaging

### Phase 2: Financial Operations
- [ ] Complete fee calculation logic (Airbnb, Lodgify, Stripe)
- [ ] Implement VAT calculations and reporting
- [ ] Create revenue tracking and reporting

### Phase 3: SEF Compliance
- [ ] Build SEF registration data generation
- [ ] Implement guest data validation
- [ ] Create SEF form PDF generation
- [ ] Set up automated SEF reporting

### Phase 4: Document Generation
- [ ] Complete invoice PDF generation
- [ ] Implement receipt generation
- [ ] Create SEF form templates
- [ ] Build billing CSV export functionality

### Phase 5: Automation & Scheduling
- [ ] Implement scheduled tasks (cron-like)
- [ ] Set up automated reservation processing
- [ ] Create alert monitoring system
- [ ] Build notification workflows

### Phase 6: Enhanced Features
- [ ] Add error handling and retry logic
- [ ] Implement logging and monitoring
- [ ] Create admin dashboard/web interface
- [ ] Add data export and reporting features

## Development

### Adding New Services

1. Create a new file in the `services/` directory
2. Define classes/functions with proper docstrings
3. Import and use configuration from `config.settings`
4. Add corresponding tests in the `tests/` directory

### Environment Variables

All configuration is managed through environment variables loaded via `python-dotenv`. See `config/settings.py` for available configuration getters.

### Contributing

1. Create a feature branch
2. Implement changes with appropriate tests
3. Ensure all tests pass
4. Update documentation as needed

## License

[Add your license information here]

## Support

For issues or questions, please [create an issue] or contact the development team.

