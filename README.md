
# ETL Automation Script

## Overview
This script automates the extraction, transformation, and loading (ETL) of healthcare-related data from multiple MySQL databases. It processes registration and claims data for various organizations, generates output files, uploads them to SFTP servers, and sends email notifications.

## Features
- Extracts data from multiple databases using SQLAlchemy and pandas.
- Transforms and filters data based on business logic.
- Saves output as `.txt` or `.csv` files.
- Uploads files to SFTP servers using Paramiko.
- Sends email notifications for success or failure.

## Setup
1. Clone the repository.
2. Install dependencies:
   ```bash
   python -m venv .venv
   venv\Scripts\activate.bat
   pip install -r requirements.txt
   ```
3. Create a `.env` file with the required environment variables.

## Environment Variables
- **Database Credentials**:
  - `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME1`, `DB_NAME2`
- **SFTP Credentials**:
  - `GEMS_SERVER`, `GEMS_USERNAME`, `GEMS_PASSWORD`
  - `SAM_SERVER`, `SAM_USERNAME`, `SAM_PASSWORD`
  - `MED_SERVER`, `MED_USERNAME`, `MED_PASSWORD`
  - `HR_SERVER`, `HR_USERNAME`, `HR_PASSWORD`
  - `PMB_SERVER`, `PMB_USERNAME`, `PMB_PASSWORD`
- **Email Configuration**:
  - `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_SERVER`

## Usage
Run the script manually or schedule it:
```bash
python automation.py
```

### Behavior Based on Day of Week
- **Monday**: Includes data from Saturday and Sunday.
- **Other Days**: Includes only yesterday's data.

## Class Descriptions
- `GEMS_DataExtract`: Extracts GEMS registration data.
- `PMB_DataExtract`: Extracts PMB registration data.
- `PMB_SFTP`: Compares GEMS and PMB data, uploads to SFTP, sends email.
- `Send_Email`: Base class for sending email notifications.
- `MEDIKRED_DataExtract`: Extracts Medikredit claims.
- `GHIV_DataExtract`: Extracts GEMS HIV data.
- `SAM_DataExtract`: Extracts SAMWUMED HIV data.
- `HighRisk_DataExtract`: Extracts High Risk data.

## Error Handling
- Errors during extraction or upload trigger email alerts.
- Email includes filename, error message, and upload status.

## Author
Thomas Mohlapo  
Tshela Health Care
