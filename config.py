"""
Configuration settings for Cadence Calculator
"""

# Application settings
APP_NAME = "Cadence Calculator"
APP_VERSION = "1.0.0"

# File settings
ALLOWED_EXTENSIONS = ['xlsx', 'xls']
MAX_FILE_SIZE_MB = 50

# BLR Configuration
BLR_CORE = ['US', 'UK', 'SG', 'AU', 'AE', 'GB', 'ANY', 'Any']
BLR_CROSS = [
    'US', 'UK', 'GB', 'SA', 'AE', 'DE', 'SG', 'AU', 'IT', 'ES',
    'FR', 'TR', 'Any', 'ANY', 'BR', 'ZA', 'IN', 'NL', 'BE', 'QA', 'AT'
]

# Excluded sources for CrossListing
EXCLUDED_SOURCES = [
    'JP', 'DE', 'TR', 'FR', 'IT', 'ES', 'CN', 'PL', 'NL', 'SE', 'MX', 'EG'
]

# Months list
MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Cadence score mapping based on risk score
RISK_TO_CADENCE = {
    0: 30,
    1: 30,
    2: 60,
    3: 90,
    4: 180,
    5: 365
}