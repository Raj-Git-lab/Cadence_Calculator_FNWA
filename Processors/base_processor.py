"""
Base Processor Class
Contains common functionality for all node processors
"""

import pandas as pd
import numpy as np
from datetime import datetime
from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    """Abstract base class for all node processors"""

    NOT_FOUND = 'not Found!'

    def __init__(self, status_callback=None):
        self.logs = []
        self.status_callback = status_callback
        self.node_name = "BASE"  # Override in child classes

    def log(self, message):
        """Add log message and update UI if callback exists"""
        self.logs.append(message)
        print(message)
        if self.status_callback:
            self.status_callback(message)

    def get_logs(self):
        """Return all logs"""
        return self.logs

    def clear_logs(self):
        """Clear logs"""
        self.logs = []

    # ============== COMMON HELPER FUNCTIONS ==============

    def is_not_found(self, value):
        """Check if value is 'not Found!' or equivalent"""
        if value is None:
            return True
        if pd.isna(value):
            return True
        if isinstance(value, str):
            val_lower = value.strip().lower()
            if val_lower in ['not found!', 'not found', 'nan', 'nat', 'none', '']:
                return True
        return False

    def safe_to_numeric(self, series):
        """Safely convert series to numeric, returning NaN for invalid values"""
        def convert(x):
            if self.is_not_found(x):
                return np.nan
            try:
                return float(x)
            except (ValueError, TypeError):
                return np.nan
        return series.apply(convert)

    def to_string_safe(self, value):
        """Convert any value to string safely"""
        if self.is_not_found(value):
            return self.NOT_FOUND
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.strftime('%Y-%m-%d')
        if pd.isna(value):
            return self.NOT_FOUND
        return str(value)

    def date_to_string(self, date_value):
        """Convert date/timestamp to string format"""
        if self.is_not_found(date_value):
            return self.NOT_FOUND
        try:
            if isinstance(date_value, str):
                if date_value.strip().lower() in ['not found!', 'not found', 'nan', 'nat', '']:
                    return self.NOT_FOUND
                return date_value
            if isinstance(date_value, (pd.Timestamp, datetime)):
                return date_value.strftime('%Y-%m-%d')
            if hasattr(date_value, 'strftime'):
                return date_value.strftime('%Y-%m-%d')
            parsed = pd.to_datetime(date_value, errors='coerce')
            if pd.notna(parsed):
                return parsed.strftime('%Y-%m-%d')
            return self.NOT_FOUND
        except Exception:
            return self.NOT_FOUND

    def add_days_to_date(self, date_value, days):
        """Add days to a date and return as string"""
        if self.is_not_found(date_value):
            return self.NOT_FOUND
        try:
            if isinstance(date_value, str):
                parsed = pd.to_datetime(date_value, errors='coerce')
            elif isinstance(date_value, (pd.Timestamp, datetime)):
                parsed = pd.Timestamp(date_value)
            else:
                parsed = pd.to_datetime(date_value, errors='coerce')

            if pd.isna(parsed):
                return self.NOT_FOUND

            new_date = parsed + pd.Timedelta(days=days)
            return new_date.strftime('%Y-%m-%d')
        except Exception:
            return self.NOT_FOUND

    def create_mixed_column(self, series, fill_value='not Found!'):
        """Create a column that can hold mixed types"""
        result = series.copy()
        result = result.fillna(fill_value)
        return result.astype(str)

    # ============== ABSTRACT METHODS (Must be implemented by child classes) ==============

    @abstractmethod
    def get_required_files(self):
        """Return list of required file descriptions for this processor"""
        pass

    @abstractmethod
    def process(self, files_dict, output_month):
        """Main processing function - must be implemented by each processor"""
        pass

    @abstractmethod
    def get_node_config(self):
        """Return node-specific configuration"""
        pass