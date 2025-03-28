import os
import re
import json
import datetime
from typing import Dict, Any, List, Optional

class TenderPreprocessor:
    """Preprocessor for tender data before normalization."""
    
    def __init__(self):
        """Initialize the preprocessor."""
        # Load preprocessing rules
        self.rules = self._load_rules()
    
    def preprocess(self, tender_data: Dict[str, Any], source_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess tender data according to source schema."""
        # Debug to track what's being passed in
        print(f"DEBUG: Preprocessing tender with keys: {list(tender_data.keys())}")
        
        # Ensure tender_data is a dictionary
        if not isinstance(tender_data, dict):
            print(f"ERROR: tender_data is not a dictionary: {type(tender_data)}")
            # Return a minimal valid dict to avoid errors
            return {"title": "Error: Invalid tender data type", "error": f"Expected dict, got {type(tender_data)}"}
        
        # Make a copy to avoid modifying the original
        preprocessed_data = tender_data.copy()
        
        # Ensure source_schema is a dictionary with expected structure
        if not isinstance(source_schema, dict):
            print(f"WARNING: source_schema is not a dictionary: {type(source_schema)}")
            source_schema = {}  # Use empty dict as fallback
        
        # Process fields according to schema
        fields_schema = source_schema.get('fields', {}) if isinstance(source_schema, dict) else {}
        
        # Clean and process data using schema information
        preprocessed_data = self._clean_data(preprocessed_data, fields_schema)
        
        # Process date fields
        date_fields = [field for field, info in fields_schema.items() 
                     if isinstance(info, dict) and info.get('type') == 'date']
        if date_fields:
            preprocessed_data = self._process_dates(preprocessed_data, date_fields)
        
        # Process monetary values
        monetary_fields = [field for field, info in fields_schema.items() 
                         if isinstance(info, dict) and info.get('type') == 'monetary']
        if monetary_fields:
            preprocessed_data = self._process_monetary_values(preprocessed_data, monetary_fields)
        
        # Process text fields
        text_fields = [field for field, info in fields_schema.items() 
                     if isinstance(info, dict) and info.get('type') == 'string']
        if text_fields:
            preprocessed_data = self._process_text_fields(preprocessed_data, text_fields)
        
        # Return preprocessed data
        return preprocessed_data
    
    def _load_rules(self) -> Dict[str, Any]:
        """Load preprocessing rules."""
        # In a real implementation, this would load from a file or database
        # For now, we'll define some basic rules inline
        return {
            "adb": {
                "date_format": "%Y-%m-%d",
                "text_cleanup": True,
                "field_mappings": {
                    "published_date": "date_published",
                    "deadline": "closing_date"
                }
            },
            "wb": {
                "date_format": "%d-%b-%Y",
                "text_cleanup": True,
                "field_mappings": {
                    "publication_date": "date_published",
                    "closing_date": "closing_date"
                }
            },
            "ungm": {
                "date_format": "%d-%m-%Y",
                "text_cleanup": True,
                "field_mappings": {
                    "published": "date_published",
                    "deadline": "closing_date"
                }
            },
            # Add rules for other sources as needed
        }
    
    def _clean_data(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Clean data by handling various issues like HTML, extra whitespace, etc."""
        print(f"DEBUG: Cleaning data with {len(schema)} schema fields")
        
        cleaned_data = data.copy()
        
        # Process each field based on schema
        for field_name, field_info in schema.items():
            # Skip if field_info is not a dict
            if not isinstance(field_info, dict):
                continue
                
            # Skip if field doesn't exist in data
            if field_name not in data:
                continue
                
            field_value = data[field_name]
            
            # Skip None values
            if field_value is None:
                continue
                
            # Process based on field type
            field_type = field_info.get('type')
            
            if field_type == 'string' and isinstance(field_value, str):
                # Clean strings
                cleaned_value = self._clean_text(field_value)
                cleaned_data[field_name] = cleaned_value
                
            elif field_type == 'date' and field_value:
                # Dates are handled in _process_dates
                pass
                
            elif field_type == 'monetary' and field_value:
                # Monetary values are handled in _process_monetary_values
                pass
                
            # Handle nested dictionaries
            elif isinstance(field_value, dict) and isinstance(field_info.get('fields'), dict):
                # Recursively clean nested dictionaries
                nested_schema = field_info.get('fields', {})
                cleaned_data[field_name] = self._clean_data(field_value, nested_schema)
                
            # Handle lists of dictionaries
            elif isinstance(field_value, list) and field_info.get('is_array') and field_info.get('item_schema'):
                item_schema = field_info.get('item_schema', {})
                cleaned_items = []
                
                for item in field_value:
                    if isinstance(item, dict):
                        cleaned_item = self._clean_data(item, item_schema)
                        cleaned_items.append(cleaned_item)
                    else:
                        # If it's not a dict, just keep it as is
                        cleaned_items.append(item)
                        
                cleaned_data[field_name] = cleaned_items
                
        return cleaned_data
    
    def _process_dates(self, data: Dict[str, Any], date_fields: List[str]) -> Dict[str, Any]:
        """Process and normalize date fields."""
        processed_data = data.copy()
        
        for field_name in date_fields:
            if field_name in data and data[field_name]:
                field_value = data[field_name]
                
                # Skip if not a string
                if not isinstance(field_value, str):
                    continue
                    
                # Try to parse and normalize date
                try:
                    # Use dateutil if available
                    from dateutil import parser
                    
                    # Parse date
                    parsed_date = parser.parse(field_value)
                    
                    # Format as ISO
                    processed_data[field_name] = parsed_date.strftime('%Y-%m-%d')
                except ImportError:
                    print("dateutil not available, using basic date processing")
                    # Basic date handling (could be enhanced)
                    processed_data[field_name] = field_value
                except Exception as e:
                    print(f"Error parsing date '{field_value}': {e}")
                    # Keep original value
                    processed_data[field_name] = field_value
                    
        return processed_data
    
    def _process_monetary_values(self, data: Dict[str, Any], monetary_fields: List[str]) -> Dict[str, Any]:
        """Process and normalize monetary values."""
        processed_data = data.copy()
        
        # Regular expression for matching currency codes and symbols
        import re
        currency_pattern = r'([A-Z]{3}|\$|€|£|¥)'
        
        for field_name in monetary_fields:
            if field_name in data and data[field_name]:
                field_value = data[field_name]
                
                # Handle string values
                if isinstance(field_value, str):
                    # Look for currency codes/symbols
                    currency_matches = re.findall(currency_pattern, field_value)
                    
                    if currency_matches:
                        # Extract currency
                        currency = currency_matches[0]
                        
                        # Currency symbol to code mapping
                        currency_map = {'$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY'}
                        if currency in currency_map:
                            currency = currency_map[currency]
                            
                        # Extract numeric value - remove non-numeric characters (except decimal point)
                        numeric_part = re.sub(r'[^\d.]', '', field_value)
                        
                        # Update data with separated value and currency
                        if 'currency' not in processed_data:
                            processed_data['currency'] = currency
                            
                        processed_data[field_name] = numeric_part.strip()
                        
        return processed_data
    
    def _process_text_fields(self, data: Dict[str, Any], text_fields: List[str]) -> Dict[str, Any]:
        """Process and clean text fields."""
        processed_data = data.copy()
        
        for field_name in text_fields:
            if field_name in data and data[field_name]:
                field_value = data[field_name]
                
                # Skip if not a string
                if not isinstance(field_value, str):
                    continue
                    
                # Clean the text
                processed_data[field_name] = self._clean_text(field_value)
                
        return processed_data
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text data."""
        # Skip if not a string
        if not isinstance(text, str):
            return text
            
        # Remove extra whitespace
        cleaned_text = ' '.join(text.split())
        
        # Try to use BeautifulSoup for HTML cleaning if available
        try:
            from bs4 import BeautifulSoup
            
            # Check if it looks like HTML
            if '<' in cleaned_text and '>' in cleaned_text:
                soup = BeautifulSoup(cleaned_text, 'html.parser')
                cleaned_text = soup.get_text(' ', strip=True)
        except ImportError:
            pass
            
        # Basic HTML tag removal fallback
        if '<' in cleaned_text and '>' in cleaned_text:
            import re
            cleaned_text = re.sub(r'<[^>]+>', ' ', cleaned_text)
            cleaned_text = ' '.join(cleaned_text.split())
            
        return cleaned_text
