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
        preprocessed_data = tender_data.copy()
        
        # Apply general cleaning
        preprocessed_data = self._clean_data(preprocessed_data)
        
        # Apply source-specific preprocessing
        source_name = source_schema.get("source_name", "")
        if source_name and source_name in self.rules:
            preprocessed_data = self._apply_source_rules(preprocessed_data, source_name)
        
        # Process dates
        preprocessed_data = self._process_dates(preprocessed_data, source_schema)
        
        # Process monetary values
        preprocessed_data = self._process_monetary_values(preprocessed_data, source_schema)
        
        # Process text fields
        preprocessed_data = self._process_text_fields(preprocessed_data, source_schema)
        
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
    
    def _clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply general cleaning to all data fields."""
        cleaned_data = {}
        
        for key, value in data.items():
            if isinstance(value, str):
                # Remove extra whitespace
                value = re.sub(r'\s+', ' ', value).strip()
                
                # Remove HTML tags
                value = re.sub(r'<[^>]+>', '', value)
                
                # Replace special characters
                value = value.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
            
            cleaned_data[key] = value
        
        return cleaned_data
    
    def _apply_source_rules(self, data: Dict[str, Any], source_name: str) -> Dict[str, Any]:
        """Apply source-specific preprocessing rules."""
        source_rules = self.rules.get(source_name, {})
        processed_data = data.copy()
        
        # Apply field mappings
        field_mappings = source_rules.get("field_mappings", {})
        for old_field, new_field in field_mappings.items():
            if old_field in processed_data:
                processed_data[new_field] = processed_data[old_field]
        
        return processed_data
    
    def _process_dates(self, data: Dict[str, Any], source_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Process and standardize date fields."""
        processed_data = data.copy()
        source_name = source_schema.get("source_name", "")
        date_format = self.rules.get(source_name, {}).get("date_format", "%Y-%m-%d")
        
        # Identify date fields from schema
        date_fields = []
        for field, field_info in source_schema.items():
            if field_info.get("type") == "date" or "date" in field.lower():
                date_fields.append(field)
        
        # Process each date field
        for field in date_fields:
            if field in processed_data and processed_data[field]:
                try:
                    # Parse the date using the source format
                    date_value = processed_data[field]
                    if isinstance(date_value, str):
                        # Try to parse with the specified format
                        try:
                            date_obj = datetime.datetime.strptime(date_value, date_format)
                        except ValueError:
                            # Try common formats if the specified format fails
                            common_formats = [
                                "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y",
                                "%b %d, %Y", "%d %b %Y", "%B %d, %Y", "%d %B %Y"
                            ]
                            for fmt in common_formats:
                                try:
                                    date_obj = datetime.datetime.strptime(date_value, fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                # If all formats fail, keep the original value
                                continue
                        
                        # Convert to ISO format
                        processed_data[field] = date_obj.strftime("%Y-%m-%d")
                except Exception as e:
                    # If date processing fails, keep the original value
                    print(f"Error processing date field {field}: {e}")
        
        return processed_data
    
    def _process_monetary_values(self, data: Dict[str, Any], source_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Process and standardize monetary values."""
        processed_data = data.copy()
        
        # Identify monetary fields from schema
        monetary_fields = []
        for field, field_info in source_schema.items():
            if field_info.get("type") == "monetary" or any(term in field.lower() for term in ["value", "budget", "cost", "price"]):
                monetary_fields.append(field)
        
        # Process each monetary field
        for field in monetary_fields:
            if field in processed_data and processed_data[field]:
                value = processed_data[field]
                if isinstance(value, str):
                    # Extract currency and numeric value
                    currency_match = re.search(r'([A-Z]{3})', value)
                    currency = currency_match.group(1) if currency_match else ""
                    
                    # Extract numeric value
                    numeric_match = re.search(r'([\d,\.]+)', value)
                    numeric_value = numeric_match.group(1) if numeric_match else ""
                    
                    if numeric_value:
                        # Remove commas and convert to float
                        numeric_value = numeric_value.replace(',', '')
                        try:
                            numeric_value = float(numeric_value)
                            # Format as number with currency
                            if currency:
                                processed_data[field] = f"{numeric_value} {currency}"
                                # Add separate currency field if it doesn't exist
                                currency_field = f"{field}_currency"
                                if currency_field not in processed_data:
                                    processed_data[currency_field] = currency
                            else:
                                processed_data[field] = str(numeric_value)
                        except ValueError:
                            # If conversion fails, keep the original value
                            pass
        
        return processed_data
    
    def _process_text_fields(self, data: Dict[str, Any], source_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Process and clean text fields."""
        processed_data = data.copy()
        source_name = source_schema.get("source_name", "")
        text_cleanup = self.rules.get(source_name, {}).get("text_cleanup", False)
        
        if not text_cleanup:
            return processed_data
        
        # Identify text fields from schema
        text_fields = []
        for field, field_info in source_schema.items():
            if field_info.get("type") == "string" or field_info.get("type") == "text":
                text_fields.append(field)
        
        # Process each text field
        for field in text_fields:
            if field in processed_data and isinstance(processed_data[field], str):
                text = processed_data[field]
                
                # Remove excessive newlines
                text = re.sub(r'\n{3,}', '\n\n', text)
                
                # Remove excessive spaces
                text = re.sub(r' {2,}', ' ', text)
                
                # Fix common encoding issues
                text = text.replace('â€™', "'").replace('â€œ', '"').replace('â€', '"')
                
                processed_data[field] = text
        
        return processed_data
