import json
from typing import Dict, Any, List, Optional
from supabase import create_client, Client

class TenderTrailIntegration:
    """Integration layer for TenderTrail normalization workflow."""
    
    def __init__(self, normalizer, preprocessor, supabase_url, supabase_key):
        """Initialize the integration layer."""
        self.normalizer = normalizer
        self.preprocessor = preprocessor
        self.supabase = create_client(supabase_url, supabase_key)
    
    def process_source(self, source_name: str, batch_size: int = 100) -> Dict[str, Any]:
        """Process tenders from a specific source."""
        # Get source schema
        source_schema = self._get_source_schema(source_name)
        
        # Get target schema
        target_schema = self._get_target_schema()
        
        # Get raw tenders from source table
        raw_tenders = self._get_raw_tenders(source_name, batch_size)
        
        # Process each tender
        processed_count = 0
        success_count = 0
        error_count = 0
        normalized_tenders = []
        errors = []
        
        for tender in raw_tenders:
            try:
                # Preprocess tender
                preprocessed_tender = self.preprocessor.preprocess(tender, source_schema)
                
                # Normalize tender
                normalized_tender = self.normalizer.normalize_tender(
                    preprocessed_tender, source_schema, target_schema
                )
                
                # Add metadata
                normalized_tender['source'] = source_name
                normalized_tender['raw_id'] = tender.get('id')
                normalized_tender['processed_at'] = self._get_current_timestamp()
                
                # Add to batch
                normalized_tenders.append(normalized_tender)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append({
                    'tender_id': tender.get('id'),
                    'error': str(e),
                    'source': source_name
                })
                print(f"Error processing tender {tender.get('id')}: {e}")
            
            processed_count += 1
        
        # Insert normalized tenders into unified table
        if normalized_tenders:
            self._insert_normalized_tenders(normalized_tenders)
        
        # Log errors
        if errors:
            self._log_errors(errors)
        
        return {
            'source_name': source_name,
            'processed_count': processed_count,
            'success_count': success_count,
            'error_count': error_count
        }
    
    def _get_source_schema(self, source_name: str) -> Dict[str, Any]:
        """Get source schema from database or config."""
        try:
            response = self.supabase.table('source_schemas').select('*').eq('name', source_name).execute()
            if response.data:
                return json.loads(response.data[0]['schema'])
            else:
                # Fallback to default schema if not found in database
                return self._get_default_source_schema(source_name)
        except Exception as e:
            print(f"Error getting source schema: {e}")
            return self._get_default_source_schema(source_name)
    
    def _get_default_source_schema(self, source_name: str) -> Dict[str, Any]:
        """Get default schema for a source."""
        # Basic default schemas for common sources
        default_schemas = {
            "adb": {
                "source_name": "adb",
                "title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "published_date": {"type": "date", "maps_to": "date_published"},
                "deadline": {"type": "date", "maps_to": "closing_date"},
                "budget": {"type": "monetary", "maps_to": "tender_value"},
                "location": {"type": "string", "maps_to": "location"},
                "authority": {"type": "string", "maps_to": "issuing_authority"},
                "language": "en"
            },
            "wb": {
                "source_name": "wb",
                "title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "publication_date": {"type": "date", "maps_to": "date_published"},
                "closing_date": {"type": "date", "maps_to": "closing_date"},
                "value": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "borrower": {"type": "string", "maps_to": "issuing_authority"},
                "language": "en"
            },
            "ungm": {
                "source_name": "ungm",
                "title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "published": {"type": "date", "maps_to": "date_published"},
                "deadline": {"type": "date", "maps_to": "closing_date"},
                "value": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "agency": {"type": "string", "maps_to": "issuing_authority"},
                "language": "en"
            }
        }
        
        return default_schemas.get(source_name, {
            "source_name": source_name,
            "language": "en"
        })
    
    def _get_target_schema(self) -> Dict[str, Any]:
        """Get target schema from database or config."""
        try:
            response = self.supabase.table('target_schema').select('*').execute()
            if response.data:
                return json.loads(response.data[0]['schema'])
            else:
                return self._get_default_target_schema()
        except Exception as e:
            print(f"Error getting target schema: {e}")
            return self._get_default_target_schema()
    
    def _get_default_target_schema(self) -> Dict[str, Any]:
        """Get default target schema."""
        return {
            "title": {
                "type": "string",
                "description": "Title of the tender",
                "format": "Title case, max 200 characters"
            },
            "description": {
                "type": "string",
                "description": "Detailed description of the tender",
                "format": "Plain text, max 2000 characters",
                "requires_translation": True
            },
            "date_published": {
                "type": "string",
                "description": "Date when the tender was published",
                "format": "ISO 8601 (YYYY-MM-DD)"
            },
            "closing_date": {
                "type": "string",
                "description": "Deadline for tender submissions",
                "format": "ISO 8601 (YYYY-MM-DD)"
            },
            "tender_value": {
                "type": "string",
                "description": "Estimated value of the tender",
                "format": "Numeric value followed by currency code (e.g., 1000000 USD)"
            },
            "tender_currency": {
                "type": "string",
                "description": "Currency of the tender value",
                "format": "ISO 4217 currency code (e.g., USD, EUR)",
                "extract_from": {
                    "field": "tender_value"
                }
            },
            "location": {
                "type": "string",
                "description": "Location where the project will be implemented",
                "format": "City, Country"
            },
            "issuing_authority": {
                "type": "string",
                "description": "Organization issuing the tender",
                "format": "Official organization name"
            },
            "keywords": {
                "type": "string",
                "description": "Keywords related to the tender",
                "format": "Comma-separated list of keywords",
                "extract_from": {
                    "field": "description"
                }
            },
            "tender_type": {
                "type": "string",
                "description": "Type of tender",
                "format": "One of: Goods, Works, Services, Consulting",
                "extract_from": {
                    "field": "description"
                }
            },
            "project_size": {
                "type": "string",
                "description": "Size of the project",
                "format": "One of: Small, Medium, Large, Very Large",
                "extract_from": {
                    "field": "tender_value"
                }
            },
            "contact_information": {
                "type": "string",
                "description": "Contact information for inquiries",
                "format": "Name, email, phone number",
                "default": ""
            },
            "language": "en"
        }
    
    def _get_raw_tenders(self, source_name: str, batch_size: int) -> List[Dict[str, Any]]:
        """Get raw tenders from source table."""
        try:
            table_name = f"{source_name}_tenders"
            response = self.supabase.table(table_name).select('*').limit(batch_size).execute()
            return response.data
        except Exception as e:
            print(f"Error getting raw tenders: {e}")
            return []
    
    def _insert_normalized_tenders(self, normalized_tenders: List[Dict[str, Any]]) -> None:
        """Insert normalized tenders into unified table."""
        try:
            response = self.supabase.table('unified_tenders').insert(normalized_tenders).execute()
            return response.data
        except Exception as e:
            print(f"Error inserting normalized tenders: {e}")
            return None
    
    def _log_errors(self, errors: List[Dict[str, Any]]) -> None:
        """Log processing errors to database."""
        try:
            response = self.supabase.table('normalization_errors').insert(errors).execute()
            return response.data
        except Exception as e:
            print(f"Error logging errors: {e}")
            return None
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.utcnow().isoformat()
