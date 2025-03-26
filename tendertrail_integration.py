import json
import sys
import subprocess
from typing import Dict, Any, List, Optional
from supabase import create_client, Client
import uuid
import traceback
import datetime
import re
import os

class TenderTrailIntegration:
    """Integration layer for TenderTrail normalization workflow."""
    
    def __init__(self):
        """Initialize the integration."""
        # Set default attribute values
        self.supabase = None
        self.current_source = None
        self.translation_available = False
        self.translator = None
        self.translation_cache = {}

        # Try to import deep_translator for translation capability
        try:
            import deep_translator
            self.translation_available = True
            print("Translation capability is available")
        except ImportError:
            print("Translation library not available. Install with: pip install deep-translator")

        # Initialize Supabase client
        try:
            self.supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
            print("Successfully initialized Supabase client")
        except Exception as e:
            print(f"Failed to initialize Supabase client: {e}")
            traceback.print_exc()
    
    def process_source(self, tenders_or_source, source_name_or_batch_size=None, create_tables=True):
        """
        Process tenders from a source, normalize and insert them into the database.
        This method is overloaded to handle two different call patterns:
        
        1. process_source(tenders, source_name, create_tables=True) - Process a list of tenders
        2. process_source(source_name, batch_size=100, create_tables=True) - Load tenders from database
        """
        # Add debug logging for input parameters
        print(f"DEBUG: process_source called with:")
        print(f"DEBUG: tenders_or_source type: {type(tenders_or_source)}")
        print(f"DEBUG: tenders_or_source value: {tenders_or_source}")
        print(f"DEBUG: source_name_or_batch_size: {source_name_or_batch_size}")
        
        # Detect which call pattern is being used - FIXED LOGIC
        if isinstance(tenders_or_source, (list, tuple)):
            # First pattern with list/tuple: process_source(tenders, source_name)
            tenders = tenders_or_source
            source_name = source_name_or_batch_size
            batch_size = None
            print(f"DEBUG: Using first pattern - direct tender processing with list/tuple")
        elif isinstance(source_name_or_batch_size, int):
            # Second pattern with int batch_size: process_source(source_name, batch_size=100)
            source_name = tenders_or_source
            batch_size = source_name_or_batch_size
            print(f"DEBUG: Using second pattern - fetching from database with batch_size={batch_size}")
            
            # Skip processing if source_name is a single character
            if isinstance(source_name, str) and len(source_name.strip()) <= 1:
                print(f"Skipping invalid source name: {source_name}")
                return 0, 0
                
            # Get tenders from database
            print(f"DEBUG: Current source before _get_raw_tenders: {getattr(self, '_current_source', 'None')}")
            tenders = self._get_raw_tenders(source_name, batch_size)
            print(f"DEBUG: Current source after _get_raw_tenders: {getattr(self, '_current_source', 'None')}")
            print(f"DEBUG: Received tenders type: {type(tenders)}")
            print(f"DEBUG: Number of tenders: {len(tenders) if isinstance(tenders, (list, tuple)) else 'not a list'}")
        else:
            # First pattern with other data: process_source(tenders, source_name)
            tenders = tenders_or_source
            source_name = source_name_or_batch_size
            batch_size = None
            print(f"DEBUG: Using first pattern - direct tender processing with other data type")
            
        print(f"Processing {len(tenders) if isinstance(tenders, (list, tuple)) else 'unknown number of'} tenders from source: {source_name}")
        
        try:
            # Store the current source name for use in normalization
            self._current_source = source_name
            
            # Exit early if no tenders
            if not tenders:
                print(f"No tenders to process for source: {source_name}")
                return 0, 0
            
            # Skip if tenders is just a string (likely a source name character)
            if isinstance(tenders, str) and len(tenders.strip()) <= 1:
                print(f"Skipping invalid tender data: {tenders}")
                return 0, 0
            
            # Track statistics
            processed_count = 0
            error_count = 0
            normalized_tenders = []
            
            # Ensure tenders is a list
            if not isinstance(tenders, (list, tuple)):
                tenders = [tenders]
            
            # Process each tender
            for tender in tenders:
                try:
                    # Skip single character strings
                    if isinstance(tender, str) and len(tender.strip()) <= 1:
                        print(f"Skipping single character tender: {tender}")
                        continue
                        
                    # First attempt direct normalization (preferred for known source formats)
                    normalized_tender = self._normalize_tender(tender, source_name)
                    
                    # If we got a valid normalized tender, use it
                    if normalized_tender and isinstance(normalized_tender, dict) and normalized_tender.get("notice_title"):
                        normalized_tenders.append(normalized_tender)
                        processed_count += 1
                        continue
                    
                    # If direct normalization failed, try the standard process with preprocessor and normalizer
                    if self.preprocessor and self.normalizer:
                        # Check if the preprocessor has the required method
                        if hasattr(self.preprocessor, 'preprocess_tender') and callable(getattr(self.preprocessor, 'preprocess_tender')):
                            # Preprocess the tender
                            preprocessed_tender = self.preprocessor.preprocess_tender(tender)
                            
                            # Check if the normalizer has the required method
                            if hasattr(self.normalizer, 'normalize_tender') and callable(getattr(self.normalizer, 'normalize_tender')):
                                # Normalize the preprocessed tender
                                normalized_tender = self.normalizer.normalize_tender(preprocessed_tender)
                                
                                # Add to the list of normalized tenders
                                normalized_tenders.append(normalized_tender)
                                processed_count += 1
                                continue
                            else:
                                print(f"Warning: Normalizer doesn't have normalize_tender method")
                        else:
                            print(f"Warning: Preprocessor doesn't have preprocess_tender method")
                    
                    # If we reach here, create a minimal record
                    print("Warning: Creating minimal record as normalization failed")
                    minimal_tender = {
                        "notice_id": str(uuid.uuid4()),
                        "notice_type": "Default",
                        "notice_title": f"Untitled Tender from {source_name}",
                        "description": "Normalization failed, creating minimal record",
                        "source": source_name,
                        "country": "",
                        "location": "",
                        "issuing_authority": str(source_name),
                        "date_published": None,
                        "closing_date": None
                    }
                    normalized_tenders.append(minimal_tender)
                    processed_count += 1
                except Exception as e:
                    error_message = f"Error processing tender from {source_name}: {e}"
                    print(error_message)
                    error_count += 1
                    
                    # Create error record
                    self._insert_error(str(source_name), "processing_error", error_message, str(tender) if tender else "")
            
            # Insert all normalized tenders into the database
            if normalized_tenders:
                insert_count = self._insert_normalized_tenders(normalized_tenders, create_tables)
                print(f"Inserted {insert_count} tenders from source: {source_name}")
            else:
                print(f"No tenders were successfully normalized for source: {source_name}")
            
            # Clear the current source after processing
            self._current_source = None
            
            return processed_count, error_count
        except Exception as e:
            print(f"Error processing source {source_name}: {e}")
            traceback.print_exc()
            # Clear the current source in case of error
            self._current_source = None
            return 0, 0
    
    def process_json_tenders(self, json_data, source_name):
        """
        Process tenders from JSON data for a specific source.
        This is a simpler entry point that doesn't require database tables for raw tenders.
        
        Args:
            json_data: List of tender dictionaries or a dictionary containing a list
            source_name: Name of the source (e.g., 'afd', 'ungm', 'sam_gov')
            
        Returns:
            Tuple (processed_count, error_count)
        """
        try:
            print(f"Processing JSON data for source: {source_name}")
            
            # Handle different input structures
            tenders = []
            if isinstance(json_data, list):
                tenders = json_data
                print(f"Found {len(tenders)} tenders in list format")
            elif isinstance(json_data, dict):
                # Try to find a list in the dictionary
                list_found = False
                for key, value in json_data.items():
                    if isinstance(value, list) and value:
                        tenders = value
                        list_found = True
                        print(f"Found {len(tenders)} tenders in dictionary key: '{key}'")
                        break
                
                if not list_found and "data" in json_data and json_data["data"]:
                    if isinstance(json_data["data"], list):
                        tenders = json_data["data"]
                        print(f"Found {len(tenders)} tenders in 'data' field")
                    else:
                        tenders = [json_data["data"]]
                        print("Using 'data' field as a single tender")
            else:
                print(f"Unsupported JSON data type: {type(json_data)}")
                print("Expected a list of tenders or a dictionary containing a list of tenders")
                return 0, 0
            
            # Process the tenders
            if not tenders:
                print(f"No tenders found for source: {source_name}")
                return 0, 0
            
            print(f"Processing {len(tenders)} tenders for source: {source_name}")
            
            # Show a preview of the first tender for debugging
            try:
                if tenders and len(tenders) > 0:
                    first_tender = tenders[0]
                    preview = str(first_tender)[:500] + "..." if len(str(first_tender)) > 500 else str(first_tender)
                    print(f"First tender preview: {preview}")
            except Exception as preview_e:
                print(f"Could not preview first tender: {preview_e}")
            
            return self.process_source(tenders, source_name)
        
        except Exception as e:
            print(f"Error processing JSON data for source {source_name}: {e}")
            traceback.print_exc()
            return 0, 0
    
    def _ensure_dict(self, data: Any) -> Dict[str, Any]:
        """Ensure that data is a dictionary."""
        # Add more debugging
        print(f"Ensuring dictionary for data of type: {type(data)}")
        
        # Check for dict directly first
        if isinstance(data, dict):
            return data
        
        # Handle string case
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    return parsed
                elif isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
                    # If it's a list with dict as first item, return that
                    return parsed[0]
                else:
                    # Create a simple wrapper dict for the parsed data
                    return {"data": parsed, "id": "unknown"}
            except Exception as e:
                # For strings that aren't JSON, create a simple container
                return {"text": data, "id": "unknown"}
        
        # Handle list case
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                return data[0]
            # Try to parse first item if it's a string
            elif isinstance(data[0], str):
                try:
                    parsed = json.loads(data[0])
                    if isinstance(parsed, dict):
                        return parsed
                except:
                    pass
        
        # Check for common object patterns with get methods
        if hasattr(data, 'get') and callable(data.get):
            # Try to access common tender fields
            common_fields = ['id', 'title', 'description', 'data']
            result = {}
            
            # Build a dict from available fields
            for field in common_fields:
                try:
                    value = data.get(field)
                    if value is not None:
                        result[field] = value
                except:
                    pass
            
            # If we found any fields, return the constructed dict
            if result:
                return result
            
            # Special case for 'data' field that might contain nested data
            try:
                data_field = data.get('data')
                if isinstance(data_field, dict):
                    return data_field
                elif isinstance(data_field, str):
                    try:
                        parsed = json.loads(data_field)
                        if isinstance(parsed, dict):
                            return parsed
                    except:
                        pass
            except:
                pass
        
        # Last resort: create a basic placeholder dict
        print(f"Unable to convert {type(data)} to dictionary, creating placeholder")
        return {
            "id": str(id(data)),
            "error": f"Unable to convert {type(data)} to proper dictionary",
            "raw_type": str(type(data))
        }
    
    def _extract_tender_id(self, tender: Any, default_id: int) -> str:
        """Safely extract tender ID from various data formats."""
        # Handle dictionary directly
        if isinstance(tender, dict):
            return tender.get('id', default_id)
        
        # Handle JSON string
        if isinstance(tender, str):
            try:
                parsed = json.loads(tender)
                if isinstance(parsed, dict):
                    return parsed.get('id', default_id)
            except:
                pass
        
        # Handle record-like objects
        if hasattr(tender, 'get') and callable(tender.get):
            id_val = tender.get('id')
            if id_val is not None:
                return id_val
        
        # Fall back to default
        return default_id
    
    def _get_source_schema(self, source_name: str) -> Dict[str, Any]:
        """Get source schema from database or config."""
        try:
            response = self.supabase.table('source_schemas').select('*').eq('name', source_name).execute()
            if response.data:
                schema_data = response.data[0]['schema']
                # Check if schema_data is already a dict (no need to parse)
                if isinstance(schema_data, dict):
                    return schema_data
                # If it's a string, try to parse it
                return json.loads(schema_data) if isinstance(schema_data, str) else schema_data
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
                "notice_title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "publication_date": {"type": "date", "maps_to": "date_published"},
                "due_date": {"type": "date", "maps_to": "closing_date"},
                "contract_amount": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "contractor": {"type": "string", "maps_to": "issuing_authority"},
                "language": "en"
            },
            "afd": {
                "source_name": "afd",
                "notice_title": {"type": "string", "maps_to": "title"},
                "notice_content": {"type": "string", "maps_to": "description"},
                "publication_date": {"type": "string", "maps_to": "date_published"},
                "deadline": {"type": "string", "maps_to": "closing_date"},
                "country": {"type": "string", "maps_to": "location"},
                "agency": {"type": "string", "maps_to": "issuing_authority"},
                "language": "fr"
            },
            "afdb": {
                "source_name": "afdb",
                "title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "publication_date": {"type": "string", "maps_to": "date_published"},
                "closing_date": {"type": "date", "maps_to": "closing_date"},
                "estimated_value": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "language": "en"
            },
            "aiib": {
                "source_name": "aiib",
                "project_notice": {"type": "string", "maps_to": "title"},
                "pdf_content": {"type": "string", "maps_to": "description"},
                "date": {"type": "string", "maps_to": "date_published"},
                "member": {"type": "string", "maps_to": "location"},
                "language": "en"
            },
            "iadb": {
                "source_name": "iadb",
                "notice_title": {"type": "string", "maps_to": "title"},
                "url_pdf": {"type": "string", "maps_to": "document_url"},
                "publication_date": {"type": "date", "maps_to": "date_published"},
                "pue_date": {"type": "date", "maps_to": "closing_date"},
                "country": {"type": "string", "maps_to": "location"},
                "language": "en"
            },
            "sam_gov": {
                "source_name": "sam_gov",
                "opportunity_title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "publish_date": {"type": "timestamp", "maps_to": "date_published"},
                "response_date": {"type": "timestamp", "maps_to": "closing_date"},
                "place_of_performance": {"type": "jsonb", "maps_to": "location"},
                "language": "en"
            },
            "ted_eu": {
                "source_name": "ted_eu",
                "title": {"type": "string", "maps_to": "title"},
                "summary": {"type": "string", "maps_to": "description"},
                "publication_date": {"type": "date", "maps_to": "date_published"},
                "deadline_date": {"type": "date", "maps_to": "closing_date"},
                "organisation_name": {"type": "string", "maps_to": "issuing_authority"},
                "language": "en"
            },
            "ungm": {
                "source_name": "ungm",
                "title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "published_on": {"type": "string", "maps_to": "date_published"},
                "deadline_on": {"type": "string", "maps_to": "closing_date"},
                "beneficiary_countries": {"type": "string", "maps_to": "location"},
                "language": "en"
            },
            "wb": {
                "source_name": "wb",
                "title": {"type": "string", "maps_to": "title"},
                "description": {"type": "string", "maps_to": "description"},
                "publication_date": {"type": "timestamp", "maps_to": "date_published"},
                "deadline": {"type": "timestamp", "maps_to": "closing_date"},
                "country": {"type": "string", "maps_to": "location"},
                "contact_organization": {"type": "string", "maps_to": "issuing_authority"},
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
            # Try to get from database, but don't error if not available
            try:
                response = self.supabase.table('target_schema').select('*').execute()
                if response.data:
                    schema_data = response.data[0]['schema']
                    # Check if schema_data is already a dict (no need to parse)
                    if isinstance(schema_data, dict):
                        print("Retrieved target schema from database")
                        return schema_data
                    # If it's a string, try to parse it
                    parsed_schema = json.loads(schema_data) if isinstance(schema_data, str) else schema_data
                    print("Retrieved and parsed target schema from database")
                    return parsed_schema
                else:
                    print("No schema found in target_schema table, using default")
                    # Try to create target schema table if empty
                    try:
                        self._create_target_schema_table()
                    except Exception as create_e:
                        print(f"Failed to create target schema: {create_e}")
                    # Return default schema
                    return self._get_default_target_schema()
            except Exception as e:
                print(f"Error getting target schema from database: {e}")
                # Try to create the target schema table
                try: 
                    self._create_target_schema_table()
                except Exception as create_e:
                    print(f"Failed to create target schema table: {create_e}")
                
                # Fall back to default schema
                default_schema = self._get_default_target_schema()
                print("Using default target schema")
                return default_schema
        except Exception as e:
            print(f"General error in _get_target_schema: {e}")
            return self._get_default_target_schema()
    
    def _create_target_schema_table(self) -> None:
        """Create target_schema table if it doesn't exist and insert default schema."""
        try:
            # Check if table already exists using simple query
            try:
                try:
                    # Try direct query
                    response = self.supabase.table('target_schema').select('id').limit(1).execute()
                    if hasattr(response, 'data'):
                        print("target_schema table already exists")
                        
                        # If the table exists but is empty, try to populate it
                        if not response.data:
                            try:
                                print("Adding default schema to empty target_schema table")
                                default_schema = self._get_default_target_schema()
                                self.supabase.table('target_schema').insert({
                                    'schema': default_schema
                                }).execute()
                                print("Successfully added default schema to target_schema")
                            except Exception as e:
                                print(f"Error adding default schema: {e}")
                        
                        return
                except Exception as e:
                    # If the error indicates the table doesn't exist, log it
                    if "relation" in str(e) and "does not exist" in str(e):
                        print(f"target_schema table doesn't exist, but may be created by another process")
                    else:
                        print(f"target_schema table check failed: {e}")
            
            except Exception as e:
                print(f"Error checking target_schema: {e}")
            
            # In API-only mode, we can't create tables directly
            print("Cannot create target_schema table in API-only mode")
            print("Please create the table using the Supabase UI or SQL Editor with this schema:")
            print("""
            CREATE TABLE IF NOT EXISTS public.target_schema (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                schema JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            );
            """)
            
            # We'll continue with the in-memory default schema
            print("Using in-memory default schema")
        except Exception as general_e:
            print(f"General error in _create_target_schema_table: {general_e}")
            print("Continuing with in-memory schema")
    
    def _get_raw_tenders(self, source_name: str, batch_size: int) -> List[Dict[str, Any]]:
        """Get raw tenders from source table."""
        try:
            print(f"DEBUG: _get_raw_tenders called with source_name: {source_name}, batch_size: {batch_size}")
            print(f"DEBUG: Current source at start: {getattr(self, '_current_source', 'None')}")
            
            # Store the source name for use in normalization
            self._current_source = source_name
            print(f"DEBUG: Set current source to: {self._current_source}")
            
            # First, try the source_tenders table format
            table_name = f"{source_name}_tenders"
            try:
                print(f"DEBUG: Attempting to query table: {table_name}")
                response = self.supabase.table(table_name).select('*').limit(batch_size).execute()
                if response and response.data:
                    print(f"DEBUG: Found data in {table_name}. First item type: {type(response.data[0])}")
                    # Process tenders and ensure source name is preserved
                    processed = []
                    for tender in response.data:
                        if isinstance(tender, dict):
                            # Ensure source name is correct
                            tender['source'] = source_name
                            # Store raw data if not already present
                            if 'raw_data' not in tender:
                                tender['raw_data'] = json.dumps(tender)
                            processed.append(tender)
                        elif isinstance(tender, str):
                            # Skip single character strings as they're likely not valid tenders
                            if len(tender.strip()) <= 1:
                                print(f"Skipping single character tender: {tender}")
                                continue
                                
                            try:
                                parsed = json.loads(tender)
                                if isinstance(parsed, dict):
                                    parsed['source'] = source_name
                                    parsed['raw_data'] = tender
                                    tender = parsed
                                else:
                                    # If parsed but not a dict, wrap it
                                    tender = {
                                        'content': tender,
                                        'source': source_name,
                                        'raw_data': tender
                                    }
                            except:
                                # If parsing fails, wrap the string
                                tender = {
                                    'content': tender,
                                    'source': source_name,
                                    'raw_data': tender
                                }
                            processed.append(tender)
                    print(f"Processed {len(processed)} tenders from {table_name}")
                    return processed
            except Exception as e:
                print(f"DEBUG: Error querying {table_name}: {str(e)}")
            
            # Try direct source name as table
            try:
                print(f"DEBUG: Attempting to query direct table: {source_name}")
                # Print a sample of the first tender to diagnose issues
                try:
                    sample_response = self.supabase.table(source_name).select('*').limit(1).execute()
                    if sample_response and sample_response.data:
                        print(f"DEBUG: Sample tender from {source_name}: {json.dumps(sample_response.data[0])[:500]}...")
                except Exception as sample_e:
                    print(f"DEBUG: Error getting sample tender: {sample_e}")
                
                # First try with processed field
                try:
                    response = self.supabase.table(source_name).select('*').eq('processed', False).limit(batch_size).execute()
                    # Mark processed records
                    if response and response.data:
                        print(f"DEBUG: Found {len(response.data)} unprocessed tenders in {source_name}")
                        ids = []
                        processed = []
                        for item in response.data:
                            # Skip single character strings
                            if isinstance(item, str) and len(item.strip()) <= 1:
                                print(f"Skipping single character tender: {item}")
                                continue
                                
                            if isinstance(item, dict):
                                # Ensure source name is correct
                                item['source'] = source_name
                                if 'id' in item:
                                    ids.append(item['id'])
                                # Store raw data if not already present
                                if 'raw_data' not in item:
                                    item['raw_data'] = json.dumps(item)
                            elif isinstance(item, str):
                                try:
                                    parsed = json.loads(item)
                                    if isinstance(parsed, dict):
                                        parsed['source'] = source_name
                                        parsed['raw_data'] = item
                                        item = parsed
                                    else:
                                        # If parsed but not a dict, wrap it
                                        item = {
                                            'content': item,
                                            'source': source_name,
                                            'raw_data': item
                                        }
                                except:
                                    # If parsing fails, wrap the string
                                    item = {
                                        'content': item,
                                        'source': source_name,
                                        'raw_data': item
                                    }
                            processed.append(item)
                        
                        if ids:
                            self.supabase.table(source_name).update({'processed': True}).in_('id', ids).execute()
                        
                        return processed
                except Exception as e:
                    print(f"DEBUG: Error querying with processed field, trying without: {e}")
                    # Try without processed field
                    response = self.supabase.table(source_name).select('*').limit(batch_size).execute()
                    if response and response.data:
                        # Process tenders and ensure source name is preserved
                        processed = []
                        for item in response.data:
                            # Skip single character strings
                            if isinstance(item, str) and len(item.strip()) <= 1:
                                print(f"Skipping single character tender: {item}")
                                continue
                                
                            if isinstance(item, dict):
                                # Ensure source name is correct
                                item['source'] = source_name
                                # Store raw data if not already present
                                if 'raw_data' not in item:
                                    item['raw_data'] = json.dumps(item)
                            elif isinstance(item, str):
                                try:
                                    parsed = json.loads(item)
                                    if isinstance(parsed, dict):
                                        parsed['source'] = source_name
                                        parsed['raw_data'] = item
                                        item = parsed
                                    else:
                                        # If parsed but not a dict, wrap it
                                        item = {
                                            'content': item,
                                            'source': source_name,
                                            'raw_data': item
                                        }
                                except:
                                    # If parsing fails, wrap the string
                                    item = {
                                        'content': item,
                                        'source': source_name,
                                        'raw_data': item
                                    }
                            processed.append(item)
                        return processed
            except Exception as e:
                print(f"DEBUG: Error querying table {source_name}: {e}")
            
            # If we reach here, it means we couldn't find any valid tenders
            # Instead of processing source name characters, return empty list
            print(f"No valid tenders found for source {source_name}")
            return []
            
        except Exception as e:
            print(f"Error getting raw tenders: {e}")
            return []
    
    def _process_raw_tenders(self, raw_data: List[Any]) -> List[Dict[str, Any]]:
        """Process raw tenders data to ensure all items are dictionaries."""
        processed_tenders = []
        
        # Extra debugging to understand the data format
        sample_item = raw_data[0] if raw_data else None
        print(f"Sample raw tender type: {type(sample_item)}")
        if sample_item:
            print(f"Sample raw tender preview: {str(sample_item)[:200]}")
        
        # Process each item
        for item in raw_data:
            try:
                # First check if it's a string that needs to be parsed
                if isinstance(item, str):
                    try:
                        parsed_item = json.loads(item)
                        processed_tenders.append(parsed_item)
                        continue
                    except json.JSONDecodeError:
                        # Not valid JSON, keep as is - we'll handle it in process_source
                        processed_tenders.append(item)
                        continue
                
                # If it's a dict, use it directly
                if isinstance(item, dict):
                    processed_tenders.append(item)
                    continue
                    
                # If it has a 'data' field that contains the tender
                if hasattr(item, 'get') and callable(item.get):
                    try:
                        data = item.get('data')
                        if data is not None:
                            if isinstance(data, str):
                                try:
                                    parsed_data = json.loads(data)
                                    processed_tenders.append(parsed_data)
                                    continue
                                except:
                                    # Not valid JSON, keep the string
                                    processed_tenders.append(data)
                                    continue
                            else:
                                processed_tenders.append(data)
                                continue
                    except:
                        # Fallback if get fails
                        pass
                
                # Add the item as-is and let process_source handle it
                processed_tenders.append(item)
                
            except Exception as e:
                print(f"Error processing raw tender item: {e}")
                # Add the raw item anyway, we'll try to handle it in process_source
                processed_tenders.append(item)
        
        return processed_tenders
    
    def _insert_normalized_tenders(self, normalized_tenders, create_tables=False):
        """
        Insert normalized tenders into the unified database.
        
        Args:
            normalized_tenders (list): List of normalized tenders to insert
            create_tables (bool): Whether to create tables if they don't exist
        
        Returns:
            int: Number of inserted tenders
        """
        if not normalized_tenders:
            return 0
            
        print(f"Preparing to insert {len(normalized_tenders)} tenders")
        batch_size = 10  # Insert in smaller batches to avoid timeouts
        
        # Initialize translator if needed
        if self.translator is None and self.translation_available:
            try:
                from deep_translator import GoogleTranslator
                self.translator = GoogleTranslator(source='auto', target='en')
                print("Translation capability is available")
            except ImportError:
                self.translation_available = False
                print("Translation capability not available")
        
        # Initialize Supabase client if needed
        if not self.supabase:
            self._init_supabase_client()
        
        # Only try to create the table once per session to avoid excessive warnings
        if create_tables and not hasattr(self, '_table_creation_attempted'):
            self._table_creation_attempted = True
            try:
                # Create table if needed
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS unified_tenders (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    title TEXT,
                    description TEXT,
                    date_published DATE,
                    closing_date DATE,
                    tender_value NUMERIC,
                    tender_currency TEXT,
                    location TEXT,
                    country TEXT,
                    issuing_authority TEXT,
                    keywords TEXT[],
                    tender_type TEXT,
                    project_size TEXT,
                    contact_information TEXT,
                    source TEXT,
                    raw_id TEXT,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    metadata JSONB
                );
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                """
                # Try to execute SQL directly using RPC if available
                result = self.supabase.rpc('exec_sql', {'query': create_table_sql}).execute()
                if hasattr(result, 'error') and result.error:
                    print(f"Notice: Table creation not supported via API. Will insert assuming table exists.")
                else:
                    print("Created unified_tenders table if it didn't exist")
            except Exception as e:
                print(f"Notice: Table operations not available via API. Will continue with existing table.")
            
        # Prepare records for insertion
        records = []
        
        for tender in normalized_tenders:
            if not tender:
                continue
                
            # Convert metadata to JSON string if it's a dictionary
            if isinstance(tender.get('metadata'), dict):
                # Convert to JSON string
                tender['metadata'] = json.dumps(tender['metadata'])
            
            # Map normalized tender fields to database columns
            record = {
                'title': tender.get('notice_title', '').strip(),
                'description': tender.get('description', ''),
                'date_published': tender.get('date_published'),
                'closing_date': tender.get('closing_date'),
                'tender_value': tender.get('tender_value'),
                'tender_currency': tender.get('currency'),
                'location': tender.get('location'),
                'country': tender.get('country', ''),  # Add country field directly
                'issuing_authority': tender.get('issuing_authority'),
                'tender_type': tender.get('notice_type'),
                'contact_information': ', '.join(filter(None, [
                    tender.get('contact_email'),
                    tender.get('contact_phone')
                ])),
                'source': tender.get('source'),
                'raw_id': tender.get('notice_id'),
                'metadata': tender.get('metadata'),
                'keywords': []  # Initialize empty array for keywords
            }
            
            # Try to translate description if it's not in English, but don't fail if it doesn't work
            if self.translation_available and record['description'] and len(record['description']) > 0:
                try:
                    if self._detect_non_english(record['description']):
                        original_desc = record['description']
                        translated_desc = self._translate_text(original_desc)
                        if translated_desc and translated_desc != original_desc:
                            record['description'] = translated_desc
                except Exception as e:
                    print(f"Error during description translation: {str(e)[:100]}...")
                    # Continue with original description if translation fails
            
            records.append(record)
        
        # Insert records in batches
        inserted_count = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            print(f"Inserting batch {i//batch_size + 1}/{len(records)//batch_size + (1 if len(records) % batch_size > 0 else 0)}")
            
            try:
                result = self.supabase.table('unified_tenders').insert(batch).execute()
                if hasattr(result, 'error') and result.error:
                    print(f"Error inserting batch: {result.error}")
                else:
                    inserted_count += len(batch)
                    print(f"Successfully inserted batch with {len(batch)} tenders")
            except Exception as e:
                print(f"Error inserting batch: {str(e)[:100]}...")
        
        print(f"Successfully inserted {inserted_count} out of {len(normalized_tenders)} tenders")
        return inserted_count
    
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

    def _clean_html(self, html_content):
        """Remove HTML tags and convert entities to get clean text."""
        if not html_content:
            return ""
            
        # Replace common HTML entities
        html_content = html_content.replace("&nbsp;", " ")
        html_content = html_content.replace("&amp;", "&")
        html_content = html_content.replace("&lt;", "<")
        html_content = html_content.replace("&gt;", ">")
        html_content = html_content.replace("&quot;", "\"")
        html_content = html_content.replace("&#39;", "'")
        
        # Replace <br>, <p>, <div> tags with newlines
        html_content = re.sub(r'<br\s*/?>|</p>|</div>', '\n', html_content)
        
        # Replace multiple newlines with a single newline
        html_content = re.sub(r'\n\s*\n', '\n\n', html_content)
        
        # Remove all HTML tags
        html_content = re.sub(r'<[^>]*>', '', html_content)
        
        # Clean up extra spaces
        html_content = re.sub(r' +', ' ', html_content)
        
        return html_content.strip()
        
    def _standardize_country(self, country):
        """Standardize country names to proper case format."""
        if not country:
            return ""
            
        # Handle empty strings
        country = country.strip()
        if not country:
            return ""
            
        # Already proper case, return as is
        if not country.isupper() and not country.islower():
            return country
            
        # Uppercase country codes that should remain uppercase
        if len(country) <= 3 and country.isupper():
            return country
            
        # Convert to proper case
        words = country.split()
        proper_case = []
        
        for word in words:
            # Common country prefixes remain lowercase after first word
            if word.lower() in ['of', 'the', 'and', 'de', 'del', 'da', 'dos'] and proper_case:
                proper_case.append(word.lower())
            # Special case for USA, UK, UAE, etc.
            elif word.upper() in ['USA', 'UK', 'UAE', 'EU', 'DRC']:
                proper_case.append(word.upper())
            else:
                proper_case.append(word.capitalize())
                
        return ' '.join(proper_case)
        
    def _extract_numeric(self, value):
        """Extract numeric value from various formats."""
        if not value:
            return None
            
        if isinstance(value, (int, float)):
            return value
            
        if isinstance(value, str):
            # Try to extract numeric value from string using regex
            matches = re.search(r'([\d,]+\.?\d*|\d*\.?\d+)', value)
            if matches:
                # Extract the matched part and remove commas
                number_str = matches.group(1).replace(',', '')
                try:
                    return float(number_str)
                except ValueError:
                    pass
                    
        return None
        
    def _extract_currency(self, value):
        """Extract currency code from value string."""
        if not value:
            return None
            
        # Common currency symbols and their codes
        currency_map = {
            '$': 'USD',
            '€': 'EUR',
            '£': 'GBP',
            '¥': 'JPY',
            '₹': 'INR',
            'USD': 'USD',
            'EUR': 'EUR',
            'GBP': 'GBP',
            'JPY': 'JPY',
            'CHF': 'CHF',
            'CAD': 'CAD',
            'AUD': 'AUD',
            'INR': 'INR',
            'CNY': 'CNY',
            'ZAR': 'ZAR'
        }
        
        if isinstance(value, str):
            # First check for currency codes or symbols at the start
            for symbol, code in currency_map.items():
                if value.strip().startswith(symbol):
                    return code
                    
            # Look for currency names in the string
            value_lower = value.lower()
            for name, code in {
                'dollar': 'USD',
                'euro': 'EUR',
                'pound': 'GBP',
                'yen': 'JPY',
                'rupee': 'INR',
                'yuan': 'CNY',
                'rand': 'ZAR',
                'franc': 'CHF'
            }.items():
                if name in value_lower:
                    return code
                    
        return None
        
    def _normalize_tender(self, tender, source_name=None):
        """
        Normalize a tender from various formats into a standardized format.
        This handles both dictionary and string inputs, extracts key fields,
        and returns a cleaned structure.
        """
        if not tender:
            print("Warning: Empty tender received")
            return None

        # Convert tender to dictionary if it's a string
        if isinstance(tender, str):
            try:
                tender = json.loads(tender)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse tender string: {tender[:100]}...")
                return None

        if not isinstance(tender, dict):
            print(f"Warning: Unsupported tender type: {type(tender)}")
            return None

        # Set source if provided
        if source_name and not tender.get("source"):
            tender["source"] = source_name

        # Extract key fields with helper method
        normalized_tender = {
            "notice_id": self._extract_field(tender, ["id", "notice_id", "tender_id", "opportunity_id", "reference", "solicitation_number"]),
            "notice_type": self._extract_field(tender, ["notice_type", "tender_type", "type", "opportunity_type"]),
            "notice_title": self._extract_field(tender, ["title", "notice_title", "tender_title", "opportunity_title", "project_notice"]),
            "description": self._extract_field(tender, ["description", "summary", "notice_description", "tender_description", "pdf_content"]),
            "date_published": self._format_date(self._extract_field(tender, ["publication_date", "published_on", "publish_date", "date_published", "date"])),
            "closing_date": self._format_date(self._extract_field(tender, ["deadline", "deadline_date", "closing_date", "deadline_on", "response_date", "pue_date"])),
            "tender_value": None,  # Will extract below
            "currency": None,  # Will extract below
            "issuing_authority": self._extract_field(tender, ["issuing_authority", "authority", "organisation_name", "buyer", "agency_name"]),
            "country": None,  # Will extract below
            "location": self._extract_field(tender, ["location", "place_of_performance", "city"]),
            "source": tender.get("source", source_name),
            "categories": self._extract_field(tender, ["categories", "category", "sector", "classification_code"]),
            "contact_email": self._extract_field(tender, ["contact_email", "email"]),
            "contact_phone": self._extract_field(tender, ["contact_phone", "phone"]),
            "url": self._extract_field(tender, ["url", "notice_url", "tender_url", "contact_url"]),
            "bid_reference_no": self._extract_field(tender, ["bid_reference_no", "solicitation_number", "reference", "project_number"])
        }

        # Extract country from different sources
        country = self._extract_field(tender, ["country", "beneficiary_countries", "member"])
        
        # For sam.gov, try to extract from place_of_performance
        if not country and source_name == "sam_gov" and isinstance(tender.get("place_of_performance"), dict):
            place = tender.get("place_of_performance")
            if isinstance(place.get("country"), dict) and place["country"].get("code"):
                # Convert country code to name if possible
                country_code = place["country"].get("code")
                # In a real system, you'd use a country code lookup table here
                # This is a simple example for common country codes
                country_map = {
                    "USA": "United States",
                    "GBR": "United Kingdom",
                    "CAN": "Canada", 
                    "AUS": "Australia",
                    "IND": "India",
                    "CHN": "China",
                    "JPN": "Japan",
                    "DEU": "Germany",
                    "FRA": "France",
                    "ITA": "Italy",
                    "MEX": "Mexico",
                    "BRA": "Brazil",
                    "ZAF": "South Africa",
                    "RUS": "Russia",
                    "KOR": "South Korea",
                    "SGP": "Singapore",
                    "AFG": "Afghanistan",
                    "SLE": "Sierra Leone"
                }
                country = country_map.get(country_code, country_code)
            elif isinstance(place.get("country"), str):
                country = place.get("country")
                
        # Standardize country name format
        if country:
            normalized_tender["country"] = self._standardize_country(country)
        
        # Extract tender value and currency
        # First check if there's a dedicated value field
        value_field = self._extract_field(tender, ["tender_value", "value", "contract_value", "amount", "budget"])
        currency_field = self._extract_field(tender, ["currency", "tender_currency"])
        
        # Extract numeric value
        normalized_tender["tender_value"] = self._extract_numeric(value_field)
        
        # Extract currency
        currency = None
        if currency_field:
            currency = currency_field
        elif isinstance(value_field, str):
            # Try to extract currency from the value string
            currency = self._extract_currency(value_field)
            
        if currency:
            normalized_tender["currency"] = currency

        # Ensure at least a default title exists
        if not normalized_tender["notice_title"]:
            normalized_tender["notice_title"] = f"Untitled Tender from {source_name}"
        
        # Clean description if it contains HTML
        description = normalized_tender["description"]
        if description and ("<" in description and ">" in description):
            normalized_tender["description"] = self._clean_html(description)

        # Store the raw data for reference
        normalized_tender["metadata"] = {
            "raw_data": json.dumps(tender),
            "id": self._extract_field(tender, ["id"]),
            "project_name": self._extract_field(tender, ["project_name"]),
            "project_id": self._extract_field(tender, ["project_id", "project_number"]),
            "procurement_method": self._extract_field(tender, ["procurement_method", "procedure_type"]),
            "processed": tender.get("processed", False)
        }

        # Initialize translation if needed
        if self.translator is None and self.translation_available:
            try:
                from deep_translator import GoogleTranslator
                self.translator = GoogleTranslator(source='auto', target='en')
                self.translation_cache = {}
            except ImportError:
                self.translation_available = False
                print("Translation capability not available")

        # Translate title if it contains non-English content
        if normalized_tender["notice_title"]:
            original_title = normalized_tender["notice_title"]
            translated_title = self._translate_text(original_title)
            if translated_title != original_title:
                normalized_tender["metadata"]["original_title"] = original_title
                normalized_tender["notice_title"] = translated_title
                print(f"Translated title from: '{original_title[:30]}...' to '{translated_title[:30]}...'")

        # Translate description if it contains non-English content
        if normalized_tender["description"]:
            description = normalized_tender["description"]
            if "<" in description and ">" in description:
                # Handle HTML content
                original_desc = description
                translated_desc = self._translate_html(description)
                if translated_desc != original_desc:
                    normalized_tender["metadata"]["original_description"] = original_desc
                    normalized_tender["description"] = translated_desc
                    print(f"Translated HTML description: {len(original_desc)} chars to {len(translated_desc)} chars")
            else:
                # Plain text description
                original_desc = description
                translated_desc = self._translate_text(description)
                if translated_desc != original_desc:
                    normalized_tender["metadata"]["original_description"] = original_desc
                    normalized_tender["description"] = translated_desc
                    print(f"Translated description: {len(original_desc)} chars to {len(translated_desc)} chars")

        # Translate issuing authority if needed
        if normalized_tender["issuing_authority"] and self._detect_non_english(normalized_tender["issuing_authority"]):
            original_auth = normalized_tender["issuing_authority"]
            translated_auth = self._translate_text(original_auth)
            if translated_auth != original_auth:
                normalized_tender["metadata"]["original_issuing_authority"] = original_auth
                normalized_tender["issuing_authority"] = translated_auth

        return normalized_tender
        
    def _detect_non_english(self, text):
        """Detect if text is likely not English based on common words and patterns."""
        if not text or len(text) < 5:
            return False
            
        # Simple fast check for non-ASCII characters
        if any(ord(c) > 127 for c in text):
            return True
            
        # List of common words in various European languages that aren't English words
        non_english_words = [
            # German
            'der', 'die', 'das', 'und', 'für', 'mit', 'von', 'zu', 'ist', 'auf', 'nicht', 'ein', 'eine',
            # French
            'le', 'la', 'les', 'du', 'de', 'des', 'et', 'en', 'un', 'une', 'est', 'sont', 'avec', 'pour', 'dans',
            # Spanish
            'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'y', 'o', 'pero', 'porque', 'como', 'qué',
            # Italian
            'il', 'lo', 'la', 'i', 'gli', 'le', 'un', 'uno', 'una', 'del', 'della', 'e', 'ed', 'o', 'ma', 'perché',
            # Portuguese
            'o', 'a', 'os', 'as', 'um', 'uma', 'uns', 'umas', 'e', 'ou', 'mas', 'porque', 'como',
            # Dutch
            'de', 'het', 'een', 'en', 'van', 'voor', 'met', 'op', 'in', 'is', 'zijn', 'niet',
            # Special foreign-language markers
            'gmbh', 'mbh', 'sarl', 'bvba', 'sprl', 'ltda', 'aménagement', 'amélioration', 'développement',
            'adquisición', 'suministro', 'construcción', 'rehabilitación'
        ]
        
        # Convert text to lowercase and split into words
        words = re.findall(r'\b\w+\b', text.lower())
        
        if not words:
            return False
            
        # Count how many words match non-English common words
        non_english_count = sum(1 for word in words if word in non_english_words)
        
        # If more than 5% of words are from non-English common words, consider it non-English
        # Lower threshold to catch more cases
        if len(words) > 5 and non_english_count / len(words) > 0.05:
            return True
            
        # Check for common non-English characters: ä, ö, ü, é, è, ê, ñ, etc.
        non_english_chars = 'äöüßéèêëàâçñøåæœÿòùìíîïðúû'
        if any(c in non_english_chars for c in text.lower()):
            return True
            
        return False

    def _extract_field(self, data, possible_keys, default=None):
        """Extract a field value from a dictionary using multiple possible keys."""
        if not data or not isinstance(data, dict):
            return default
            
        # Try all possible keys
        for key in possible_keys:
            if key in data and data[key] not in (None, "", "null", "undefined"):
                return data[key]
                
        return default
        
    def _format_date(self, date_str):
        """Format a date string to ISO format."""
        if not date_str:
            return None
            
        # Convert to string if needed
        date_str = str(date_str)
        
        # Try to parse the date
        try:
            # Handle common date formats
            formats = [
                '%Y-%m-%d',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%d-%m-%Y',
                '%m-%d-%Y',
                '%d %b %Y',
                '%b %d, %Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt).isoformat()
                except ValueError:
                    continue
                    
            # If all fails, return as is
            return date_str
        except Exception:
            return date_str
            
    def _extract_numeric(self, value):
        """Extract numeric value from string."""
        if not value:
            return None
            
        # Convert to string
        value = str(value)
        
        # Remove currency symbols and formatting
        for char in ['$', '€', '£', '¥', ',', ' ', 'USD', 'EUR', 'GBP']:
            value = value.replace(char, '')
            
        # Try to convert to float
        try:
            return float(value)
        except ValueError:
            return None

    def _extract_tender_data(self, content, source):
        """
        Attempt to extract meaningful tender data from a string or non-dict content.
        """
        try:
            # Print some debug info to see what we're working with
            content_preview = content[:100] if isinstance(content, str) else str(content)[:100]
            print(f"Extracting data from content type {type(content)}: {content_preview}...")
            
            # If content is a string, try to identify structured data
            if isinstance(content, str):
                # If it's a single character or very short, treat it as an ID and try to fetch the full tender
                if len(content.strip()) < 10 and content.strip().isalnum():
                    print(f"Content is very short, trying to use it as an ID: {content}")
                    try:
                        # Try to fetch from the source table using content as ID
                        response = self.supabase.table(source).select('*').eq('id', content).execute()
                        if response and response.data and len(response.data) > 0:
                            print(f"Found tender by ID {content}")
                            return {**response.data[0], 'source': source}
                    except Exception as e:
                        print(f"Failed to fetch tender by ID: {e}")
                
                # Try to identify XML
                if content.strip().startswith('<?xml') or content.strip().startswith('<'):
                    import xml.etree.ElementTree as ET
                    try:
                        root = ET.fromstring(content)
                        return self._xml_to_dict(root)
                    except Exception as xml_e:
                        print(f"Failed to parse XML: {xml_e}")
                
                # Try to identify HTML and extract content
                if content.strip().startswith('<html') or '<body' in content:
                    try:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        # Extract title
                        title = soup.title.string if soup.title else None
                        
                        # Extract main content
                        main_content = ''
                        for paragraph in soup.find_all('p'):
                            if paragraph.text and len(paragraph.text.strip()) > 10:
                                main_content += paragraph.text.strip() + "\n\n"
                        
                        # Extract all text if main content is empty
                        if not main_content:
                            main_content = soup.get_text()
                        
                        return {
                            'title': title or f"Tender from {source}",
                            'description': main_content[:2000],
                            'source': source,
                            'content': content,
                            'raw_data': content
                        }
                    except ImportError:
                        print("BeautifulSoup not installed, skipping HTML parsing")
                    except Exception as html_e:
                        print(f"Failed to parse HTML: {html_e}")
                
                # Try to identify JSON-like structure
                if '{' in content and '}' in content:
                    try:
                        # Find all JSON-like structures in the text
                        import re
                        json_matches = re.findall(r'\{[^{}]*\}', content)
                        for json_str in json_matches:
                            try:
                                parsed = json.loads(json_str)
                                if isinstance(parsed, dict) and len(parsed) > 2:
                                    parsed['source'] = source
                                    return parsed
                            except:
                                continue
                    except Exception as json_e:
                        print(f"Failed to extract JSON: {json_e}")
                
                # Try to identify key-value pairs in the text
                data = {}
                lines = content.split('\n')
                for line in lines:
                    if ':' in line:
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            key, value = parts
                            key = key.strip().lower().replace(' ', '_')
                            value = value.strip()
                            if value:
                                data[key] = value
                
                if len(data) > 2:
                    data['source'] = source
                    return data
            
            # If content is a list, try to convert it to a meaningful dictionary
            elif isinstance(content, list):
                if len(content) > 0:
                    if all(isinstance(item, dict) for item in content):
                        # Merge all dictionaries
                        data = {}
                        for item in content:
                            data.update(item)
                        data['source'] = source
                        return data
                    elif len(content) == 1:
                        # If it's a single item, use that
                        if isinstance(content[0], dict):
                            content[0]['source'] = source
                            return content[0]
                        else:
                            return {
                                'content': str(content[0]),
                                'source': source,
                            }
                    else:
                        # For multiple items
                        data = {}
                        for i, item in enumerate(content):
                            if isinstance(item, dict):
                                for k, v in item.items():
                                    data[k] = v
                            else:
                                data[f'item_{i}'] = str(item)
                        data['source'] = source
                        return data
            
            # For other types, try to extract meaningful data
            elif isinstance(content, dict):
                # Already a dictionary, just ensure source is set
                content['source'] = source
                return content
            
            # If all extraction attempts fail, create a basic wrapper
            print(f"Failed to extract structured data from content, creating basic wrapper")
            if isinstance(content, str):
                return {
                    'title': f"Tender from {source}",
                    'description': content[:2000] if len(content) > 0 else f"Tender from {source}",
                    'source': source,
                    'content': content,
                    'raw_data': content
                }
            else:
                return {
                    'title': f"Tender from {source}",
                    'description': str(content)[:2000] if str(content) else f"Tender from {source}",
                    'source': source,
                    'content': str(content),
                    'raw_data': str(content)
                }
        except Exception as e:
            print(f"Error extracting tender data: {e}")
            return {
                'title': f"Tender from {source}",
                'description': str(content)[:2000] if hasattr(content, '__str__') else f"Error extracting content from {source}",
                'source': source,
                'content': str(content) if hasattr(content, '__str__') else f"Error extracting content from {source}",
                'raw_data': str(content) if hasattr(content, '__str__') else f"Error extracting content from {source}"
            }

    def _xml_to_dict(self, element):
        """
        Convert XML element to dictionary.
        """
        result = {}
        
        # Add attributes
        for key, value in element.attrib.items():
            result[key] = value
        
        # Add child elements
        for child in element:
            if len(child) == 0:  # No children
                if child.text and child.text.strip():
                    result[child.tag] = child.text.strip()
            else:
                result[child.tag] = self._xml_to_dict(child)
        
        return result

    def _validate_normalized_tender(self, tender):
        """Validate a normalized tender to ensure required fields are present."""
        required_fields = ["notice_id", "notice_title", "source"]
        
        for field in required_fields:
            if field not in tender or tender[field] is None or tender[field] == "":
                if field == "notice_id":
                    tender[field] = str(uuid.uuid4())
                elif field == "notice_title":
                    tender[field] = f"Untitled Tender from {tender.get('source', 'Unknown')}"
                elif field == "source":
                    tender[field] = "Unknown"
        
        # Validate data types
        if "notice_title" in tender and not isinstance(tender["notice_title"], str):
            tender["notice_title"] = str(tender["notice_title"])
        
        if "description" in tender and not isinstance(tender["description"], str):
            tender["description"] = str(tender["description"])
        
        if "cpvs" in tender and not isinstance(tender["cpvs"], list):
            if isinstance(tender["cpvs"], str):
                tender["cpvs"] = [tender["cpvs"]]
            else:
                tender["cpvs"] = []
        
        if "tag" in tender and not isinstance(tender["tag"], list):
            if isinstance(tender["tag"], str):
                tender["tag"] = [tender["tag"]]
            else:
                tender["tag"] = []
        
        # Validate dates
        if "date_published" in tender and tender["date_published"]:
            if not self._is_valid_date_format(tender["date_published"]):
                tender["date_published"] = None
        
        if "closing_date" in tender and tender["closing_date"]:
            if not self._is_valid_date_format(tender["closing_date"]):
                tender["closing_date"] = None
        
        return tender

    def _create_exec_sql_function(self):
        """Attempt to create the exec_sql function if it doesn't exist."""
        try:
            # If we're in API-only mode, skip direct connection attempts
            if hasattr(self, 'skip_direct_connections') and self.skip_direct_connections:
                print("Skipping exec_sql function creation in API-only mode")
                return False
            
            # Check if exec_sql exists already
            function_exists = False
            try:
                # Try to call the function with a simple command
                self.supabase.rpc('exec_sql', {'sql': 'SELECT 1'}).execute()
                print("exec_sql function exists")
                function_exists = True
            except Exception as e:
                if "Could not find the function" in str(e) or "does not exist" in str(e):
                    print("exec_sql function does not exist, will continue without it")
                else:
                    print(f"Error checking exec_sql function: {e}")
            
            # Skip direct connection attempts in Docker/cloud environments
            print("Skipping direct PostgreSQL connection attempts - will use API-only mode")
            print("Will continue without exec_sql function, using API-based methods for operations")
            return function_exists
        except Exception as e:
            print(f"Error in _create_exec_sql_function: {e}")
            return False

    def _run_sql_directly(self, sql):
        """
        This is a stub that avoids direct PostgreSQL connections.
        In API-only mode, we don't attempt direct SQL execution.
        """
        print("Direct SQL execution disabled in API-only mode")
        print("Using Supabase REST API for all operations")
        raise NotImplementedError("Direct SQL execution is disabled in API-only mode")

    def _create_unified_tenders_table(self) -> None:
        """Create unified_tenders table if it doesn't exist with all required columns."""
        try:
            # Check if table already exists
            table_exists = False
            try:
                # Try direct query to see if table exists
                response = self.supabase.table('unified_tenders').select('id').limit(1).execute()
                if hasattr(response, 'data'):
                    table_exists = True
                    print("unified_tenders table already exists")
                    return
            except Exception as e:
                if "relation" in str(e) and "does not exist" in str(e):
                    print("unified_tenders table doesn't exist, but may be created by another process")
                else:
                    print(f"Error checking unified_tenders table: {e}")
            
            if table_exists:
                return
            
            # In API-only mode, we can't create tables directly
            print("Cannot create unified_tenders table in API-only mode")
            print("Please create the table using the Supabase UI or SQL Editor with this schema:")
            print("""
            CREATE TABLE IF NOT EXISTS unified_tenders (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                title TEXT,
                description TEXT,
                date_published TIMESTAMP,
                closing_date TIMESTAMP,
                tender_value NUMERIC,
                tender_currency TEXT,
                location TEXT,
                country TEXT, -- Added country field
                issuing_authority TEXT,
                keywords TEXT,
                tender_type TEXT,
                project_size TEXT,
                contact_information TEXT,
                source TEXT,
                raw_id TEXT,
                processed_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW(),
                metadata JSONB
            );
            
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            """)
            
            # Try inserting into the table anyway - it might exist but select was rejected due to permissions
            print("Will attempt to continue operations assuming the table exists")
        except Exception as e:
            print(f"Error in _create_unified_tenders_table: {e}")

    def _create_errors_table(self) -> None:
        """Create normalization_errors table if it doesn't exist."""
        try:
            # Check if table already exists
            table_exists = False
            try:
                # Try direct query to see if table exists  
                response = self.supabase.table('normalization_errors').select('id').limit(1).execute()
                if hasattr(response, 'data'):
                    table_exists = True
                    print("normalization_errors table already exists")
                    return
            except Exception as e:
                if "relation" in str(e) and "does not exist" in str(e):
                    print("normalization_errors table doesn't exist, but may be created by another process")
                else:
                    print(f"Error checking normalization_errors table: {e}")
            
            if table_exists:
                return
            
            # In API-only mode, we can't create tables directly
            print("Cannot create normalization_errors table in API-only mode")
            print("Please create the table using the Supabase UI or SQL Editor with this schema:")
            print("""
            CREATE TABLE IF NOT EXISTS public.normalization_errors (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source TEXT NOT NULL,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                tender_data TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            );
            """)
            
            # We'll continue without error tracking in this case
            print("Will continue without error tracking capabilities")
        except Exception as e:
            print(f"Error in _create_errors_table: {e}")

    def _insert_error(self, source: str, error_type: str, error_message: str, tender_data: str = "") -> None:
        """Log an error to the console."""
        try:
            # Truncate tender_data if it's too long
            if tender_data and len(tender_data) > 10000:
                tender_data = tender_data[:10000] + "... [truncated]"
            
            # Log to console
            print(f"ERROR RECORD [{source}] - Type: {error_type}")
            print(f"ERROR MESSAGE: {error_message[:200]}")
            if tender_data:
                print(f"ERROR DATA: {tender_data[:200]}...")
            
        except Exception as e:
            print(f"Error in _insert_error: {e}")
            # Log the original error to make sure it's visible
            print(f"Original error: [{source}] {error_type}: {error_message[:200]}")

    def _parse_date(self, date_str):
        """Parse a date string into ISO format (YYYY-MM-DD)."""
        if not date_str:
            return None
        
        if isinstance(date_str, (int, float)):
            # Unix timestamp
            import datetime
            try:
                return datetime.datetime.fromtimestamp(date_str).strftime('%Y-%m-%d')
            except:
                return None
        
        # If already ISO format, return as is
        if isinstance(date_str, str) and len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str
        
        # Try to parse with dateutil
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.strftime('%Y-%m-%d')
        except ImportError:
            print("dateutil not installed, using basic date parsing")
        except Exception as e:
            print(f"Error parsing date with dateutil: {e}")
        
        # Fallback to basic parsing
        try:
            # Try common formats
            import datetime
            
            # List of common date formats to try
            formats = [
                '%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y',
                '%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y',
                '%d.%m.%Y', '%m.%d.%Y', '%Y.%m.%d',
                '%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y',
                '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ',
                '%a, %d %b %Y %H:%M:%S %Z'
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except:
                    continue
            
            # If none of the formats worked, try to extract date with regex
            import re
            
            # Pattern for YYYY-MM-DD or similar
            iso_pattern = r'(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})'
            iso_match = re.search(iso_pattern, date_str)
            if iso_match:
                year, month, day = iso_match.groups()
                return f"{year}-{int(month):02d}-{int(day):02d}"
            
            # Pattern for DD-MM-YYYY or similar
            dmy_pattern = r'(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})'
            dmy_match = re.search(dmy_pattern, date_str)
            if dmy_match:
                day, month, year = dmy_match.groups()
                return f"{year}-{int(month):02d}-{int(day):02d}"
            
            # If all else fails, return None
            return None
        except Exception as e:
            print(f"Error in basic date parsing: {e}")
            return None

    def _is_valid_date_format(self, date_str):
        """Check if a date string is in valid ISO format."""
        if not date_str:
            return False
        
        # Check basic ISO format (YYYY-MM-DD)
        if isinstance(date_str, str) and len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            try:
                year, month, day = date_str.split('-')
                # Check valid ranges
                if 1900 <= int(year) <= 2100 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                    return True
            except:
                pass
        
        return False

    def _get_current_timestamp(self):
        """Get current timestamp in ISO format."""
        import datetime
        return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

    def _translate_text(self, text, max_chunk_size=4000):
        """Translate text from any language to English with chunking for long content"""
        if not text or not self.translation_available or not self.translator:
            return text
            
        try:
            # Handle empty or very short text
            if not text or len(text) < 5:
                return text
                
            # Check if translation is needed
            if not self._detect_non_english(text):
                return text
                
            # Use cache to avoid duplicate translations
            cache_key = f"txt:{hash(text[:100])}"
            if cache_key in self.translation_cache:
                return self.translation_cache[cache_key]
                
            # For long texts, break into chunks (API limits)
            if len(text) > max_chunk_size:
                # Split by paragraphs to maintain semantic units
                chunks = []
                paragraphs = re.split(r'(\r?\n\r?\n|\<\/p\>)', text)
                current_chunk = ""
                
                for para in paragraphs:
                    if len(current_chunk) + len(para) > (max_chunk_size * 0.9):  # 90% of limit
                        chunks.append(current_chunk)
                        current_chunk = para
                    else:
                        current_chunk += para
                        
                # Add the last chunk if not empty
                if current_chunk:
                    chunks.append(current_chunk)
                
                # Translate each chunk
                translated_chunks = []
                for chunk in chunks:
                    translated = self.translator.translate(chunk)
                    if translated:
                        translated_chunks.append(translated)
                    else:
                        translated_chunks.append(chunk)  # Fallback to original if translation fails
                
                result = "".join(translated_chunks)
            else:
                # Translate as a single piece
                result = self.translator.translate(text)
                
            # Store in cache if successful
            if result and result != text:
                self.translation_cache[cache_key] = result
                return result
            
            return text  # Return original if translation failed
        except Exception as e:
            print(f"Translation error: {str(e)[:100]}...")
            return text
            
    def _translate_html(self, html_content):
        """Extract and translate text from HTML content while preserving tags"""
        if not html_content or not self.translation_available:
            return html_content
            
        try:
            # Simple check if translation might be needed
            if not self._detect_non_english(html_content):
                return html_content
                
            # Cache check 
            cache_key = f"html:{hash(html_content[:100])}"
            if cache_key in self.translation_cache:
                return self.translation_cache[cache_key]
            
            # Simple HTML parser for paragraphs - will preserve most common tags
            parts = re.split(r'(<[^>]*>)', html_content)
            translated_parts = []
            
            for part in parts:
                if part.startswith('<') and part.endswith('>'):
                    # This is an HTML tag, don't translate
                    translated_parts.append(part)
                else:
                    # This is content, translate it
                    if part.strip():  # Only translate non-empty content
                        translated_parts.append(self._translate_text(part))
                    else:
                        translated_parts.append(part)
            
            result = ''.join(translated_parts)
            
            # Store in cache if successful
            if result and result != html_content:
                self.translation_cache[cache_key] = result
                
            return result
        except Exception as e:
            print(f"HTML translation error: {str(e)[:100]}...")
            return html_content
