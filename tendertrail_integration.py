import json
import sys
import subprocess
from typing import Dict, Any, List, Optional
from supabase import create_client, Client
import uuid
import traceback
import re

class TenderTrailIntegration:
    """Integration layer for TenderTrail normalization workflow."""
    
    def __init__(self, normalizer, preprocessor, supabase_url, supabase_key, skip_direct_connections=False):
        """
        Initialize the TenderTrail integration.
        
        Args:
            normalizer: The normalizer instance to use
            preprocessor: The preprocessor instance to use
            supabase_url: URL of the Supabase project
            supabase_key: API key for the Supabase project
            skip_direct_connections: Whether to skip direct PostgreSQL connections (default: False)
        """
        try:
            self.normalizer = normalizer
            self.preprocessor = preprocessor
            self.supabase_url = supabase_url
            self.supabase_key = supabase_key
            self.skip_direct_connections = skip_direct_connections
            
            # Initialize Supabase client
            try:
                self.supabase: Client = create_client(supabase_url, supabase_key)
                print("Successfully initialized Supabase client")
            except Exception as e:
                print(f"Error initializing Supabase client: {e}")
                raise ValueError(f"Failed to initialize Supabase client: {e}")
            
            # Initialize translation cache
            self.translation_cache = {}
            
            # Initialize schema cache
            self.target_schema = None
            
        except Exception as e:
            print(f"Error in __init__: {e}")
            traceback.print_exc()
            raise ValueError(f"Failed to initialize TenderTrailIntegration: {e}")
    
    def process_source(self, tenders_or_source, source_name_or_batch_size=None, create_tables=True):
        """
        Process tenders from a source, normalize and insert them into the database.
        This method is overloaded to handle two different call patterns:
        
        1. process_source(tenders, source_name, create_tables=True) - Process a list of tenders
        2. process_source(source_name, batch_size=100, create_tables=True) - Load tenders from database
        
        The method automatically detects which pattern is being used based on the types of arguments.
        """
        # Detect which call pattern is being used
        if isinstance(tenders_or_source, (list, tuple)) or (isinstance(tenders_or_source, str) and source_name_or_batch_size is not None):
            # First pattern: process_source(tenders, source_name)
            tenders = tenders_or_source
            source_name = source_name_or_batch_size
            batch_size = None
        else:
            # Second pattern: process_source(source_name, batch_size)
            source_name = tenders_or_source
            batch_size = source_name_or_batch_size or 100
            # Get tenders from database
            tenders = self._get_raw_tenders(source_name, batch_size)
        
        print(f"Processing {len(tenders) if isinstance(tenders, (list, tuple)) else 'unknown number of'} tenders from source: {source_name}")
        
        try:
            # Store the current source name for use in normalization
            self._current_source = source_name
            
            # Exit early if no tenders
            if not tenders:
                print(f"No tenders to process for source: {source_name}")
                return 0, 0
            
            # Track statistics
            processed_count = 0
            error_count = 0
            
            # Use the enhanced processing pipeline
            normalized_tenders = self._enhanced_process_raw_tenders(tenders, source_name)
            processed_count = len(normalized_tenders)

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
            # Store the source name for use in normalization
            self._current_source = source_name
            
            # First, try the source_tenders table format
            table_name = f"{source_name}_tenders"
            try:
                response = self.supabase.table(table_name).select('*').limit(batch_size).execute()
                if response and response.data:
                    print(f"Found {len(response.data)} tenders in {table_name}")
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
                print(f"Table {table_name} not found, trying direct source table {source_name}")
            
            # Try direct source name as table
            try:
                # Print a sample of the first tender to diagnose issues
                try:
                    sample_response = self.supabase.table(source_name).select('*').limit(1).execute()
                    if sample_response and sample_response.data:
                        print(f"Sample tender from {source_name}: {json.dumps(sample_response.data[0])[:500]}...")
                except Exception as sample_e:
                    print(f"Error getting sample tender: {sample_e}")
                
                # First try with processed field
                try:
                    response = self.supabase.table(source_name).select('*').eq('processed', False).limit(batch_size).execute()
                    # Mark processed records
                    if response and response.data:
                        print(f"Found {len(response.data)} unprocessed tenders in {source_name}")
                        ids = []
                        processed = []
                        for item in response.data:
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
                    print(f"Error querying with processed field, trying without: {e}")
                    # Try without processed field
                    response = self.supabase.table(source_name).select('*').limit(batch_size).execute()
                    if response and response.data:
                        # Process tenders and ensure source name is preserved
                        processed = []
                        for item in response.data:
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
                print(f"Error querying table {source_name}: {e}")
            
            # No valid data found
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
    
    def _insert_normalized_tenders(self, normalized_tenders: List[Dict[str, Any]], create_tables=True) -> int:
        """Insert normalized tenders into unified table and return count of successful insertions."""
        if not normalized_tenders:
            print("No tenders to insert")
            return 0
        
        try:
            print(f"Preparing to insert {len(normalized_tenders)} tenders")
            
            # Create a copy to avoid modifying the original
            tenders_to_insert = []
            inserted_count = 0
            
            # Field mapping between normalized tender fields and database fields
            field_mapping = {
                "notice_title": "title",
                "notice_type": "tender_type",
                "issuing_authority": "issuing_authority",
                "date_published": "date_published",
                "closing_date": "closing_date",
                "description": "description",
                "location": "location",
                "country": "location",  # Use country as fallback for location
                "source": "source",
                "value": "tender_value",
                "currency": "tender_currency",
                "email": "contact_information",
                "cpvs": "keywords",  # Store CPVs as keywords
                "url": "url",  # Add URL field if exists in db
                "buyer": "buyer",  # Add buyer field if exists in db
                "raw_id": "raw_id",
                "notice_id": "raw_id"  # Use notice_id as fallback for raw_id
            }
            
            # Try to load deep-translator if available
            translator = None
            try:
                from deep_translator import GoogleTranslator
                translator = GoogleTranslator(source='auto', target='en')
                print("Translation capability is available")
            except ImportError:
                print("deep-translator not found, no translation will be performed")
                print("Consider installing with: pip install deep-translator")
            except Exception as e:
                print(f"Error initializing translator: {e}")
            
            # First check if metadata column exists in the table
            metadata_column_exists = False
            try:
                # Try a simple query to check if metadata column exists
                self.supabase.table('unified_tenders').select('metadata').limit(1).execute()
                metadata_column_exists = True
                print("Metadata column exists in unified_tenders table")
            except Exception as e:
                if "metadata" in str(e) and "does not exist" in str(e):
                    print("Metadata column does not exist in unified_tenders table")
                    print("Will skip metadata field in inserts")
                else:
                    print(f"Error checking metadata column: {e}")
            
            # Ensure all fields are properly formatted
            for tender in normalized_tenders:
                try:
                    cleaned_tender = {}
                    metadata = {}
                    
                    # Extract metadata if present
                    if "metadata" in tender and tender["metadata"]:
                        try:
                            if isinstance(tender["metadata"], str):
                                metadata = json.loads(tender["metadata"])
                            elif isinstance(tender["metadata"], dict):
                                metadata = tender["metadata"]
                        except Exception as md_e:
                            print(f"Error parsing metadata: {md_e}")
                    
                    # Map fields using the field mapping
                    for norm_field, db_field in field_mapping.items():
                        if norm_field in tender and tender[norm_field] is not None and tender[norm_field] != "":
                            # For text fields, handle translation if needed
                            if db_field in ["title", "description"] and translator and isinstance(tender[norm_field], str):
                                # Check if translation is needed (non-English text)
                                text = tender[norm_field]
                                needs_translation = False
                                
                                # Check for non-ASCII characters that might indicate non-English text
                                if any(ord(c) > 127 for c in text):
                                    needs_translation = True
                                
                                # Common non-English indicators
                                non_english_indicators = ['à', 'á', 'â', 'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 
                                                         'ì', 'í', 'î', 'ï', 'ñ', 'ò', 'ó', 'ô', 'õ', 'ö', 'ø', 'ù', 
                                                         'ú', 'û', 'ü', 'ý', 'ÿ']
                                
                                if any(indicator in text.lower() for indicator in non_english_indicators):
                                    needs_translation = True
                                
                                # Try to translate if needed
                                if needs_translation:
                                    try:
                                        translated_text = translator.translate(text[:5000])  # Limit length for API
                                        if translated_text and len(translated_text) > 10:  # Sanity check on result
                                            # Store original in metadata
                                            metadata[f"original_{norm_field}"] = text
                                            # Use translated text
                                            cleaned_tender[db_field] = translated_text[:2000]  # Truncate if needed
                                            continue
                                    except Exception as trans_e:
                                        print(f"Translation error: {trans_e}")
                            
                            # Special handling for date fields
                            if db_field in ["date_published", "closing_date"]:
                                # Parse date if it's a string
                                if isinstance(tender[norm_field], str):
                                    parsed_date = self._parse_date(tender[norm_field])
                                    if parsed_date:
                                        # Sanity check on year - if after 2030 or before 2000, probably incorrect
                                        try:
                                            year = int(parsed_date.split('-')[0])
                                            if year > 2030:
                                                # Adjust year to current year
                                                import datetime
                                                current_year = datetime.datetime.now().year
                                                parts = parsed_date.split('-')
                                                parsed_date = f"{current_year}-{parts[1]}-{parts[2]}"
                                        except:
                                            pass
                                        
                                        cleaned_tender[db_field] = parsed_date
                                    else:
                                        cleaned_tender[db_field] = None
                                else:
                                    cleaned_tender[db_field] = tender[norm_field]
                                
                            # Special handling for value field
                            elif db_field == "tender_value" and tender[norm_field] is not None:
                                try:
                                    # Try to convert to float
                                    if isinstance(tender[norm_field], (int, float)):
                                        cleaned_tender[db_field] = str(tender[norm_field])
                                    elif isinstance(tender[norm_field], str):
                                        # Remove any non-numeric characters except decimal point
                                        import re
                                        numeric_str = re.sub(r'[^\d.]', '', tender[norm_field])
                                        if numeric_str:
                                            cleaned_tender[db_field] = str(float(numeric_str))
                                except Exception as val_e:
                                    print(f"Error parsing value: {val_e}")
                                    
                            # Special handling for CPV codes
                            elif norm_field == "cpvs" and db_field == "keywords":
                                if isinstance(tender[norm_field], list):
                                    cleaned_tender[db_field] = ",".join([str(cpv) for cpv in tender[norm_field]])
                                elif isinstance(tender[norm_field], str):
                                    cleaned_tender[db_field] = tender[norm_field]
                                    
                            # For other text fields, just truncate if needed
                            elif isinstance(tender[norm_field], str):
                                cleaned_tender[db_field] = tender[norm_field][:2000]  # Truncate long values
                            elif isinstance(tender[norm_field], (dict, list)):
                                # Convert complex objects to JSON string
                                cleaned_tender[db_field] = json.dumps(tender[norm_field])[:2000]
                            else:
                                cleaned_tender[db_field] = str(tender[norm_field])[:2000]
                    
                    # Ensure required fields exist with defaults
                    if "title" not in cleaned_tender or not cleaned_tender["title"]:
                        cleaned_tender["title"] = f"Untitled Tender from {tender.get('source', 'Unknown')}"
                        
                    if "source" not in cleaned_tender or not cleaned_tender["source"]:
                        cleaned_tender["source"] = "Unknown"
                        
                    if "description" not in cleaned_tender or not cleaned_tender["description"]:
                        # Try to create a description from other fields
                        description_parts = []
                        for field in ["notice_type", "location", "country", "issuing_authority"]:
                            if field in tender and tender[field]:
                                description_parts.append(f"{field.replace('_', ' ').title()}: {tender[field]}")
                        
                        if description_parts:
                            cleaned_tender["description"] = " | ".join(description_parts)
                        else:
                            cleaned_tender["description"] = "No detailed content available"
                    
                    # Add processed_at if not present
                    if "processed_at" not in cleaned_tender:
                        cleaned_tender["processed_at"] = self._get_current_timestamp()
                    
                    # Add metadata column if it exists in the database and we have metadata
                    if metadata_column_exists and metadata:
                        cleaned_tender["metadata"] = json.dumps(metadata)
                    
                    tenders_to_insert.append(cleaned_tender)
                except Exception as tender_e:
                    print(f"Error preparing tender for insertion: {tender_e}")
                    continue
            
            # Insert in batches to avoid timeout issues
            batch_size = 10  # Smaller batch size for better error handling
            for i in range(0, len(tenders_to_insert), batch_size):
                batch = tenders_to_insert[i:i + batch_size]
                try:
                    print(f"Inserting batch {i//batch_size + 1}/{(len(tenders_to_insert) + batch_size - 1)//batch_size}")
                    response = self.supabase.table('unified_tenders').insert(batch).execute()
                    if hasattr(response, 'data'):
                        print(f"Successfully inserted batch with {len(batch)} tenders")
                        inserted_count += len(batch)
                    else:
                        print(f"Batch insert completed but no data returned")
                        inserted_count += len(batch)  # Assume success
                except Exception as batch_error:
                    print(f"Error inserting batch: {batch_error}")
                    
                    # Check for common errors
                    error_str = str(batch_error)
                    if "timeout" in error_str.lower():
                        print("Connection timeout error - consider using smaller batch sizes")
                    elif "duplicate key" in error_str.lower():
                        print("Duplicate tender entries detected - some tenders already exist in the database")
                    
                    # Try to insert one by one with even smaller batches
                    print(f"Attempting one-by-one insertion for batch {i//batch_size + 1}")
                    for j in range(0, len(batch), 3):  # Try with micro-batches of 3
                        micro_batch = batch[j:j+3]
                        try:
                            response = self.supabase.table('unified_tenders').insert(micro_batch).execute()
                            if hasattr(response, 'data'):
                                inserted_count += len(micro_batch)
                                print(f"Inserted micro-batch of {len(micro_batch)} tenders")
                        except Exception as micro_error:
                            # Final fallback - truly one by one
                            for tender in micro_batch:
                                try:
                                    response = self.supabase.table('unified_tenders').insert(tender).execute()
                                    if hasattr(response, 'data'):
                                        inserted_count += 1
                                        print(f"Inserted single tender")
                                except Exception as single_error:
                                    print(f"Error inserting single tender: {single_error}")
            
            print(f"Successfully inserted {inserted_count} out of {len(normalized_tenders)} tenders")
            return inserted_count
        except Exception as e:
            print(f"Error inserting normalized tenders: {e}")
            if "timeout" in str(e).lower():
                print("Connection timeout - network conditions may be affecting database access")
            print("Data sample that failed:", normalized_tenders[0] if normalized_tenders else "No data")
            return 0
    
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

    def _normalize_tender(self, tender, source_name=None):
        """Normalize a tender to the target schema."""
        if not tender:
            return None
            
        try:
            # If source_name is not provided, use the current source
            if not source_name and hasattr(self, '_current_source'):
                source_name = self._current_source
                
            # Initialize normalized tender
            normalized = {}
            enriched_metadata = {}

            # Basic fields
            normalized["notice_id"] = self._extract_tender_id(tender, uuid.uuid4())
            normalized["source"] = source_name or "unknown"
            
            # Try to extract content from known source formats or fall back to generic extraction
            if source_name == "sam_gov":
                # SAM.gov specific extraction
                normalized["notice_title"] = self._extract_field(tender, ["subject", "title", "solicitationTitle"]) or "Untitled SAM.gov Tender"
                normalized["description"] = self._clean_html(self._extract_field(tender, ["description", "body", "generalDescription"]) or "")
                normalized["notice_type"] = self._extract_field(tender, ["type", "noticeType", "solicitationType"]) or "Default"
                normalized["issuing_authority"] = self._extract_field(tender, ["office", "contractingOffice", "department", "agency"]) or "Unknown"
                normalized["date_published"] = self._format_date(self._extract_field(tender, ["postedDate", "publishDate", "date", "datePosted"]))
                normalized["closing_date"] = self._format_date(self._extract_field(tender, ["responseDeadLine", "closeDate", "deadlineDate", "responseDate"]))
                
                # Extract place of performance for sam.gov
                place_of_performance = self._extract_field(tender, ["placeOfPerformance", "pop", "performanceLocation"])
                if place_of_performance:
                    if isinstance(place_of_performance, dict):
                        # Try to get structured country info
                        country_code = place_of_performance.get("countryCode", "")
                        if country_code:
                            normalized["country"] = self._standardize_country(country_code)
                        
                        # Get state and city
                        state = place_of_performance.get("state", "")
                        city = place_of_performance.get("city", "")
                        if state or city:
                            location_parts = [part for part in [city, state] if part]
                            normalized["location"] = ", ".join(location_parts)
                            
                        # Save complete place of performance to metadata
                        enriched_metadata["place_of_performance"] = place_of_performance
                    elif isinstance(place_of_performance, str):
                        normalized["location"] = place_of_performance
                
                # Additional metadata fields
                enriched_metadata["naics_code"] = self._extract_field(tender, ["naicsCode", "naics"])
                enriched_metadata["set_aside"] = self._extract_field(tender, ["setAside", "setAsideCode", "setAsideType"])
                enriched_metadata["solicitation_number"] = self._extract_field(tender, ["solicitationNumber", "noticeId", "referenceNumber"])
                
                # Extract contact information
                contact_info = self._extract_field(tender, ["pointOfContact", "contact", "contactInfo"])
                if contact_info:
                    if isinstance(contact_info, dict):
                        normalized["contact_email"] = contact_info.get("email")
                        normalized["contact_phone"] = contact_info.get("phone")
                        # Save full contact to metadata
                        enriched_metadata["contact_info"] = contact_info
                    elif isinstance(contact_info, str):
                        normalized["contact_information"] = contact_info
                
            elif source_name == "ted_eu":
                # European TED specific extraction
                normalized["notice_title"] = self._extract_field(tender, ["title", "contractTitle", "name"]) or "Untitled TED Tender"
                normalized["description"] = self._clean_html(self._extract_field(tender, ["description", "shortDescription", "contractDescription"]) or "")
                normalized["notice_type"] = self._extract_field(tender, ["procedureType", "contractType", "type"]) or "Default"
                normalized["issuing_authority"] = self._extract_field(tender, ["contractingAuthority", "authority", "organization"]) or "Unknown"
                normalized["date_published"] = self._format_date(self._extract_field(tender, ["publicationDate", "publishDate", "datePublished"]))
                normalized["closing_date"] = self._format_date(self._extract_field(tender, ["submissionDeadline", "deadline", "closingDate"]))
                normalized["country"] = self._standardize_country(self._extract_field(tender, ["country", "countryCode", "location"]) or "")
                normalized["location"] = self._extract_field(tender, ["addressTown", "city", "place"]) or ""
                
                # Extract additional TED specific information
                enriched_metadata["cpv_codes"] = self._extract_field(tender, ["cpvCodes", "cpvs", "commonProcurementVocabulary"])
                enriched_metadata["nuts_code"] = self._extract_field(tender, ["nutsCode", "nuts"])
                enriched_metadata["document_number"] = self._extract_field(tender, ["documentNumber", "tedDocumentNumber", "documentId"])
                
                # Extract value information
                value_data = self._extract_field(tender, ["estimatedValue", "contractValue", "value"])
                if value_data:
                    if isinstance(value_data, dict):
                        normalized["tender_value"] = self._extract_numeric(value_data.get("amount", ""))
                        normalized["currency"] = value_data.get("currency", "")
                    elif isinstance(value_data, str):
                        normalized["tender_value"] = self._extract_numeric(value_data)
                        normalized["currency"] = self._extract_currency(value_data)
                    
            elif source_name == "ungm":
                # UNGM specific extraction
                normalized["notice_title"] = self._extract_field(tender, ["title", "tenderTitle", "subject"]) or "Untitled UNGM Tender"
                normalized["description"] = self._clean_html(self._extract_field(tender, ["description", "summary", "details"]) or "")
                normalized["notice_type"] = self._extract_field(tender, ["type", "category", "tenderType"]) or "Default"
                normalized["issuing_authority"] = self._extract_field(tender, ["organization", "agency", "orgName"]) or "Unknown"
                normalized["date_published"] = self._format_date(self._extract_field(tender, ["publishedDate", "postDate", "publicationDate"]))
                normalized["closing_date"] = self._format_date(self._extract_field(tender, ["deadlineDate", "closingDate", "endDate"]))
                normalized["country"] = self._standardize_country(self._extract_field(tender, ["country", "deliveryCountry", "location"]) or "")
                normalized["location"] = self._extract_field(tender, ["city", "deliveryLocation", "place"]) or ""
                
                # UNGM specific metadata
                enriched_metadata["reference"] = self._extract_field(tender, ["reference", "noticeNumber", "refNo"])
                enriched_metadata["un_organization"] = self._extract_field(tender, ["unOrganization", "unAgency", "unEntity"])
                enriched_metadata["deadline_timezone"] = self._extract_field(tender, ["timezone", "deadlineTimezone"]) 
            
            elif source_name == "wb" or source_name == "worldbank":
                # World Bank specific extraction
                normalized["notice_title"] = self._extract_field(tender, ["title", "projectTitle", "name"]) or "Untitled World Bank Tender"
                normalized["description"] = self._clean_html(self._extract_field(tender, ["description", "projectDescription", "summary"]) or "")
                normalized["notice_type"] = self._extract_field(tender, ["procurementType", "contractType", "type"]) or "Default"
                normalized["issuing_authority"] = self._extract_field(tender, ["borrower", "client", "agency"]) or "Unknown"
                normalized["date_published"] = self._format_date(self._extract_field(tender, ["publishedDate", "noticeDate", "publicationDate"]))
                normalized["closing_date"] = self._format_date(self._extract_field(tender, ["bidDeadline", "closingDate", "dueDate"]))
                normalized["country"] = self._standardize_country(self._extract_field(tender, ["country", "region", "location"]) or "")
                normalized["location"] = self._extract_field(tender, ["city", "implementingAgency", "place"]) or ""
                
                # World Bank specific metadata
                enriched_metadata["project_id"] = self._extract_field(tender, ["projectId", "projectNumber", "projId"])
                enriched_metadata["financing"] = self._extract_field(tender, ["financing", "loanNo", "fundingSource"])
                enriched_metadata["wb_region"] = self._extract_field(tender, ["region", "regionalDepartment"])
                
                # Extract value information if available
                value_data = self._extract_field(tender, ["loanAmount", "projectCost", "contractValue"])
                if value_data:
                    if isinstance(value_data, dict):
                        normalized["tender_value"] = self._extract_numeric(value_data.get("amount", ""))
                        normalized["currency"] = value_data.get("currency", "")
                    elif isinstance(value_data, str):
                        normalized["tender_value"] = self._extract_numeric(value_data)
                        normalized["currency"] = self._extract_currency(value_data)
            
            # Generic extraction (fallback for all sources)
            if not normalized.get("notice_title"):
                # Try common title fields
                normalized["notice_title"] = self._extract_field(tender, ["title", "name", "subject", "summary", "projectTitle", "tenderTitle", "contract_title", "tender_name"]) or f"Untitled Tender from {source_name}"
                
            if not normalized.get("description"):
                # Try common description fields
                description = self._extract_field(tender, ["description", "details", "body", "content", "text", "summary", "abstract", "project_description", "tender_description", "scope"])
                normalized["description"] = self._clean_html(description or "")
                
            if not normalized.get("notice_type"):
                # Try common type fields
                normalized["notice_type"] = self._extract_field(tender, ["type", "category", "procurement_type", "tender_type", "contractType", "procedure_type", "noticeType"]) or "Default"
                
            if not normalized.get("issuing_authority"):
                # Try common authority fields
                normalized["issuing_authority"] = self._extract_field(tender, ["issuing_authority", "authority", "agency", "organization", "buyer", "department", "office", "contracting_authority", "client"]) or "Unknown"
                
            if not normalized.get("date_published"):
                # Try common published date fields
                published_date = self._extract_field(tender, ["date_published", "published_date", "publication_date", "post_date", "issue_date", "date", "publicationDate", "postDate"])
                normalized["date_published"] = self._format_date(published_date)
                
            if not normalized.get("closing_date"):
                # Try common closing date fields
                closing_date = self._extract_field(tender, ["closing_date", "deadline", "due_date", "end_date", "closing", "submission_deadline", "bidDeadline", "response_date", "expiry_date", "closingDate", "deadlineDate"])
                normalized["closing_date"] = self._format_date(closing_date)
                
                # If no closing date is found, estimate one based on publication date
                if not normalized.get("closing_date") and normalized.get("date_published"):
                    normalized["closing_date"] = self._extract_deadline_date(tender)
                
            if not normalized.get("country"):
                # Try common country fields
                country = self._extract_field(tender, ["country", "country_code", "nation", "region", "location", "place_of_performance", "delivery_country"])
                normalized["country"] = self._standardize_country(country or "")
                
            if not normalized.get("location"):
                # Try common location fields
                normalized["location"] = self._extract_field(tender, ["location", "city", "place", "region", "district", "state", "province", "town", "address_town"]) or ""
            
            # Try to extract value and currency from various fields if not already set
            if not normalized.get("tender_value"):
                value = self._extract_field(tender, ["tender_value", "value", "price", "amount", "budget", "estimated_value", "contract_value", "bid_value", "total_value", "estimatedValue", "totalValue"])
                if value:
                    normalized["tender_value"] = self._extract_numeric(value)
                    if not normalized.get("currency"):
                        normalized["currency"] = self._extract_currency(value)
                else:
                    # Try to find value in description
                    if normalized.get("description"):
                        normalized["tender_value"] = self._extract_numeric(normalized["description"])
                        if not normalized.get("currency") and normalized.get("tender_value"):
                            normalized["currency"] = self._extract_currency(normalized["description"])
            
            # If currency is not found, try to extract it from various fields
            if not normalized.get("currency"):
                currency = self._extract_field(tender, ["currency", "tender_currency", "currency_code", "value_currency"])
                if currency:
                    normalized["currency"] = currency
            
            # Extract CPVs (Common Procurement Vocabulary) codes
            cpvs = self._extract_field(tender, ["cpvs", "cpv_codes", "cpv", "common_procurement_vocabulary", "codes"])
            if cpvs:
                if isinstance(cpvs, list):
                    normalized["cpvs"] = cpvs
                elif isinstance(cpvs, str):
                    normalized["cpvs"] = [cpvs]
            
            # Extract categories/tags
            categories = self._extract_categories(normalized.get("description", ""), source_name)
            if categories:
                normalized["tag"] = categories
            
            # Add the URL if available
            url = self._extract_field(tender, ["url", "link", "tender_url", "notice_url", "document_url", "external_url", "web_link"])
            if url:
                normalized["url"] = url
            
            # Add the buyer info if available
            buyer = self._extract_field(tender, ["buyer", "buyer_name", "purchaser", "contracting_authority", "contracting_entity", "client"])
            if buyer:
                normalized["buyer"] = buyer
                
            # Add contact information if available 
            contact_email = self._extract_field(tender, ["contact_email", "email", "enquiry_email", "contact_mail", "inquiry_email"])
            if contact_email:
                normalized["contact_email"] = contact_email
                
            contact_phone = self._extract_field(tender, ["contact_phone", "phone", "telephone", "tel", "contact_number", "inquiry_phone"])
            if contact_phone:
                normalized["contact_phone"] = contact_phone
            
            # Create structured metadata
            metadata = {}
            
            # Add source data metadata
            raw_id = self._extract_field(tender, ["id", "tender_id", "notice_id", "reference", "ref_no", "reference_number"])
            if raw_id:
                normalized["raw_id"] = str(raw_id)
                metadata["original_id"] = raw_id
            
            # Add source metadata
            source_info = {}
            
            if source_name == "sam_gov":
                source_info["type"] = "US Federal Government"
                if not normalized.get("country"):
                    normalized["country"] = "United States"
            elif source_name == "ted_eu":
                source_info["type"] = "European Union"
            elif source_name == "ungm":
                source_info["type"] = "United Nations"
            elif source_name == "wb" or source_name == "worldbank":
                source_info["type"] = "World Bank"
            elif source_name == "adb":
                source_info["type"] = "Asian Development Bank"
            elif source_name == "afdb":
                source_info["type"] = "African Development Bank"
            elif source_name == "iadb":
                source_info["type"] = "Inter-American Development Bank"
                
            if source_info:
                metadata["source_info"] = source_info
            
            # Add all extra found metadata fields
            if enriched_metadata:
                metadata.update(enriched_metadata)
            
            # Add processing metadata
            metadata["processed_at"] = self._get_current_timestamp()
            metadata["version"] = "2.0"
            
            # Store the original tender in metadata if it's a simple structure
            try:
                if isinstance(tender, dict) and len(str(tender)) < 10000:
                    metadata["original_data"] = tender
            except:
                pass
            
            normalized["metadata"] = metadata
            
            return normalized
        
        except Exception as e:
            error_message = f"Error normalizing tender: {e}"
            print(error_message)
            if source_name:
                self._insert_error(str(source_name), "normalization_error", error_message, str(tender) if tender else "")
            return None

    def _extract_field(self, data: Dict[str, Any], field_names: List[str]) -> Any:
        """
        Extract a field from a dictionary by trying different possible field names.
        
        Args:
            data: The dictionary to extract the field from
            field_names: List of possible field names to try
            
        Returns:
            The value of the field if found, None otherwise
        """
        if not isinstance(data, dict):
            return None
            
        # Try each field name
        for field in field_names:
            if field in data and data[field] is not None:
                return data[field]
                
        # Check for nested dictionaries using dot notation (e.g., "metadata.field")
        for field in field_names:
            if "." in field:
                parts = field.split(".")
                current = data
                found = True
                
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        found = False
                        break
                        
                if found:
                    return current
                    
        # Try case-insensitive matching
        lower_data = {k.lower(): v for k, v in data.items()}
        for field in field_names:
            if field.lower() in lower_data:
                return lower_data[field.lower()]
                
        return None
    
    def _clean_html(self, html_content: str) -> str:
        """
        Clean HTML content by removing tags and entities.
        
        Args:
            html_content: HTML content to clean
            
        Returns:
            Cleaned text
        """
        if not html_content:
            return ""
            
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # Replace HTML entities
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")
        
        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)
        
        # Trim whitespace
        text = text.strip()
        
        return text
    
    def _format_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Format a date string to ISO format.
        
        Args:
            date_str: Date string to format
            
        Returns:
            Formatted date string or None if invalid
        """
        if not date_str:
            return None
            
        try:
            # Try to parse with dateutil
            from dateutil import parser
            date_obj = parser.parse(date_str)
            return date_obj.strftime('%Y-%m-%d')
        except:
            try:
                # Try a manual approach for common formats
                
                # Remove time part if present
                if ' ' in date_str:
                    date_part = date_str.split(' ')[0]
                elif 'T' in date_str:
                    date_part = date_str.split('T')[0]
                else:
                    date_part = date_str
                
                # Check if it's already in ISO format
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
                    return date_part
                
                # Check MM/DD/YYYY
                if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_part):
                    parts = date_part.split('/')
                    return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                
                # Check DD/MM/YYYY
                if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_part):
                    parts = date_part.split('/')
                    return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                
                # Return as is if we can't parse it
                return date_str
            except:
                return date_str
    
    def _standardize_country(self, country_str: Optional[str]) -> str:
        """
        Standardize country names and codes.
        
        Args:
            country_str: Country name or code
            
        Returns:
            Standardized country name
        """
        if not country_str:
            return ""
            
        # Convert to string and lowercase for comparison
        country = str(country_str).strip().lower()
        
        # Country code mappings
        country_map = {
            # ISO codes to country names
            'us': 'United States',
            'usa': 'United States',
            'united states of america': 'United States',
            'uk': 'United Kingdom',
            'gb': 'United Kingdom',
            'great britain': 'United Kingdom',
            'ca': 'Canada',
            'au': 'Australia',
            'de': 'Germany',
            'fr': 'France',
            'jp': 'Japan',
            'it': 'Italy',
            'cn': 'China',
            'in': 'India',
            'br': 'Brazil',
            'ru': 'Russia',
            'za': 'South Africa',
            'sg': 'Singapore',
            'ae': 'United Arab Emirates',
            'uae': 'United Arab Emirates',
            'kr': 'South Korea',
            'south korea': 'South Korea',
            'republic of korea': 'South Korea',
            
            # Common name variations
            'united states': 'United States',
            'united kingdom': 'United Kingdom',
            'england': 'United Kingdom',
            'u.s.': 'United States',
            'u.s.a.': 'United States',
            'u.k.': 'United Kingdom',
            'deutschland': 'Germany',
            'italia': 'Italy',
            'brasil': 'Brazil',
        }
        
        # Return the standardized country name if found
        return country_map.get(country, country_str)
    
    def _extract_numeric(self, value: Any) -> Optional[float]:
        """
        Extract a numeric value from a string or dictionary.
        
        Args:
            value: Value to extract numeric from
            
        Returns:
            Numeric value or None if extraction fails
        """
        if value is None:
            return None
            
        # Already a number
        if isinstance(value, (int, float)):
            return float(value)
            
        # Handle dictionary input
        if isinstance(value, dict):
            # Try common keys for amount
            for key in ["amount", "value", "price", "cost", "estimated"]:
                if key in value and value[key]:
                    try:
                        # Try to convert to float
                        return float(value[key])
                    except:
                        # If not a direct number, try to extract from string
                        if isinstance(value[key], str):
                            return self._extract_numeric(value[key])
            return None
            
        # Handle string input
        if isinstance(value, str):
            # Remove currency symbols and other non-numeric characters
            numeric_str = re.sub(r'[^\d.]', '', value.replace(',', '.'))
            
            # Try to extract the first number
            numbers = re.findall(r'\d+(?:\.\d+)?', numeric_str)
            if numbers:
                try:
                    return float(numbers[0])
                except:
                    return None
                    
        return None
    
    def _extract_tender_data(self, content, source):
        """
        Extract tender data from the content.
        
        Args:
            content: Content containing tender data
            source: Name of the source
            
        Returns:
            List of tender dictionaries
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

    def _detect_potential_duplicate(self, tender, normalized_tenders):
        """Check if a tender might be a duplicate of already processed tenders."""
        if not tender or not normalized_tenders:
            return False
            
        # Extract key fields for comparison
        title = tender.get("notice_title", "").lower()
        ref_num = tender.get("notice_id", "").lower()
        
        # Skip short titles or reference numbers that are too generic
        if len(title) < 10 or len(ref_num) < 3:
            return False
            
        # Check for exact match by reference number
        for existing in normalized_tenders:
            existing_ref = existing.get("notice_id", "").lower()
            if existing_ref and existing_ref == ref_num:
                print(f"Potential duplicate detected by reference: {ref_num}")
                return True
                
        # Check for high similarity in titles
        for existing in normalized_tenders:
            existing_title = existing.get("notice_title", "").lower()
            
            # Skip comparison with short titles
            if len(existing_title) < 10:
                continue
                
            # Very simple similarity check - more sophisticated would use edit distance
            # But this is efficient for a quick check
            
            # 1. Check for exact title match
            if title == existing_title:
                print(f"Potential duplicate detected by exact title: {title[:30]}...")
                return True
                
            # 2. Check if title is substring of existing or vice versa
            if title in existing_title or existing_title in title:
                print(f"Potential duplicate detected by title substring: {title[:30]}...")
                return True
                
            # 3. Check for word overlap (if enough words match)
            title_words = set(title.split())
            existing_words = set(existing_title.split())
            
            if len(title_words) > 3 and len(existing_words) > 3:
                common_words = title_words.intersection(existing_words)
                
                # If more than 70% words match, likely a duplicate
                if len(common_words) / min(len(title_words), len(existing_words)) > 0.7:
                    print(f"Potential duplicate detected by word similarity: {title[:30]}...")
                    return True
                    
        return False

    def _validate_normalized_tender(self, tender):
        """Validate a normalized tender to ensure required fields are present."""
        # Return None if tender is invalid
        if not tender:
            return None
            
        # Required fields from previous version
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
        
        # Enhanced validation for data quality
        
        # Ensure description is clean and not just an error message
        description = tender.get("description", "")
        if description:
            # Check if description is too short to be useful
            if len(description.strip()) < 20:
                print(f"Warning: Description too short ({len(description)} chars), might not be useful")
                
            # Check if description is likely an error message
            error_indicators = ["error", "not found", "404", "403", "access denied", 
                               "forbidden", "unavailable", "sorry", "cannot access"]
            
            if any(indicator in description.lower() for indicator in error_indicators) and len(description) < 200:
                print(f"Warning: Description appears to be an error message: {description[:100]}...")
                # Don't reject, but mark in metadata
                if not isinstance(tender.get("metadata"), dict):
                    tender["metadata"] = {}
                tender["metadata"]["possible_error"] = True
                
        # Ensure country is properly formatted 
        if tender.get("country") and isinstance(tender["country"], str):
            tender["country"] = self._standardize_country(tender["country"])
            
        # Validate and clean URLs
        if tender.get("url") and isinstance(tender["url"], str):
            url = tender["url"].strip()
            
            # Add http:// prefix if missing
            if not (url.startswith("http://") or url.startswith("https://")):
                url = "http://" + url
                
            # Simple URL validation
            if re.match(r'^https?://[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+(:[0-9]+)?(/.*)?$', url):
                tender["url"] = url
            else:
                # URL doesn't look valid, save to metadata
                if not isinstance(tender.get("metadata"), dict):
                    tender["metadata"] = {}
                tender["metadata"]["original_url"] = url
                tender["url"] = None
                
        # Fix lists that should be strings
        for field in ["contact_email", "contact_phone", "issuing_authority"]:
            if isinstance(tender.get(field), list):
                if tender[field]:
                    # Join list items with commas
                    tender[field] = ", ".join(str(item) for item in tender[field] if item)
                else:
                    tender[field] = None
                    
        # Make sure currency is standardized
        if tender.get("currency") and isinstance(tender["currency"], str):
            # Normalize currency code
            curr = tender["currency"].strip().upper()
            # Map common variants
            currency_map = {
                "US$": "USD", "USD$": "USD", "$": "USD", "DOLLAR": "USD", "DOLLARS": "USD",
                "€": "EUR", "EURO": "EUR", "EUROS": "EUR",
                "£": "GBP", "UKP": "GBP", "POUND": "GBP", "POUNDS": "GBP",
                "¥": "JPY", "YEN": "JPY",
                "₹": "INR", "RUPEE": "INR", "RUPEES": "INR"
            }
            tender["currency"] = currency_map.get(curr, curr)
            
        # Ensure tender value is numeric
        if tender.get("tender_value") and not isinstance(tender["tender_value"], (int, float)):
            try:
                tender["tender_value"] = float(tender["tender_value"])
            except (ValueError, TypeError):
                tender["tender_value"] = None
                
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
            CREATE TABLE IF NOT EXISTS public.unified_tenders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                title TEXT,
                description TEXT,
                date_published DATE,
                closing_date DATE,
                tender_type TEXT,
                tender_value TEXT,
                tender_currency TEXT,
                location TEXT,
                issuing_authority TEXT,
                keywords TEXT,
                contact_information TEXT,
                source TEXT,
                url TEXT,
                buyer TEXT,
                raw_id TEXT,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                metadata JSONB,
                CONSTRAINT unified_tenders_source_raw_id_unique UNIQUE (source, raw_id)
            );
            
            CREATE INDEX IF NOT EXISTS unified_tenders_source_idx ON public.unified_tenders (source);
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

    def _extract_address_information(self, description):
        """Extract address information from description text."""
        if not description or not isinstance(description, str):
            return {}
            
        address_info = {}
        
        # Common address indicators
        address_indicators = [
            r'address\s*:\s*([^\.;]*[0-9][^\.;]*)',
            r'located at\s*[:-]?\s*([^\.;]*[0-9][^\.;]*)',
            r'location\s*[:-]?\s*([^\.;]*[0-9][^\.;]*)',
            r'([A-Za-z0-9\s]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})',  # US format: Street, City, ST ZIPCODE
            r'([A-Za-z0-9\s]+,\s*[A-Za-z\s]+,\s*[A-Z]{2})',  # US format without zip: Street, City, ST
            r'([A-Za-z0-9\s]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})',  # EU format: Street, City ZIPCODE
        ]
        
        for pattern in address_indicators:
            matches = re.findall(pattern, description, re.IGNORECASE)
            if matches:
                # Take the longest match as it likely contains more information
                longest_match = max(matches, key=len)
                address_info['full_address'] = longest_match.strip()
                break
                
        # Try to extract postal/zip code
        zip_patterns = [
            r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b',  # UK format
            r'\b(\d{5}(-\d{4})?)\b',  # US format
            r'\b([A-Z]\d[A-Z]\s*\d[A-Z]\d)\b',  # Canadian format
            r'\b(\d{4}\s*[A-Z]{2})\b',  # Dutch format
            r'\b(\d{5})\b'  # Simple 5-digit format
        ]
        
        for pattern in zip_patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            if matches:
                # Take the first match
                if isinstance(matches[0], tuple):
                    # Some regex groups return tuples
                    zipcode = matches[0][0].strip()
                else:
                    zipcode = matches[0].strip()
                address_info['postal_code'] = zipcode
                break
                
        return address_info

    def _enhanced_process_raw_tenders(self, raw_data, source_name=None):
        """
        Enhanced processing of raw tenders with better validation and duplicate detection.
        
        Args:
            raw_data (list): List of raw tender data
            source_name (str): Optional source name for context-specific processing
            
        Returns:
            list: List of normalized tenders
        """
        if not raw_data:
            return []
            
        print(f"Processing {len(raw_data)} raw tenders with enhanced validation")
        
        # Track already processed tenders to avoid duplicates
        processed_tenders = []
        skipped_tenders = 0
        error_tenders = 0
        
        # First pass to clean and standardize data
        cleaned_data = []
        for item in raw_data:
            try:
                # First check if it's a string that needs to be parsed
                if isinstance(item, str):
                    try:
                        parsed_item = json.loads(item)
                        cleaned_data.append(parsed_item)
                        continue
                    except json.JSONDecodeError:
                        # Not valid JSON, keep as is
                        cleaned_data.append(item)
                        continue
                
                # If it's a dict, use it directly
                if isinstance(item, dict):
                    cleaned_data.append(item)
                    continue
                    
                # If it has a 'data' field that contains the tender
                if hasattr(item, 'get') and callable(item.get):
                    try:
                        data = item.get('data')
                        if data is not None:
                            if isinstance(data, str):
                                try:
                                    parsed_data = json.loads(data)
                                    cleaned_data.append(parsed_data)
                                    continue
                                except:
                                    # Not valid JSON, keep the string
                                    cleaned_data.append(data)
                                    continue
                            else:
                                cleaned_data.append(data)
                                continue
                    except:
                        # Fallback if get fails
                        pass
                
                # Add the item as-is
                cleaned_data.append(item)
                
            except Exception as e:
                print(f"Error pre-processing raw tender item: {e}")
                error_tenders += 1
        
        # Second pass to normalize and validate
        for tender in cleaned_data:
            try:
                # Normalize the tender
                normalized_tender = self._normalize_tender(tender, source_name)
                
                if not normalized_tender:
                    skipped_tenders += 1
                    continue
                    
                # Check if this might be a duplicate of something we already processed
                if self._detect_potential_duplicate(normalized_tender, processed_tenders):
                    print(f"Skipping potential duplicate: {normalized_tender.get('notice_title', '')[:50]}...")
                    skipped_tenders += 1
                    continue
                    
                # Validate and clean the tender data
                validated_tender = self._validate_normalized_tender(normalized_tender)
                
                if validated_tender:
                    # Extract address info if available
                    if 'description' in validated_tender and validated_tender['description']:
                        address_info = self._extract_address_information(validated_tender['description'])
                        if address_info:
                            # Add to metadata
                            if not isinstance(validated_tender.get('metadata'), dict):
                                validated_tender['metadata'] = {}
                            validated_tender['metadata']['address_info'] = address_info
                    
                    processed_tenders.append(validated_tender)
                else:
                    skipped_tenders += 1
                    
            except Exception as e:
                print(f"Error during tender normalization: {e}")
                error_tenders += 1
        
        print(f"Enhanced processing results: {len(processed_tenders)} valid tenders, {skipped_tenders} skipped, {error_tenders} errors")
        return processed_tenders
