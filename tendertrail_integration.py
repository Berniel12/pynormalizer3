import json
import sys
import subprocess
from typing import Dict, Any, List, Optional
from supabase import create_client, Client
import uuid
import traceback

class TenderTrailIntegration:
    """Integration layer for TenderTrail normalization workflow."""
    
    def __init__(self, normalizer, preprocessor, supabase_url, supabase_key):
        """Initialize the integration layer."""
        self.normalizer = normalizer
        self.preprocessor = preprocessor
        self.supabase = create_client(supabase_url, supabase_key)
        
        # Try to install psycopg2 if it's missing
        try:
            import psycopg2
        except ImportError:
            print("Installing psycopg2-binary...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
                print("psycopg2-binary installed successfully")
            except Exception as e:
                print(f"Failed to install psycopg2-binary: {e}")
        
        # Try to create exec_sql function if it doesn't exist
        self._create_exec_sql_function()
        
        # Ensure required tables exist
        self._create_unified_tenders_table()
        self._create_errors_table()
        self._create_target_schema_table()
    
    def process_source(self, tenders, source_name, create_tables=True):
        """
        Process tenders from a source, normalize and insert them into the database.
        """
        print(f"Processing {len(tenders)} tenders from source: {source_name}")
        
        try:
            # Make sure tables exist if requested
            if create_tables:
                self._create_unified_tenders_table()
                self._create_errors_table()
                self._create_target_schema_table()
            
            # Exit early if no tenders
            if not tenders:
                print(f"No tenders to process for source: {source_name}")
                return 0, 0
            
            # Track statistics
            processed_count = 0
            error_count = 0
            normalized_tenders = []
            
            # Process each tender
            for tender in tenders:
                try:
                    # First attempt direct normalization (preferred for known source formats)
                    normalized_tender = self._normalize_tender(tender, source_name)
                    
                    # If we got a valid normalized tender, use it
                    if normalized_tender and isinstance(normalized_tender, dict) and normalized_tender.get("notice_title"):
                        normalized_tenders.append(normalized_tender)
                        processed_count += 1
                        continue
                    
                    # If direct normalization failed, try the standard process with preprocessor and normalizer
                    if self.preprocessor and self.normalizer:
                        # Preprocess the tender
                        preprocessed_tender = self.preprocessor.preprocess_tender(tender)
                        
                        # Normalize the preprocessed tender
                        normalized_tender = self.normalizer.normalize_tender(preprocessed_tender)
                        
                        # Add to the list of normalized tenders
                        normalized_tenders.append(normalized_tender)
                        processed_count += 1
                    else:
                        # If no preprocessor or normalizer, create a minimal record
                        print("Warning: No preprocessor or normalizer available, creating minimal record")
                        minimal_tender = {
                            "notice_id": str(uuid.uuid4()),
                            "notice_type": "Default",
                            "notice_title": f"Untitled Tender from {source_name}",
                            "description": "No preprocessor or normalizer available",
                            "source": source_name,
                            "country": "",
                            "location": "",
                            "issuing_authority": source_name,
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
                    self._insert_error(source_name, "processing_error", error_message, str(tender) if tender else "")
            
            # Insert all normalized tenders into the database
            if normalized_tenders:
                insert_count = self._insert_normalized_tenders(normalized_tenders)
                print(f"Inserted {insert_count} tenders from source: {source_name}")
            else:
                print(f"No tenders were successfully normalized for source: {source_name}")
            
            return processed_count, error_count
        except Exception as e:
            print(f"Error processing source {source_name}: {e}")
            traceback.print_exc()
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
            elif isinstance(json_data, dict):
                # Try to find a list in the dictionary
                for key, value in json_data.items():
                    if isinstance(value, list) and value:
                        tenders = value
                        print(f"Found tenders list in key: {key}")
                        break
                
                if not tenders and "data" in json_data and json_data["data"]:
                    tenders = [json_data["data"]]
                    print("Using 'data' field as a single tender")
            else:
                print(f"Unsupported JSON data type: {type(json_data)}")
                return 0, 0
            
            # Process the tenders
            print(f"Found {len(tenders)} tenders to process for source: {source_name}")
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
                    # If the error indicates the table doesn't exist, try to create it
                    if "relation" in str(e) and "does not exist" in str(e):
                        print(f"target_schema table doesn't exist, creating it: {e}")
                    else:
                        print(f"target_schema table check failed: {e}")
                        
                # Try to create the table without RPC first
                try:
                    # We will directly try a simple INSERT
                    print("Creating target_schema table via direct table create")
                    default_schema = self._get_default_target_schema()
                    
                    # Try to create a minimal schema with select, upsert, and delete
                    create_table_sql = """
                    CREATE TABLE IF NOT EXISTS public.target_schema (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        schema JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                    );
                    """
                    
                    # Try to execute the create table via other functions if available
                    for function_name in ['exec_sql', 'run_sql', 'execute_sql']:
                        try:
                            print(f"Attempting to create table via '{function_name}' RPC")
                            self.supabase.rpc(function_name, {'sql': create_table_sql}).execute()
                            print(f"Successfully created table via '{function_name}'")
                            # If successful, try to insert default schema
                            try:
                                self.supabase.table('target_schema').insert({
                                    'schema': default_schema
                                }).execute()
                                print("Successfully added default schema")
                            except Exception as insert_e:
                                print(f"Failed to insert default schema: {insert_e}")
                            return
                        except Exception as fn_e:
                            print(f"Function '{function_name}' failed: {fn_e}")
                    
                    # If we get here, table creation methods failed
                    print("Could not create target_schema table, continuing with in-memory schema")
                except Exception as create_e:
                    print(f"Error creating target_schema table: {create_e}")
            except Exception as e:
                print(f"Error checking target_schema: {e}")
            
            # We'll continue with the in-memory default schema
            print("Using in-memory default schema")
        except Exception as general_e:
            print(f"General error in _create_target_schema_table: {general_e}")
            print("Continuing with in-memory schema")
    
    def _get_raw_tenders(self, source_name: str, batch_size: int) -> List[Dict[str, Any]]:
        """Get raw tenders from source table."""
        try:
            # First, try the source_tenders table format
            table_name = f"{source_name}_tenders"
            try:
                response = self.supabase.table(table_name).select('*').limit(batch_size).execute()
                if response and response.data:
                    # Process tenders
                    return self._process_raw_tenders(response.data)
            except Exception as e:
                print(f"Table {table_name} not found, trying direct source table {source_name}")
            
            # Try direct source name as table
            try:
                # First try with processed field
                try:
                    response = self.supabase.table(source_name).select('*').eq('processed', False).limit(batch_size).execute()
                    # Mark processed records
                    if response and response.data:
                        ids = []
                        for item in response.data:
                            if isinstance(item, dict) and 'id' in item:
                                ids.append(item['id'])
                        
                        if ids:
                            self.supabase.table(source_name).update({'processed': True}).in_('id', ids).execute()
                        
                        return self._process_raw_tenders(response.data)
                except Exception as e:
                    print(f"Error querying with processed field, trying without: {e}")
                    # Try without processed field
                    response = self.supabase.table(source_name).select('*').limit(batch_size).execute()
                    if response and response.data:
                        return self._process_raw_tenders(response.data)
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
    
    def _insert_normalized_tenders(self, normalized_tenders: List[Dict[str, Any]]) -> None:
        """Insert normalized tenders into unified table."""
        if not normalized_tenders:
            print("No tenders to insert")
            return None
        
        try:
            print(f"Preparing to insert {len(normalized_tenders)} tenders")
            
            # Create a copy to avoid modifying the original
            tenders_to_insert = []
            
            # Ensure all fields are properly formatted
            for tender in normalized_tenders:
                cleaned_tender = {}
                
                # Required fields for the unified_tenders table 
                required_fields = {
                    "title": "Untitled Tender",
                    "source": "unknown"
                }
                
                # Optional fields that match the database schema 
                optional_fields = [
                    "tender_value", "tender_currency", "location", 
                    "issuing_authority", "keywords", "tender_type", 
                    "project_size", "contact_information", "raw_id", 
                    "processed_at"
                ]
                
                # Date fields that need special handling
                date_fields = ["date_published", "closing_date"]
                
                # Text fields that should always be included
                text_fields = ["description"]
                
                # Ensure required fields exist
                for field, default_value in required_fields.items():
                    if field in tender and tender[field] is not None and tender[field] != "":
                        cleaned_tender[field] = str(tender[field])[:1000]  # Truncate long values
                    else:
                        cleaned_tender[field] = default_value
                
                # Handle text fields
                for field in text_fields:
                    if field in tender and tender[field] is not None:
                        cleaned_tender[field] = str(tender[field])[:2000]  # Truncate long values
                    else:
                        cleaned_tender[field] = ""  # Empty string is valid for text fields
                
                # Handle date fields - set to NULL (None) if empty or invalid
                for field in date_fields:
                    if field in tender and tender[field] and tender[field] != "":
                        try:
                            # Try to parse and format the date if it's not already in YYYY-MM-DD format
                            if isinstance(tender[field], str) and not self._is_valid_date_format(tender[field]):
                                parsed_date = self._parse_date(tender[field])
                                if parsed_date:
                                    cleaned_tender[field] = parsed_date
                                else:
                                    cleaned_tender[field] = None  # Set to NULL if can't parse
                            else:
                                cleaned_tender[field] = tender[field]
                        except:
                            cleaned_tender[field] = None  # Set to NULL on error
                    else:
                        cleaned_tender[field] = None  # Set to NULL if empty
                
                # Add optional fields if they exist
                for field in optional_fields:
                    if field in tender and tender[field] is not None and tender[field] != "" and field not in cleaned_tender:
                        if isinstance(tender[field], (dict, list)):
                            # Convert complex objects to JSON string
                            cleaned_tender[field] = json.dumps(tender[field])[:2000]  # Truncate long JSON
                        else:
                            cleaned_tender[field] = str(tender[field])[:2000]  # Truncate long values
                            
                # Add processed_at if not present
                if "processed_at" not in cleaned_tender:
                    cleaned_tender["processed_at"] = self._get_current_timestamp()
                    
                tenders_to_insert.append(cleaned_tender)
                
            # Create unified_tenders table if it doesn't exist
            try:
                self._create_unified_tenders_table()
            except Exception as e:
                print(f"Error creating unified_tenders table: {e}")
            
            # Insert in batches to avoid timeout issues
            batch_size = 50
            for i in range(0, len(tenders_to_insert), batch_size):
                batch = tenders_to_insert[i:i + batch_size]
                try:
                    print(f"Inserting batch {i//batch_size + 1}/{(len(tenders_to_insert) + batch_size - 1)//batch_size}")
                    response = self.supabase.table('unified_tenders').insert(batch).execute()
                    if hasattr(response, 'data'):
                        print(f"Successfully inserted batch with {len(batch)} tenders")
                    else:
                        print(f"Batch insert completed but no data returned")
                except Exception as batch_error:
                    print(f"Error inserting batch: {batch_error}")
                    # Try to insert one by one
                    print(f"Attempting one-by-one insertion for batch {i//batch_size + 1}")
                    for tender in batch:
                        try:
                            self.supabase.table('unified_tenders').insert(tender).execute()
                        except Exception as single_error:
                            print(f"Error inserting single tender: {single_error}")
            
            print(f"Completed inserting {len(tenders_to_insert)} tenders")
            return True
        except Exception as e:
            print(f"Error inserting normalized tenders: {e}")
            print("Data sample that failed:", normalized_tenders[0] if normalized_tenders else "No data")
            return None
    
    def _is_valid_date_format(self, date_str):
        """Check if a date string is in YYYY-MM-DD format."""
        if not isinstance(date_str, str):
            return False
        
        try:
            # Try to match YYYY-MM-DD format
            import re
            return re.match(r'^\d{4}-\d{2}-\d{2}$', date_str) is not None
        except:
            return False
    
    def _parse_date(self, date_str):
        """Try to parse a date string into YYYY-MM-DD format."""
        if not date_str or not isinstance(date_str, str):
            return None
        
        # Common date formats to try
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.strftime('%Y-%m-%d')
        except:
            try:
                # Try common formats
                import datetime
                formats = [
                    '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d',
                    '%b %d, %Y', '%d %b %Y', '%B %d, %Y', '%d %B %Y'
                ]
                
                for fmt in formats:
                    try:
                        return datetime.datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                    except:
                        continue
                    
                # If nothing works, return None
                return None
            except:
                return None
    
    def _create_unified_tenders_table(self):
        """Create unified_tenders table if it doesn't exist."""
        try:
            # Check if table already exists using simple query
            try:
                try:
                    # Try direct query
                    response = self.supabase.table('unified_tenders').select('id').limit(1).execute()
                    if hasattr(response, 'data'):
                        print("unified_tenders table already exists")
                        return
                except Exception as e:
                    # If the error indicates the table doesn't exist, try to create it
                    if "relation" in str(e) and "does not exist" in str(e):
                        print(f"unified_tenders table doesn't exist, creating it: {e}")
                    else:
                        print(f"unified_tenders table check failed: {e}")
                
                # Create table using RPC if available
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS public.unified_tenders (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    title TEXT NOT NULL,
                    description TEXT,
                    date_published DATE,
                    closing_date DATE,
                    tender_value TEXT,
                    tender_currency TEXT,
                    location TEXT,
                    issuing_authority TEXT,
                    keywords TEXT,
                    tender_type TEXT,
                    project_size TEXT,
                    contact_information TEXT,
                    source TEXT NOT NULL,
                    raw_id TEXT,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                )
                """
                
                # Try to execute the create table via other functions if available
                for function_name in ['exec_sql', 'run_sql', 'execute_sql']:
                    try:
                        print(f"Attempting to create unified_tenders table via '{function_name}' RPC")
                        self.supabase.rpc(function_name, {'sql': create_table_sql}).execute()
                        print(f"Successfully created unified_tenders table via '{function_name}'")
                        return
                    except Exception as fn_e:
                        print(f"Function '{function_name}' failed: {fn_e}")
                
                # If we get here, table creation methods failed but we'll continue
                print("Could not create unified_tenders table via RPC, will try insert anyway")
            except Exception as check_e:
                print(f"Error checking unified_tenders table: {check_e}")
        except Exception as general_e:
            print(f"General error in _create_unified_tenders_table: {general_e}")
            print("Continuing without table creation")
    
    def _log_errors(self, errors: List[Dict[str, Any]]) -> None:
        """Log processing errors to database."""
        if not errors:
            print("No errors to log")
            return None
            
        try:
            print(f"Preparing to log {len(errors)} errors")
            
            # Skip table creation to avoid hanging
            # self._create_errors_table()
            
            # Truncate very long error messages to avoid DB issues
            for error in errors:
                if 'error' in error and isinstance(error['error'], str) and len(error['error']) > 1000:
                    error['error'] = error['error'][:997] + '...'
                
            try:    
                print("Inserting errors into normalization_errors table")
                response = self.supabase.table('normalization_errors').insert(errors).execute()
                print(f"Successfully logged {len(errors)} errors")
                return response.data
            except Exception as e:
                print(f"Failed to log errors to database: {e}")
                # Print the first few errors to console as fallback
                print("Sample errors:")
                for i, error in enumerate(errors[:5]):
                    print(f"  Error {i+1}: {error.get('tender_id', 'unknown')} - {error.get('error', 'unknown')}")
                return None
        except Exception as e:
            print(f"Error logging errors: {e}")
            return None
    
    def _create_errors_table(self):
        """Create normalization_errors table if it doesn't exist."""
        try:
            # Check if table already exists using simple query
            try:
                try:
                    # Try direct query
                    response = self.supabase.table('normalization_errors').select('id').limit(1).execute()
                    if hasattr(response, 'data'):
                        print("normalization_errors table already exists")
                        return
                except Exception as e:
                    # If the error indicates the table doesn't exist, try to create it
                    if "relation" in str(e) and "does not exist" in str(e):
                        print(f"normalization_errors table doesn't exist, creating it: {e}")
                    else:
                        print(f"normalization_errors table check failed: {e}")
                
                # Create table using RPC if available
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS public.normalization_errors (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    tender_id TEXT,
                    error TEXT,
                    source TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                )
                """
                
                # Try to execute the create table via other functions if available
                for function_name in ['exec_sql', 'run_sql', 'execute_sql']:
                    try:
                        print(f"Attempting to create normalization_errors table via '{function_name}' RPC")
                        self.supabase.rpc(function_name, {'sql': create_table_sql}).execute()
                        print(f"Successfully created normalization_errors table via '{function_name}'")
                        return
                    except Exception as fn_e:
                        print(f"Function '{function_name}' failed: {fn_e}")
                
                # If we get here, table creation methods failed but we'll continue
                print("Could not create normalization_errors table via RPC, will try insert anyway")
            except Exception as check_e:
                print(f"Error checking normalization_errors table: {check_e}")
        except Exception as general_e:
            print(f"General error in _create_errors_table: {general_e}")
            print("Continuing without table creation")
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    def _run_sql_directly(self, sql: str) -> None:
        """Run SQL directly using psycopg2 connection if available."""
        try:
            # This is a fallback method if RPC is not available
            # It will attempt to use psycopg2 directly if available
            try:
                import psycopg2
            except ImportError:
                print("Installing psycopg2-binary...")
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
                    import psycopg2
                    print("psycopg2-binary installed successfully")
                except Exception as e:
                    print(f"Failed to install psycopg2-binary: {e}")
                    return
                
            from urllib.parse import urlparse
            
            # Parse Supabase URL to get connection details
            # SyncClient in Docker uses different attribute names
            try:
                # Try to get URL from different possible attributes
                url = None
                if hasattr(self.supabase, 'url'):
                    url = self.supabase.url
                elif hasattr(self.supabase, '_url'):
                    url = self.supabase._url
                elif hasattr(self.supabase, 'rest_url'):
                    url = self.supabase.rest_url
                
                if not url:
                    print("Unable to find URL attribute in Supabase client, skipping direct SQL execution")
                    return
                
                # Try to get key from different possible attributes
                key = None
                if hasattr(self.supabase, 'key'):
                    key = self.supabase.key
                elif hasattr(self.supabase, '_key'):
                    key = self.supabase._key
                elif hasattr(self.supabase, 'supabase_key'):
                    key = self.supabase.supabase_key
                
                if not key:
                    print("Unable to find key attribute in Supabase client, skipping direct SQL execution")
                    return
                
                # Parse URL to get host
                parsed_url = urlparse(url)
                
                # Check if the URL is valid
                if not parsed_url.netloc:
                    print(f"Invalid Supabase URL format: {url}, skipping direct SQL execution")
                    return
                
                host_parts = parsed_url.netloc.split('.')
                if len(host_parts) < 2:
                    print(f"Cannot extract project ID from URL: {url}, skipping direct SQL execution")
                    return
                
                host = host_parts[0]
                
                # Connect directly to PostgreSQL with timeout
                print(f"Attempting direct database connection to {host}.supabase.co")
                
                # Add connection timeout to prevent hanging
                conn = psycopg2.connect(
                    host=f"{host}.supabase.co",
                    database="postgres",
                    user="postgres",
                    password=key,
                    connect_timeout=10  # 10 second timeout
                )
                
                # Execute SQL
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
                conn.close()
                print(f"Successfully executed SQL directly")
            except Exception as e:
                print(f"Error connecting to database: {e}")
                print("Continuing without table creation")
        except Exception as e:
            print(f"Failed to run SQL directly: {e}")
            print("Continuing without table creation")
            
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

    def _normalize_tender(self, tender, source='unknown'):
        """
        Normalize a tender directly, bypassing the preprocessor for known source formats.
        This is a more direct approach that avoids errors in the preprocessor.
        """
        try:
            if not tender:
                print(f"Warning: Empty tender received from source {source}")
                return {}
            
            # Start with a base document that contains all required fields with default values
            normalized = {
                "notice_id": str(uuid.uuid4()),  # Default ID if none is found
                "notice_type": "Default",
                "notice_title": "Untitled Tender",
                "description": "",
                "country": "",
                "location": "",
                "issuing_authority": source,
                "date_published": None,
                "closing_date": None,
                "currency": "",
                "value": None,
                "cpvs": [],
                "buyer": "",
                "email": "",
                "source": source,
                "url": "",
                "tag": [],
                "language": "en"
            }
            
            # Source-specific normalization logic
            if source.lower() == 'afd':
                # AFD specific normalization
                print(f"Applying AFD-specific normalization for tender: {tender.get('title', 'No Title')}")
                
                # Notice ID - Use reference number if available, otherwise afd_id
                if 'reference' in tender and tender['reference']:
                    normalized["notice_id"] = tender['reference']
                elif 'afd_id' in tender and tender['afd_id']:
                    normalized["notice_id"] = tender['afd_id']
                
                # Title
                if 'title' in tender and tender['title']:
                    normalized["notice_title"] = tender['title']
                
                # Tender type
                if 'tender_type' in tender and tender['tender_type']:
                    normalized["notice_type"] = tender['tender_type']
                
                # Description
                if 'description' in tender and tender['description']:
                    normalized["description"] = tender['description']
                
                # Country
                if 'country' in tender and tender['country']:
                    normalized["country"] = tender['country']
                
                # Set issuing authority specifically for AFD
                normalized["issuing_authority"] = "Agence Française de Développement"
                
                # Dates
                if 'published_date' in tender and tender['published_date']:
                    normalized["date_published"] = self._parse_date(tender['published_date'])
                
                if 'closing_date' in tender and tender['closing_date']:
                    normalized["closing_date"] = self._parse_date(tender['closing_date'])
                
                # URL
                if 'url' in tender and tender['url']:
                    normalized["url"] = tender['url']
                
                # Include any additional fields as metadata
                metadata = {}
                for k, v in tender.items():
                    if k not in normalized and v is not None:
                        metadata[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                
                # If we have metadata, add it to the normalized tender
                if metadata:
                    normalized["metadata"] = json.dumps(metadata)
                
            elif source.lower() == 'sam_gov':
                # SAM.gov specific normalization
                print(f"Applying SAM.gov-specific normalization for tender: {tender.get('title', 'No Title')}")
                
                # Notice ID
                if 'notice_id' in tender and tender['notice_id']:
                    normalized["notice_id"] = tender['notice_id']
                
                # Title
                if 'title' in tender and tender['title']:
                    normalized["notice_title"] = tender['title']
                
                # Type
                if 'type' in tender and tender['type']:
                    normalized["notice_type"] = tender['type']
                
                # Description
                if 'description' in tender and tender['description']:
                    normalized["description"] = tender['description']
                
                # Issuing authority
                if 'agency' in tender and tender['agency']:
                    normalized["issuing_authority"] = tender['agency']
                
                # Dates
                if 'posted_date' in tender and tender['posted_date']:
                    normalized["date_published"] = self._parse_date(tender['posted_date'])
                
                if 'response_deadline' in tender and tender['response_deadline']:
                    normalized["closing_date"] = self._parse_date(tender['response_deadline'])
                
                # URL
                if 'url' in tender and tender['url']:
                    normalized["url"] = tender['url']
                
                # Location/Country
                if 'location' in tender and tender['location']:
                    normalized["location"] = tender['location']
                    # Try to extract country from location
                    if ',' in tender['location']:
                        parts = tender['location'].split(',')
                        if len(parts) > 1:
                            normalized["country"] = parts[-1].strip()
                
                # Include any additional fields as metadata
                metadata = {}
                for k, v in tender.items():
                    if k not in normalized and v is not None:
                        metadata[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                
                # If we have metadata, add it to the normalized tender
                if metadata:
                    normalized["metadata"] = json.dumps(metadata)
                
            elif source.lower() == 'ungm':
                # UNGM specific normalization
                print(f"Applying UNGM-specific normalization for tender: {tender.get('title', 'No Title')}")
                
                # Notice ID
                if 'reference' in tender and tender['reference']:
                    normalized["notice_id"] = tender['reference']
                
                # Title
                if 'title' in tender and tender['title']:
                    normalized["notice_title"] = tender['title']
                
                # Type
                if 'type' in tender and tender['type']:
                    normalized["notice_type"] = tender['type']
                
                # Description
                if 'description' in tender and tender['description']:
                    normalized["description"] = tender['description']
                
                # Issuing authority
                if 'agency' in tender and tender['agency']:
                    normalized["issuing_authority"] = tender['agency']
                
                # Country
                if 'country' in tender and tender['country']:
                    normalized["country"] = tender['country']
                
                # Dates
                if 'published' in tender and tender['published']:
                    normalized["date_published"] = self._parse_date(tender['published'])
                
                if 'deadline' in tender and tender['deadline']:
                    normalized["closing_date"] = self._parse_date(tender['deadline'])
                
                # URL
                if 'url' in tender and tender['url']:
                    normalized["url"] = tender['url']
                
                # Include any additional fields as metadata
                metadata = {}
                for k, v in tender.items():
                    if k not in normalized and v is not None:
                        metadata[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                
                # If we have metadata, add it to the normalized tender
                if metadata:
                    normalized["metadata"] = json.dumps(metadata)
            
            elif source.lower() == 'ted_eu':
                # TED EU specific normalization
                print(f"Applying TED EU-specific normalization for tender: {tender.get('title', 'No Title')}")
                
                # Notice ID
                if 'reference' in tender and tender['reference']:
                    normalized["notice_id"] = tender['reference']
                elif 'doc_id' in tender and tender['doc_id']:
                    normalized["notice_id"] = tender['doc_id']
                
                # Title
                if 'title' in tender and tender['title']:
                    normalized["notice_title"] = tender['title']
                
                # Type
                if 'notice_type' in tender and tender['notice_type']:
                    normalized["notice_type"] = tender['notice_type']
                
                # Description
                if 'description' in tender and tender['description']:
                    normalized["description"] = tender['description']
                
                # Issuing authority
                if 'authority' in tender and tender['authority']:
                    normalized["issuing_authority"] = tender['authority']
                
                # Country
                if 'country' in tender and tender['country']:
                    normalized["country"] = tender['country']
                
                # Dates
                if 'publication_date' in tender and tender['publication_date']:
                    normalized["date_published"] = self._parse_date(tender['publication_date'])
                
                if 'deadline' in tender and tender['deadline']:
                    normalized["closing_date"] = self._parse_date(tender['deadline'])
                
                # URL
                if 'url' in tender and tender['url']:
                    normalized["url"] = tender['url']
                
                # Value and currency
                if 'value' in tender and tender['value'] is not None:
                    try:
                        normalized["value"] = float(tender['value'])
                    except (ValueError, TypeError):
                        pass
                
                if 'currency' in tender and tender['currency']:
                    normalized["currency"] = tender['currency']
                
                # CPVs
                if 'cpvs' in tender and tender['cpvs']:
                    if isinstance(tender['cpvs'], list):
                        normalized["cpvs"] = tender['cpvs']
                    elif isinstance(tender['cpvs'], str):
                        normalized["cpvs"] = [cpv.strip() for cpv in tender['cpvs'].split(',') if cpv.strip()]
                
                # Include any additional fields as metadata
                metadata = {}
                for k, v in tender.items():
                    if k not in normalized and v is not None:
                        metadata[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                
                # If we have metadata, add it to the normalized tender
                if metadata:
                    normalized["metadata"] = json.dumps(metadata)
            
            else:
                # Generic normalization for unknown sources
                # Map common field names that might be in the tender
                field_mappings = {
                    # ID fields
                    'id': 'notice_id', 
                    'tender_id': 'notice_id',
                    'notice_id': 'notice_id',
                    'reference': 'notice_id',
                    'reference_number': 'notice_id',
                    
                    # Title fields
                    'title': 'notice_title',
                    'tender_title': 'notice_title',
                    'name': 'notice_title',
                    
                    # Type fields
                    'type': 'notice_type',
                    'tender_type': 'notice_type',
                    'procedure_type': 'notice_type',
                    
                    # Description fields
                    'description': 'description',
                    'summary': 'description',
                    'details': 'description',
                    
                    # Country/Location fields
                    'country': 'country',
                    'nation': 'country',
                    'location': 'location',
                    'place': 'location',
                    
                    # Authority fields
                    'issuing_authority': 'issuing_authority',
                    'authority': 'issuing_authority',
                    'agency': 'issuing_authority',
                    'buyer': 'issuing_authority',
                    'contracting_authority': 'issuing_authority',
                    
                    # Date fields
                    'date_published': 'date_published',
                    'published_date': 'date_published',
                    'publication_date': 'date_published',
                    'posted_date': 'date_published',
                    'published': 'date_published',
                    
                    'closing_date': 'closing_date',
                    'deadline': 'closing_date',
                    'response_deadline': 'closing_date',
                    'submission_deadline': 'closing_date',
                    
                    # Value fields
                    'value': 'value',
                    'amount': 'value',
                    'total_value': 'value',
                    'estimated_value': 'value',
                    
                    'currency': 'currency',
                    
                    # CPV fields
                    'cpvs': 'cpvs',
                    'cpv_codes': 'cpvs',
                    'cpv': 'cpvs',
                    
                    # Buyer fields
                    'buyer': 'buyer',
                    'contracting_authority_name': 'buyer',
                    
                    # Contact fields
                    'email': 'email',
                    'contact_email': 'email',
                    
                    # URL fields
                    'url': 'url',
                    'link': 'url',
                    'tender_url': 'url',
                    
                    # Tags
                    'tag': 'tag',
                    'tags': 'tag',
                    'categories': 'tag',
                    
                    # Language
                    'language': 'language',
                    'lang': 'language'
                }
                
                # For each field in the tender, try to map it to a normalized field
                for tender_field, normalized_field in field_mappings.items():
                    if tender_field in tender and tender[tender_field] is not None:
                        # Special handling for cpvs field to ensure it's a list
                        if normalized_field == 'cpvs':
                            if isinstance(tender[tender_field], list):
                                normalized[normalized_field] = tender[tender_field]
                            elif isinstance(tender[tender_field], str):
                                normalized[normalized_field] = [cpv.strip() for cpv in tender[tender_field].split(',') if cpv.strip()]
                            continue
                        
                        # Special handling for date fields
                        if normalized_field in ['date_published', 'closing_date']:
                            normalized[normalized_field] = self._parse_date(tender[tender_field])
                            continue
                        
                        # Special handling for value field to ensure it's a float
                        if normalized_field == 'value' and tender[tender_field] is not None:
                            try:
                                normalized[normalized_field] = float(tender[tender_field])
                            except (ValueError, TypeError):
                                pass
                            continue
                        
                        # For other fields, just copy the value
                        normalized[normalized_field] = tender[tender_field]
                
                # Include any additional fields as metadata
                metadata = {}
                for k, v in tender.items():
                    if k not in field_mappings and v is not None:
                        metadata[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                
                # If we have metadata, add it to the normalized tender
                if metadata:
                    normalized["metadata"] = json.dumps(metadata)
            
            # Ensure the source is recorded
            normalized["source"] = source
            
            # Handle tag field to ensure it's always a list
            if not normalized.get("tag") or not isinstance(normalized["tag"], list):
                normalized["tag"] = []
            
            # Perform basic validation
            self._validate_normalized_tender(normalized)
            
            return normalized
        except Exception as e:
            print(f"Error normalizing tender from source {source}: {e}")
            traceback.print_exc()
            # Return an empty dict if normalization fails
            return {}

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
            # Check if exec_sql exists already
            function_exists = False
            try:
                # Try to call the function with a simple command
                self.supabase.rpc('exec_sql', {'sql': 'SELECT 1'}).execute()
                print("exec_sql function exists")
                function_exists = True
            except Exception as e:
                if "Could not find the function" in str(e) or "does not exist" in str(e):
                    print("exec_sql function does not exist, will try to create it")
                else:
                    print(f"Error checking exec_sql function: {e}")
            
            if function_exists:
                return True
            
            # Try to create the function using direct psycopg2 connection if possible
            try:
                # Try to use URL and key from supabase client to connect
                from urllib.parse import urlparse
                import psycopg2
                
                # Try to extract URL from client
                url = None
                if hasattr(self.supabase, 'url'):
                    url = self.supabase.url
                elif hasattr(self.supabase, '_url'):
                    url = self.supabase._url
                elif hasattr(self.supabase, 'rest_url'):
                    url = self.supabase.rest_url
                    
                # Try to extract key from client
                key = None
                if hasattr(self.supabase, 'key'):
                    key = self.supabase.key
                elif hasattr(self.supabase, '_key'):
                    key = self.supabase._key
                elif hasattr(self.supabase, 'supabase_key'):
                    key = self.supabase.supabase_key
                    
                if not url or not key:
                    print("Could not extract URL and key from Supabase client")
                    return False
                    
                # Get the host from the URL
                parsed_url = urlparse(url)
                if not parsed_url.netloc:
                    print(f"Invalid URL format: {url}")
                    return False
                    
                # Extract project ID from host 
                host_parts = parsed_url.netloc.split('.')
                if len(host_parts) < 2:
                    print(f"Could not parse host from URL: {url}")
                    return False
                    
                project_id = host_parts[0]
                
                # Connect to the database using psycopg2
                conn_string = f"postgresql://postgres:{key}@{project_id}.supabase.co:5432/postgres"
                
                print(f"Attempting to connect to {project_id}.supabase.co")
                conn = psycopg2.connect(conn_string, connect_timeout=10)
                cursor = conn.cursor()
                
                # Create the exec_sql function
                exec_sql_function = """
                CREATE OR REPLACE FUNCTION public.exec_sql(sql text)
                RETURNS void AS $$
                BEGIN
                  EXECUTE sql;
                END;
                $$ LANGUAGE plpgsql SECURITY DEFINER;
                """
                
                cursor.execute(exec_sql_function)
                conn.commit()
                cursor.close()
                conn.close()
                print("Successfully created exec_sql function")
                return True
            except Exception as direct_e:
                print(f"Could not create exec_sql function via direct connection: {direct_e}")
            
            # If we couldn't create the function, we'll just continue without it
            print("Will continue without exec_sql function, using fallback methods for schema creation")
            return False
        except Exception as e:
            print(f"Error in _create_exec_sql_function: {e}")
            return False
