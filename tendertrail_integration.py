import json
import sys
import subprocess
from typing import Dict, Any, List, Optional
from supabase import create_client, Client

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
        
        # Ensure required tables exist
        self._create_unified_tenders_table()
        self._create_errors_table()
        self._create_target_schema_table()
    
    def process_source(self, source_name: str, batch_size: int = 100) -> Dict[str, Any]:
        """Process tenders from a specific source."""
        print(f"Starting processing for source: {source_name}")
        
        # Get source schema
        source_schema = self._get_source_schema(source_name)
        print(f"Retrieved source schema for {source_name}")
        
        # Get target schema
        target_schema = self._get_target_schema()
        print(f"Retrieved target schema")
        
        # Get raw tenders from source table
        print(f"Fetching raw tenders for {source_name}")
        raw_tenders = self._get_raw_tenders(source_name, batch_size)
        print(f"Found {len(raw_tenders)} raw tenders to process")
        
        # Process each tender
        processed_count = 0
        success_count = 0
        error_count = 0
        normalized_tenders = []
        errors = []
        
        for raw_tender in raw_tenders:
            try:
                # Deep copy to avoid modifying original data
                tender = self._ensure_dict(raw_tender)
                
                # Debug what we're getting
                print(f"Processing tender type: {type(tender)}, Sample: {str(tender)[:100]}")
                
                # Try direct normalization first based on source patterns
                normalized_tender = self._normalize_tender(tender, source_name, processed_count)
                
                # If direct normalization wasn't possible, go through standard process
                if normalized_tender is None:
                    # Preprocess tender - handle string case specifically before preprocessing
                    if isinstance(tender, str):
                        # Try to parse as JSON first
                        try:
                            parsed_tender = json.loads(tender)
                            if isinstance(parsed_tender, dict):
                                tender = parsed_tender
                            else:
                                # Create a simple wrapper for non-dict parsed data
                                tender = {"text": str(parsed_tender), "id": str(processed_count)}
                        except json.JSONDecodeError:
                            # Not valid JSON, create a text container
                            tender = {"text": tender, "id": str(processed_count)}
                    
                    # If we still don't have a dictionary, create one
                    if not isinstance(tender, dict):
                        tender = {"data": str(tender), "id": str(processed_count)}
                    
                    # Direct tender data preparation for sources with known structure
                    if source_name == "ungm" and "title" in tender:
                        # For UNGM, directly map the fields without preprocessing
                        preprocessed_tender = {
                            "title": tender.get("title", ""),
                            "description": tender.get("description", ""),
                            "published_on": tender.get("published_on", ""),
                            "deadline_on": tender.get("deadline_on", ""),
                            "beneficiary_countries": tender.get("beneficiary_countries", ""),
                            "language": "en",
                            "id": tender.get("id", processed_count)
                        }
                    elif source_name == "afdb" and "title" in tender:
                        # For AFDB, directly map the fields without preprocessing
                        preprocessed_tender = {
                            "title": tender.get("title", ""),
                            "description": tender.get("description", ""),
                            "publication_date": tender.get("publication_date", ""),
                            "closing_date": tender.get("closing_date", ""),
                            "estimated_value": tender.get("estimated_value", ""),
                            "country": tender.get("country", ""),
                            "language": "en",
                            "id": tender.get("id", processed_count)
                        }
                    elif source_name == "wb" and "title" in tender:
                        # For WB, directly map the fields without preprocessing
                        preprocessed_tender = {
                            "title": tender.get("title", ""),
                            "description": tender.get("description", ""),
                            "publication_date": tender.get("publication_date", ""),
                            "deadline": tender.get("deadline", ""),
                            "country": tender.get("country", ""),
                            "contact_organization": tender.get("contact_organization", ""),
                            "language": "en",
                            "id": tender.get("id", processed_count)
                        }
                    elif source_name == "afd" and "notice_title" in tender:
                        # For AFD, directly map the fields without preprocessing
                        preprocessed_tender = {
                            "title": tender.get("notice_title", ""),
                            "description": tender.get("notice_content", ""),
                            "publication_date": tender.get("publication_date", ""),
                            "country": tender.get("country", ""),
                            "notice_id": tender.get("notice_id", ""),
                            "language": "fr",
                            "id": tender.get("id", processed_count)
                        }
                    else:
                        # For other sources or if direct mapping failed, try preprocessing
                        try:
                            # Now preprocess with the proper dictionary
                            preprocessed_tender = self.preprocessor.preprocess(tender, source_schema)
                        except Exception as preprocess_error:
                            # If preprocessor fails, create a minimal dictionary with available fields
                            print(f"Preprocessor error: {preprocess_error}. Creating minimal tender dictionary.")
                            
                            # Try to extract basic fields that might be in the schema
                            preprocessed_tender = {}
                            for field in source_schema:
                                if isinstance(field, str) and field in tender and field != "source_name" and field != "language":
                                    preprocessed_tender[field] = tender[field]
                            
                            # Add required metadata
                            preprocessed_tender["id"] = tender.get("id", processed_count)
                            preprocessed_tender["language"] = source_schema.get("language", "en")
                    
                    # Normalize tender
                    normalized_tender = self.normalizer.normalize_tender(
                        preprocessed_tender, source_schema, target_schema
                    )
                    
                    # Add metadata
                    normalized_tender['source'] = source_name
                    # Ensure raw_id is a string
                    raw_id = tender.get('id')
                    normalized_tender['raw_id'] = str(raw_id) if raw_id is not None else str(processed_count)
                    normalized_tender['processed_at'] = self._get_current_timestamp()
                
                # Add to batch
                normalized_tenders.append(normalized_tender)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                # Safely extract tender_id for error logging
                tender_id = self._extract_tender_id(raw_tender, processed_count)
                
                errors.append({
                    'tender_id': str(tender_id),
                    'error': str(e),
                    'source': source_name
                })
                print(f"Error processing tender {tender_id}: {e}")
            
            processed_count += 1
            # Print progress every 10 tenders
            if processed_count % 10 == 0:
                print(f"Processed {processed_count}/{len(raw_tenders)} tenders, {success_count} successful, {error_count} errors")
        
        # Insert normalized tenders into unified table
        if normalized_tenders:
            print(f"Inserting {len(normalized_tenders)} normalized tenders")
            self._insert_normalized_tenders(normalized_tenders)
        else:
            print("No normalized tenders to insert")
        
        # Log errors
        if errors:
            print(f"Logging {len(errors)} errors")
            self._log_errors(errors)
        else:
            print("No errors to log")
        
        print(f"Completed processing for source {source_name}")
        print(f"Summary: Processed {processed_count}, Success {success_count}, Errors {error_count}")
        
        return {
            'source_name': source_name,
            'processed_count': processed_count,
            'success_count': success_count,
            'error_count': error_count
        }
    
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
            response = self.supabase.table('target_schema').select('*').execute()
            if response.data:
                schema_data = response.data[0]['schema']
                # Check if schema_data is already a dict (no need to parse)
                if isinstance(schema_data, dict):
                    return schema_data
                # If it's a string, try to parse it
                return json.loads(schema_data) if isinstance(schema_data, str) else schema_data
            else:
                return self._get_default_target_schema()
        except Exception as e:
            print(f"Error getting target schema: {e}")
            # Create target schema table if it doesn't exist
            self._create_target_schema_table()
            return self._get_default_target_schema()
    
    def _create_target_schema_table(self) -> None:
        """Create target_schema table if it doesn't exist and insert default schema."""
        try:
            # Check if table already exists first to avoid unnecessary SQL calls
            try:
                # Use the correct method to access the table
                response = self.supabase.table('target_schema').select('id').limit(1).execute()
                if hasattr(response, 'data'):
                    print("target_schema table already exists")
                    return
            except Exception as e:
                print(f"target_schema table check failed: {e}")
                # Table probably doesn't exist, proceed with creation
                pass
                
            # Try to use REST API first before falling back to RPC
            sql = """
            CREATE TABLE IF NOT EXISTS target_schema (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                schema JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
            """
            # Try direct SQL first through RPC
            try:
                print("Attempting to create target_schema table via RPC")
                self.supabase.rpc('exec_sql', {'sql': sql}).execute()
                print("Successfully created target_schema table via RPC")
            except Exception as e:
                print(f"RPC method failed, skipping table creation: {e}")
                # Don't attempt direct SQL to avoid hanging
            
            # Skip checking the table and inserting default schema to avoid further API calls
            print("Skipping target_schema population to avoid hanging")
        except Exception as e:
            print(f"Error in _create_target_schema_table: {e}")
            print("Continuing without table creation")
    
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
        try:
            # Ensure all fields are properly formatted
            for tender in normalized_tenders:
                for key, value in list(tender.items()):
                    if value is None:
                        tender[key] = ""
            
            # Create unified_tenders table if it doesn't exist
            try:
                self._create_unified_tenders_table()
            except Exception as e:
                print(f"Error creating unified_tenders table: {e}")
            
            response = self.supabase.table('unified_tenders').insert(normalized_tenders).execute()
            return response.data
        except Exception as e:
            print(f"Error inserting normalized tenders: {e}")
            return None
    
    def _create_unified_tenders_table(self):
        """Create unified_tenders table if it doesn't exist."""
        try:
            # Check if table already exists first to avoid unnecessary SQL calls
            try:
                # Use the correct method to access the table
                response = self.supabase.table('unified_tenders').select('id').limit(1).execute()
                if hasattr(response, 'data'):
                    print("unified_tenders table already exists")
                    return
            except Exception as e:
                print(f"unified_tenders table check failed: {e}")
                # Table probably doesn't exist, proceed with creation
                pass
                
            sql = """
            CREATE TABLE IF NOT EXISTS unified_tenders (
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
            # Try direct SQL first through RPC
            try:
                print("Attempting to create unified_tenders table via RPC")
                self.supabase.rpc('exec_sql', {'sql': sql}).execute()
                print("Successfully created unified_tenders table via RPC")
            except Exception as e:
                print(f"RPC method failed, skipping table creation: {e}")
                # Don't attempt direct SQL to avoid hanging
        except Exception as e:
            print(f"Error creating unified_tenders table: {e}")
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
            # Check if table already exists first to avoid unnecessary SQL calls
            try:
                # Use the correct method to access the table
                response = self.supabase.table('normalization_errors').select('id').limit(1).execute()
                if hasattr(response, 'data'):
                    print("normalization_errors table already exists")
                    return
            except Exception as e:
                print(f"normalization_errors table check failed: {e}")
                # Table probably doesn't exist, proceed with creation
                pass
                
            sql = """
            CREATE TABLE IF NOT EXISTS normalization_errors (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tender_id TEXT,
                error TEXT,
                source TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
            """
            # Try direct SQL first through RPC
            try:
                print("Attempting to create normalization_errors table via RPC")
                self.supabase.rpc('exec_sql', {'sql': sql}).execute()
                print("Successfully created normalization_errors table via RPC")
            except Exception as e:
                print(f"RPC method failed, skipping table creation: {e}")
                # Don't attempt direct SQL to avoid hanging
        except Exception as e:
            print(f"Error creating normalization_errors table: {e}")
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

    def _normalize_tender(self, tender, source_name, processed_count):
        """Normalize a tender based on source format without using preprocessor or normalizer"""
        try:
            if source_name == "ungm" and "notice_title" in tender:
                return {
                    "title": tender.get("notice_title", ""),
                    "description": tender.get("notice_content", ""),
                    "date_published": tender.get("published_dt", ""),
                    "location": tender.get("country", ""),
                    "issuing_authority": "United Nations Global Marketplace",
                    "source": source_name,
                    "raw_id": str(tender.get("id", processed_count)),
                    "notice_id": tender.get("reference", ""),
                    "deadline": tender.get("deadline_dt", ""),
                    "processed_at": self._get_current_timestamp()
                }
            elif source_name == "afdb" and "notice_title" in tender:
                return {
                    "title": tender.get("notice_title", ""),
                    "description": tender.get("notice_content", ""),
                    "date_published": tender.get("publication_date", ""),
                    "location": tender.get("country", ""),
                    "issuing_authority": "African Development Bank",
                    "source": source_name,
                    "raw_id": str(tender.get("id", processed_count)),
                    "notice_id": tender.get("reference", ""),
                    "processed_at": self._get_current_timestamp()
                }
            elif source_name == "wb" and "notice_title" in tender:
                return {
                    "title": tender.get("notice_title", ""),
                    "description": tender.get("notice_content", ""),
                    "date_published": tender.get("publication_date", ""),
                    "location": tender.get("country", ""),
                    "issuing_authority": "World Bank",
                    "source": source_name,
                    "raw_id": str(tender.get("id", processed_count)),
                    "notice_id": tender.get("notice_id", ""),
                    "processed_at": self._get_current_timestamp()
                }
            elif source_name == "afd" and "notice_title" in tender:
                return {
                    "title": tender.get("notice_title", ""),
                    "description": tender.get("notice_content", ""),
                    "date_published": tender.get("publication_date", ""),
                    "location": tender.get("country", ""),
                    "issuing_authority": "Agence Française de Développement",
                    "source": source_name,
                    "raw_id": str(tender.get("id", processed_count)),
                    "notice_id": tender.get("notice_id", ""),
                    "processed_at": self._get_current_timestamp()
                }
            elif source_name == "sam_gov" and "opportunity_id" in tender:
                return {
                    "title": tender.get("opportunity_title", ""),
                    "description": tender.get("description", ""),
                    "date_published": tender.get("posted_date", ""),
                    "location": tender.get("place_of_performance", ""),
                    "issuing_authority": "System for Award Management (SAM.gov)",
                    "source": source_name,
                    "raw_id": str(tender.get("opportunity_id", "")),
                    "notice_id": tender.get("solicitation_number", ""),
                    "deadline": tender.get("response_deadline", ""),
                    "processed_at": self._get_current_timestamp()
                }
            elif source_name == "ted_eu" and "publication_number" in tender:
                return {
                    "title": tender.get("title", ""),
                    "description": tender.get("short_description", ""),
                    "date_published": tender.get("dispatch_date", ""),
                    "location": tender.get("nutscode", ""),
                    "issuing_authority": tender.get("authority_name", "European Union"),
                    "source": source_name,
                    "raw_id": str(tender.get("id", processed_count)),
                    "notice_id": tender.get("publication_number", ""),
                    "deadline": tender.get("deadline", ""),
                    "processed_at": self._get_current_timestamp()
                }
            
            # For other sources, try the standard approach via preprocessor and normalizer
            return None
        except Exception as e:
            self.logger.error(f"Error in direct normalization: {str(e)}")
            return None
