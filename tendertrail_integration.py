import json
import sys
import subprocess
from typing import Dict, Any, List, Optional, Tuple, Union
from supabase import create_client, Client
import uuid
import traceback
import re
import datetime
import asyncio
from datetime import timedelta

class TenderTrailIntegration:
    """Integration layer for TenderTrail normalization workflow."""
    
    def __init__(self, normalizer=None, preprocessor=None, supabase_url=None, supabase_key=None):
        """
        Initialize the TenderTrailIntegration class.
        
        Args:
            normalizer: Instance of TenderNormalizer
            preprocessor: Instance of TenderPreprocessor
            supabase_url: URL for Supabase instance
            supabase_key: API key for Supabase instance
        """
        self.normalizer = normalizer
        self.preprocessor = preprocessor
        
        # Initialize Supabase client
        if supabase_url and supabase_key:
            try:
                from supabase import create_client
                self.supabase = create_client(supabase_url, supabase_key) # Moved inside try
                print("Successfully initialized Supabase client") # Moved inside try
            except ImportError: # Correctly aligned with try
                print("Supabase client library not found. Run: pip install supabase")
                print("Disabling Supabase functionality.")
                self.supabase = None # Set to None on import error
            except Exception as e: # Correctly aligned with try
                print(f"Error initializing Supabase client: {e}")
                print("Disabling Supabase functionality.")
                self.supabase = None # Set to None on other init errors
        else:
            print("Supabase URL or key not provided. Disabling Supabase functionality.")
            self.supabase = None # Ensure supabase is set to None if not initialized
        
        # Initialize translation cache
        self.translation_cache = {}
        
        # Initialize schema cache
        self.target_schema = None
    
    async def process_source(self, tenders_or_source, source_name_or_batch_size=None, create_tables=True):
        """
        Process tenders from a source, normalize and insert them into the database.
        This method is overloaded to handle two different call patterns:
        
        1. process_source(tenders, source_name, create_tables=True) - Process a list of tenders
        2. process_source(source_name, batch_size=100, create_tables=True) - Load tenders from database
        
        The method automatically detects which pattern is being used based on the types of arguments.
        
        Returns:
            Tuple (processed_count, error_count)
        """
        # Detect which call pattern is being used
        if isinstance(tenders_or_source, (list, tuple)) or (isinstance(tenders_or_source, str) and source_name_or_batch_size is not None and not isinstance(source_name_or_batch_size, int)):
            # First pattern: process_source(tenders, source_name)
            tenders = tenders_or_source
            source_name = source_name_or_batch_size
            batch_size = None
            print(f"DEBUG: Using first pattern - direct tenders list with source_name='{source_name}'")
        else:
            # Second pattern: process_source(source_name, batch_size)
            source_name = tenders_or_source
            batch_size = 100  # Default
            if source_name_or_batch_size is not None and isinstance(source_name_or_batch_size, int):
                batch_size = source_name_or_batch_size
            print(f"DEBUG: Using second pattern - source_name='{source_name}' with batch_size={batch_size}")
            # Get tenders from database
            tenders = await self._get_raw_tenders(source_name, batch_size)
        
        print(f"Processing {len(tenders) if isinstance(tenders, (list, tuple)) else 'unknown number of'} tenders from source: {source_name}")
        
        processed_count = 0
        error_count = 0
        inserted_count = 0
        normalized_tenders = []
        
        try:
            # Store the current source name for use in normalization
            self._current_source = source_name
            
            # Use the enhanced processing pipeline
            normalized_tenders = await self._enhanced_process_raw_tenders(tenders, source_name)
            processed_count = len(normalized_tenders)
            
            # Insert all normalized tenders into the database
            if normalized_tenders:
                inserted_count = await self._insert_normalized_tenders(normalized_tenders, create_tables)
                print(f"Inserted {inserted_count} tenders from source: {source_name}")
                
                # Calculate error count based on insertion success
                error_count = processed_count - inserted_count
            else:
                print(f"No tenders were successfully normalized for source: {source_name}")
                error_count = len(tenders) # All original tenders failed if none were normalized
                
        # Correctly aligned and structured except block
        except Exception as e: 
            print(f"Error processing source {source_name}: {e}") 
            traceback.print_exc()
            error_count = len(tenders) # Assume all failed if main processing block crashed
            processed_count = 0
            inserted_count = 0
            
        finally:
            # Clear the current source when done
            self._current_source = None
            
        return processed_count, error_count
    
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
                
        except Exception as e: # Corrected indentation
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
    
    async def _get_source_schema(self, source_name=None):
        """
        Get the schema for a source from the database or return a default schema.
        
        Args:
            source_name: The name of the source
            
        Returns:
            The schema for the source
        """
        if not source_name:
            print("No source name provided, using default schema")
            return self._get_default_source_schema(None)
        
        # Try to get the schema from the database
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.supabase.table('source_schemas')
                          .select('schema')
                          .eq('name', source_name)
                          .execute()
            )
            
            if result and hasattr(result, 'data') and result.data and len(result.data) > 0:
                print(f"DEBUG: Found schema for '{source_name}' in database.")
                db_schema = result.data[0]['schema']
                
                # Wrap the schema in a 'fields' key for compatibility with TenderPreprocessor
                # The TenderPreprocessor expects: {'fields': {field1: {...}, field2: {...}, ...}}
                schema = {'fields': db_schema}
                return schema
        except Exception as e:
            print(f"Error getting schema for '{source_name}' from database: {e}")
        
        # If we get here, either the database query failed or no schema was found
        print(f"No schema found for '{source_name}', using default schema")
        # Correctly indented return statement
        return self._get_default_source_schema(source_name) 
    
    async def _get_target_schema(self):
        """
        Get the target schema for normalization.
        First tries to retrieve from database, then falls back to defaults.
        
        Returns:
            Dictionary representing the target schema
        """
        loop = asyncio.get_event_loop()
        
        try:
            # Try to get schema from database using run_in_executor
            response = await loop.run_in_executor(
                None,
                lambda: self.supabase.table('target_schema').select('schema').limit(1).execute()
            )
            
            if hasattr(response, 'data') and response.data and len(response.data) > 0:
                schema = response.data[0].get('schema')
                if schema:
                    print("DEBUG: Found target schema in database")
                    if isinstance(schema, str):
                        return json.loads(schema)
                    elif isinstance(schema, dict):
                        return schema
        except Exception as e:
            print(f"Error retrieving target schema from database: {e}")
    
        # Fallback to default schema
        print("Using default target schema")
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
            "tender_type": {
                "type": "string",
                "description": "Type of tender",
                "format": "One of: Goods, Works, Services, Consulting",
                "extract_from": {
                    "field": "description"
                }
            },
            "raw_id": {
                "type": "string",
                "description": "Original ID from the source system",
                "format": "As provided by source"
            },
            "source": {
                "type": "string",
                "description": "Source of the tender",
                "format": "Short code for the source (e.g., adb, wb, ted_eu)"
            },
            "language": "en"
        }
    
    async def _create_target_schema_table(self) -> None:
        """Create target_schema table if it doesn't exist and insert default schema."""
        loop = asyncio.get_event_loop()
        
        try: # Outer try
            # Check if table already exists using simple query
            try: # Inner try block
                # Try direct query using run_in_executor
                response = await loop.run_in_executor(
                    None,
                    lambda: self.supabase.table('target_schema').select('id').limit(1).execute()
                )
                
                if hasattr(response, 'data'):
                    print("target_schema table already exists")
                    
                    # If the table exists but is empty, try to populate it
                    if not response.data:
                        try: # Innermost try block
                            print("Adding default schema to empty target_schema table")
                            default_schema = self._get_default_target_schema()
                            
                            # Insert using run_in_executor
                            await loop.run_in_executor(
                                None,
                                lambda: self.supabase.table('target_schema').insert({
                                    'schema': default_schema
                                }).execute()
                            )
                            
                            print("Successfully added default schema to target_schema")
                        except Exception as e: # Matches innermost try
                            print(f"Error adding default schema: {e}")
                    
                    return # Exit if table exists (and potentially populated)
                
            # <<< ADDED except block for the inner try >>>
            except Exception as inner_e:
                # If the error indicates the table doesn't exist, log it nicely
                if "relation" in str(inner_e).lower() and "does not exist" in str(inner_e).lower():
                    print(f"target_schema table check confirms: table does not exist.")
                else:
                    # Log other errors encountered during the check
                    print(f"Error during target_schema existence check: {inner_e}")
                # Allow execution to continue to the manual creation info block

            # If check failed or table doesn't exist, inform user about manual creation
            print("Cannot create target_schema table directly via client library.")
            print("Please ensure the table exists or create it using the Supabase UI or SQL Editor with this schema:")
            print("""
            CREATE TABLE IF NOT EXISTS public.target_schema (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                schema JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            );
            """)
            
            # We'll continue with the in-memory default schema if creation/check fails
            print("Using in-memory default schema as fallback.")
            
        # This except matches the OUTER try
        except Exception as general_e:
            print(f"General error in _create_target_schema_table: {general_e}")
            print("Continuing with in-memory schema as fallback.")
    
    async def _get_raw_tenders(self, source_name: str, batch_size: int) -> List[Dict[str, Any]]:
        """Get raw tenders from the database for a source."""
        try:
            print(f"DEBUG: Fetching tenders from source table: {source_name}")
            
            # Use run_in_executor to run Supabase client calls asynchronously
            loop = asyncio.get_event_loop()
            
            # Use a basic query without depending on created_at
            # This matches the original implementation
            response = await loop.run_in_executor(
                None,
                lambda: self.supabase.table(source_name).select('*').limit(batch_size).execute()
            )
            
            # Check if the response contains data
            if hasattr(response, 'data'):
                raw_tenders = response.data
                print(f"DEBUG: Fetched {len(raw_tenders)} tenders from {source_name}")
                
                # Basic data validation and cleaning for robustness
                processed_tenders = []
                for item in raw_tenders:
                    try:
                        # If it's already a dict, use it
                        if isinstance(item, dict):
                            # Ensure source is set
                            if 'source' not in item:
                                item['source'] = source_name
                            processed_tenders.append(item)
                            continue
                        
                        # If it's a string, try to parse it as JSON
                        if isinstance(item, str):
                            try:
                                parsed = json.loads(item)
                                if isinstance(parsed, dict):
                                    if 'source' not in parsed:
                                        parsed['source'] = source_name
                                    processed_tenders.append(parsed)
                                    continue
                            except json.JSONDecodeError:
                                # Not valid JSON, wrap it
                                processed_tenders.append({
                                    'content': item,
                                    'source': source_name,
                                    'id': str(uuid.uuid4())
                                })
                                continue
                        
                        # If it has a get method (like a Record object)
                        if hasattr(item, 'get') and callable(item.get):
                            # Create a dictionary from the object
                            dict_item = {}
                            for key in ['id', 'title', 'description', 'data', 'content', 'body', 'source']:
                                try:
                                    value = item.get(key)
                                    if value is not None:
                                        dict_item[key] = value
                                except:
                                    pass
                            
                            # Ensure source is set
                            if 'source' not in dict_item:
                                dict_item['source'] = source_name
                                
                            # If we extracted at least something, use it
                            if dict_item:
                                processed_tenders.append(dict_item)
                                continue
                        
                        # Last resort - wrap in a minimal dict
                        processed_tenders.append({
                            'data': str(item),
                            'source': source_name,
                            'id': str(uuid.uuid4())
                        })
                        
                    except Exception as item_e:
                        print(f"Error processing tender item: {item_e}")
                        # Still include it as a wrapped error item for visibility
                        processed_tenders.append({
                            'error': str(item_e),
                            'source': source_name,
                            'id': str(uuid.uuid4())
                        })
                
                return processed_tenders
            else:
                print(f"No data found for source {source_name}")
                return []
        except Exception as e:
            print(f"Error getting raw tenders from database: {e}")
            print(f"Table {source_name} may not exist or may not be accessible")
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
                
            except Exception as e: # This except corresponds to the try block starting the loop iteration
                print(f"Error processing raw tender item: {e}")
                # Add the raw item anyway, we'll try to handle it in process_source
                processed_tenders.append(item)
        
        return processed_tenders
    
    async def _insert_normalized_tenders(self, normalized_tenders: List[Dict[str, Any]], create_tables=True) -> int:
        """Insert normalized tenders into unified table and return count of successful insertions."""
        if not normalized_tenders:
            print("No tenders to insert")
            return 0
        
        inserted_count = 0
        tenders_to_insert = [] # Renamed from batch for clarity before the loop

        try:
            print(f"Preparing to insert {len(normalized_tenders)} tenders into unified_tenders")

            # Ensure necessary tables exist (or log if they don't)
            if create_tables:
                await self._create_unified_tenders_table()
                await self._create_errors_table() # <<< ADDED CALL HERE

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
                "tender_value": "tender_value",
                "currency": "tender_currency",
                "contact_email": "contact_information",
                "contact_phone": "contact_information",
                "contact_information": "contact_information",
                "cpvs": "keywords",  # Store CPVs as keywords
                "url": "url",  # Add URL field if exists in db
                "buyer": "buyer",  # Add buyer field if exists in db
                "raw_id": "raw_id",
                "notice_id": "raw_id"  # Use notice_id as fallback for raw_id
            }

            translator = None
            try:
                from deep_translator import GoogleTranslator
                translator = GoogleTranslator(source='auto', target='en')
                print("Translation capability is available")
            except ImportError:
                print("deep-translator not available, text translation will be skipped")

            metadata_column_exists = False
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.supabase.table('unified_tenders').select('metadata').limit(1).execute()
                )
                if hasattr(response, 'data'): # Simple check if query succeeded at all
                    metadata_column_exists = True
                    print("Metadata column assumed to exist in unified_tenders table after successful check.")
                # No explicit else, as failure might be due to table not existing yet
            except Exception as e:
                if "column" in str(e).lower() and "does not exist" in str(e).lower():
                    print("Metadata column does not exist in unified_tenders table.")
                elif "relation" in str(e).lower() and "does not exist" in str(e).lower():
                    print("'unified_tenders' table likely doesn't exist yet.") # Handle case where table check fails because table is missing
                else:
                    print(f"Error checking metadata column: {e}")


            # Process each tender in batches
            batch_size = 50
            for i in range(0, len(normalized_tenders), batch_size):
                current_batch_data = [] # Data for Supabase upsert

                # Process tenders in the current sub-batch
                sub_batch = normalized_tenders[i:i+batch_size]
                print(f"Processing batch {i//batch_size + 1}: {len(sub_batch)} tenders")

                for tender in sub_batch:
                    try:
                        # Skip empty tenders
                        if not tender or not isinstance(tender, dict):
                            print(f"Skipping invalid tender data: {type(tender)}")
                            continue

                        cleaned_tender = {}
                        metadata = tender.get("metadata", {}) if isinstance(tender.get("metadata"), dict) else {}

                        # --- Start Restored Tender Processing Logic --- 
                        # Map fields from normalized tender to database fields
                        for norm_field, db_field in field_mapping.items():
                            if norm_field in tender and tender[norm_field] is not None and tender[norm_field] != "":
                                # Handle translation for specific text fields
                                if db_field in ["title", "description"] and translator and isinstance(tender[norm_field], str):
                                    text_to_process = tender[norm_field]
                                    try:
                                        # Simple check for non-English chars (can be improved)
                                        needs_translation = any(ord(c) > 127 for c in text_to_process)
                                        translated_text = text_to_process # Default to original
                                        
                                        if needs_translation:
                                            # Check cache first
                                            if text_to_process in self.translation_cache:
                                                translated_text = self.translation_cache[text_to_process]
                                                print(f"Cache hit for translation: '{text_to_process[:30]}...'")
                                            else:
                                                # Translate using run_in_executor
                                                loop = asyncio.get_event_loop()
                                                print(f"Translating text: '{text_to_process[:30]}...'")
                                                translated_text = await loop.run_in_executor(
                                                    None,
                                                    lambda: translator.translate(text_to_process)
                                                )
                                                # Cache the result
                                                if translated_text:
                                                    self.translation_cache[text_to_process] = translated_text
                                                print(f"Translated text to: '{translated_text[:30]}...'")
                                        
                                        cleaned_tender[db_field] = translated_text[:2000] # Limit length
                                    except Exception as te:
                                        print(f"Translation error for '{text_to_process[:30]}...': {te}")
                                        cleaned_tender[db_field] = text_to_process[:2000] # Use original on error
                                    else:
                                        cleaned_tender[db_field] = text_to_process[:2000] # Non-translatable or already English
                                
                                # Handle combined contact information
                                elif db_field == "contact_information":
                                    current_contact = cleaned_tender.get(db_field, "")
                                    new_info = str(tender[norm_field])[:500]
                                    if norm_field == "contact_email":
                                        new_info = f"Email: {new_info}"
                                    elif norm_field == "contact_phone":
                                         new_info = f"Phone: {new_info}"
                                    # Append new info if current info exists
                                    if current_contact:
                                        cleaned_tender[db_field] = f"{current_contact}, {new_info}"
                                    else:
                                         cleaned_tender[db_field] = new_info
                                
                                # Handle date fields
                                elif db_field in ["date_published", "closing_date"]:
                                    iso_date = self._parse_date(tender[norm_field]) # Use helper method
                                    if iso_date:
                                        cleaned_tender[db_field] = iso_date
                                    else:
                                        print(f"Could not parse date for {db_field}: {tender[norm_field]}")
                                        
                                # Handle complex types (dict/list -> JSON string), ensure keywords are joined
                                elif isinstance(tender[norm_field], (dict, list)):
                                    if db_field == "keywords" and isinstance(tender[norm_field], list):
                                        # Join list of keywords with commas, limit items and length
                                        kw_str = ", ".join(str(k)[:50] for k in tender[norm_field][:20])
                                        cleaned_tender[db_field] = kw_str[:1000]
                                    else:
                                        try:
                                            cleaned_tender[db_field] = json.dumps(tender[norm_field])[:2000] # Limit length
                                        except TypeError as json_e:
                                             print(f"Error serializing field {db_field} to JSON: {json_e}")
                                             cleaned_tender[db_field] = str(tender[norm_field])[:2000] # Fallback to string
                                else:
                                    # Default: convert to string and limit length
                                    cleaned_tender[db_field] = str(tender[norm_field])[:2000]

                        # Ensure required fields have defaults
                        if not cleaned_tender.get("title"):
                            cleaned_tender["title"] = f"Tender from {tender.get('source', 'Unknown')}"
                        if not cleaned_tender.get("source"):
                            cleaned_tender["source"] = self._current_source or "Unknown"
                        if not cleaned_tender.get("description"):
                            cleaned_tender["description"] = "No detailed description available."
                        if not cleaned_tender.get("raw_id"):
                            cleaned_tender["raw_id"] = tender.get("id", str(uuid.uuid4()))
                            
                        # Add processed_at timestamp
                        cleaned_tender["processed_at"] = self._get_current_timestamp()

                        # Add metadata if column exists and data is present
                        if metadata_column_exists and metadata:
                            try:
                                cleaned_tender['metadata'] = json.dumps(metadata)
                            except TypeError as json_meta_e:
                                print(f"Error serializing metadata to JSON: {json_meta_e}")
                                cleaned_tender['metadata'] = json.dumps(str(metadata)) # Fallback
                        # --- End Restored Tender Processing Logic --- 

                        # Add the fully processed tender to the list for insertion
                        if cleaned_tender: # Ensure we didn't add empty dicts
                            current_batch_data.append(cleaned_tender)

                    except Exception as tender_proc_e:
                        print(f"CRITICAL Error processing tender {tender.get('id', 'N/A')} for insertion: {tender_proc_e}")
                        traceback.print_exc()
                        # Log this specific error to the errors table
                        try:
                            error_payload = {
                                "source": self._current_source or tender.get('source', "unknown"),
                                "error_message": f"Tender processing failed: {tender_proc_e}",
                                "tender_data": json.dumps(tender, default=str), # Log original tender
                                "context": "Individual tender processing failure"
                            }
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None,
                                lambda: self.supabase.table('errors').insert(error_payload).execute()
                             )
                        except Exception as log_proc_err_e:
                            print(f"Failed to log tender processing error to 'errors' table: {log_proc_err_e}")

                # Insert the prepared batch into the database
                if current_batch_data:
                    print(f"Attempting to upsert batch of {len(current_batch_data)} tenders...")
                    try:
                        print(f"DEBUG: Sample data for batch upsert: {str(current_batch_data[0])[:500]}...")
                    except Exception as log_e:
                        print(f"DEBUG: Error logging sample batch data: {log_e}")

                    try:
                        loop = asyncio.get_event_loop()
                        # Use upsert with source and raw_id as conflict identifiers
                        response = await loop.run_in_executor(
                            None,
                            lambda: self.supabase.table('unified_tenders')
                                        .upsert(current_batch_data, on_conflict='source,raw_id')
                                        .execute()
                        )
                        if hasattr(response, 'data') and response.data:
                           print(f"Successfully upserted batch. Response count: {len(response.data)}")
                           inserted_count += len(response.data)
                        elif hasattr(response, 'status_code') and 200 <= response.status_code < 300:
                            # Sometimes upsert might return success status without data array
                            print(f"Successfully upserted batch (status code: {response.status_code}). Assuming count: {len(current_batch_data)}")
                            inserted_count += len(current_batch_data) # Assume all succeeded if status is ok
                        else:
                           print(f"Upsert batch completed but response indicates potential issue or no data returned. Response: {response}")
                           # Log the failed batch to the errors table for review
                           # (Code for logging already exists below)

                    except Exception as db_e:
                        print(f"DATABASE UPSERT ERROR for batch: {db_e}")
                        traceback.print_exc()
                        # Log the entire batch that failed
                        try:
                            error_payload = {
                                "source": self._current_source or "unknown", 
                                "error_message": str(db_e),
                                "tender_data": json.dumps(current_batch_data, default=str), 
                                "context": "Batch upsert failure"
                            }
                            await loop.run_in_executor(
                                None,
                                lambda: self.supabase.table('errors').insert(error_payload).execute()
                             )
                            print("Logged batch upsert error to 'errors' table.")
                        except Exception as log_err_e:
                            print(f"Failed to log batch upsert error to 'errors' table: {log_err_e}")

        # Outer exception handler for the whole insertion process
        except Exception as e:
            print(f"CRITICAL Error during overall tender insertion process: {e}")
            traceback.print_exc()

        print(f"Total successfully upserted/inserted tenders in this run: {inserted_count}")
        return inserted_count

    async def _create_unified_tenders_table(self) -> None:
        """Create unified_tenders table if it doesn't exist with all required columns."""
        try:
            # Check if table already exists
            table_exists = False
            try:
                # Try direct query to see if table exists
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.supabase.table('unified_tenders').select('id').limit(1).execute()
                )
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

    async def _create_errors_table(self) -> None:
        """Create the 'errors' table if it doesn't exist."""
        loop = asyncio.get_event_loop()
        table_name = 'errors'
        try: # Outer try for the whole operation (Line 874)
            # Check if table exists
            try: # Inner try for the check query
                response = await loop.run_in_executor(
                    None,
                    lambda: self.supabase.table(table_name).select('id', count='exact').limit(1).execute()
                )
                if response.count is not None:
                     print(f"'{table_name}' table already exists.")
                     return # Table exists, nothing more to do
            except Exception as e:
                 # Handle errors during the check phase
                 if "relation" in str(e).lower() and "does not exist" in str(e).lower():
                     print(f"'{table_name}' table does not exist. Will proceed to inform user for manual creation.")
                 else:
                     print(f"Error checking '{table_name}' existence: {e}")
                     # Depending on error, may want to raise or return here instead of proceeding

            # If code reaches here, table either doesn't exist or the check failed.
            # Inform user about manual creation as client libs typically can't CREATE TABLE.
            print(f"Cannot create '{table_name}' table directly via client library.")
            print("Please ensure the table exists or create it using the Supabase UI or SQL Editor.")
            print("Recommended schema:")
            # Correctly formatted triple-quoted string
            print("""
            CREATE TABLE IF NOT EXISTS public.errors (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                source TEXT,
                error_message TEXT,
                tender_data JSONB,
                context TEXT
            );
            """)
            # Consider raising an exception or returning a status if table is essential

        # <<< Correctly aligned except block for the outer try (Line 874) >>>
        except Exception as general_e:
            print(f"General error during '{table_name}' table check/creation info: {general_e}")

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

    async def _enhanced_process_raw_tenders(self, raw_data, source_name=None):
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
                # Skip empty items
                if not item:
                    continue
                    
                # If it's just a string ID, try to extract structured data
                if isinstance(item, str) and len(item.strip()) < 50:
                    structured_data = await self._extract_structured_data(item, source_name)
                    if structured_data:
                        cleaned_data.append(structured_data)
                    else:
                        print(f"Unable to extract structured data from string: {item}")
                        continue
                        
                # If it's a dictionary, use it directly
                elif isinstance(item, dict):
                    # Clean HTML if present in description-like fields
                    for field in ['description', 'body', 'content', 'text', 'details']:
                        if field in item and item[field] and isinstance(item[field], str):
                            item[field] = self._clean_html(item[field])
                            
                    # Make sure we have a source
                    if 'source' not in item:
                        item['source'] = source_name
                        
                    cleaned_data.append(item)
                    
                # For other types, try to convert to structured data
                else:
                    structured_data = await self._extract_structured_data(item, source_name)
                    if structured_data:
                        cleaned_data.append(structured_data)
                    else:
                        print(f"Unable to extract structured data from item of type {type(item)}")
                        continue
                        
            except Exception as e:
                print(f"Error cleaning tender: {e}")
                error_tenders += 1
                
        # Get schemas
        source_schema = await self._get_source_schema(source_name)
        target_schema = await self._get_target_schema()
        
        # Second pass to normalize and validate
        for tender in cleaned_data:
            try:
                # Debug info for tender type
                print(f"DEBUG: Processing tender of type {type(tender)}")
                
                # Ensure tender is a dictionary
                if not isinstance(tender, dict):
                    print(f"WARNING: Expected dict but got {type(tender)}: {str(tender)[:100]}")
                    tender = self._ensure_dict(tender)
                    print(f"DEBUG: Converted to dict: {str(tender)[:100]}")
                
                # Preprocess the tender using the preprocessor if available
                preprocessed_tender = None
                if hasattr(self, 'preprocessor') and self.preprocessor:
                    try:
                        # Pass both tender and source_schema
                        preprocessed_tender = self.preprocessor.preprocess(tender, source_schema)
                        if preprocessed_tender:
                            # Add source name if missing
                            if 'source' not in preprocessed_tender:
                                preprocessed_tender['source'] = source_name
                    except Exception as preproc_e:
                        print(f"Error during preprocessing: {preproc_e}")
                        # Continue with original tender
                        preprocessed_tender = None
                
                # Use the preprocessed tender if available, otherwise use the original
                tender_to_normalize = preprocessed_tender if preprocessed_tender else tender
                
                # Debug info for tender_to_normalize
                print(f"DEBUG: Tender to normalize - Type: {type(tender_to_normalize)}")
                
                # Try to use the LLM normalizer if available
                normalized_tender = None
                if hasattr(self, 'normalizer') and self.normalizer:
                    try:
                        # Use run_in_executor to run synchronous normalize_tender in a thread pool
                        loop = asyncio.get_event_loop()
                        normalized_tender = await loop.run_in_executor(
                            None,
                            lambda: self.normalizer.normalize_tender(
                                tender_to_normalize, 
                                source_schema, 
                                target_schema
                            )
                        )
                        
                        # Ensure required fields from the integration perspective
                        if normalized_tender:
                            # Add source name if missing
                            if 'source' not in normalized_tender:
                                normalized_tender['source'] = source_name
                                
                            # Map field names to match our expected schema
                            # (Since LLM might return fields like 'title' instead of 'notice_title')
                            field_mapping = {
                                'title': 'notice_title',
                                'description': 'description',
                                'date_published': 'date_published',
                                'closing_date': 'closing_date',
                                'tender_value': 'tender_value',
                                'tender_currency': 'currency',
                                'location': 'location',
                                'issuing_authority': 'issuing_authority'
                            }
                            
                            for llm_field, int_field in field_mapping.items():
                                if llm_field in normalized_tender and int_field not in normalized_tender:
                                    normalized_tender[int_field] = normalized_tender[llm_field]
                    except Exception as llm_e:
                        print(f"Error during LLM normalization: {llm_e}")
                        normalized_tender = None
                
                # Fallback to rule-based normalization if LLM failed
                if not normalized_tender:
                    print("Falling back to rule-based normalization")
                    normalized_tender = self._normalize_tender(tender_to_normalize, source_name)
                
                if not normalized_tender:
                    skipped_tenders += 1
                    continue
                    
                # Check if this might be a duplicate of something we already processed
                if self._detect_potential_duplicate(normalized_tender, processed_tenders):
                    print(f"Skipping potential duplicate: {normalized_tender.get('notice_title', '')[:50]}...")
                    skipped_tenders += 1
                    continue
                    
                # Validate and clean the tender data
                is_valid, validation_message = self._validate_normalized_tender(normalized_tender)
                
                if is_valid:
                    # Extract address info if available
                    if 'description' in normalized_tender and normalized_tender['description']:
                        address_info = self._extract_address_information(normalized_tender['description'])
                        if address_info:
                            # Add to metadata
                            if not isinstance(normalized_tender.get('metadata'), dict):
                                normalized_tender['metadata'] = {}
                            normalized_tender['metadata']['address_info'] = address_info
                    
                    processed_tenders.append(normalized_tender)
                else:
                    print(f"Validation failed: {validation_message}")
                    skipped_tenders += 1
                    
            except Exception as e:
                print(f"Error during tender normalization: {e}")
                error_tenders += 1
                
        print(f"Enhanced processing results: {len(processed_tenders)} valid tenders, {skipped_tenders} skipped, {error_tenders} errors")
        return processed_tenders

    async def _extract_structured_data(self, content, source):
        """
        Extract structured data from various content formats.

        Args:
            content: Content to extract data from
            source: Source name for context

        Returns:
            Dictionary of structured data or None if extraction fails significantly
        """
        loop = asyncio.get_event_loop() # Get event loop for run_in_executor if needed
        try:
            # Print some debug info to see what we're working with
            content_preview = str(content)[:100] if isinstance(content, (str, bytes)) else type(content).__name__
            print(f"Extracting data from content type {type(content)}: {content_preview}...")

            # --- Handle String Content ---
            if isinstance(content, str):
                content_strip = content.strip()

                # If it's a short string, potentially an ID
                if 0 < len(content_strip) < 50 and content_strip.isalnum():
                    print(f"Content is short, trying to use it as an ID: {content_strip}")
                    try:
                        response = await loop.run_in_executor(
                            None,
                            lambda: self.supabase.table(source).select('*').eq('id', content_strip).limit(1).execute()
                        )
                        if hasattr(response, 'data') and response.data:
                            print(f"Found tender by ID {content_strip}")
                            fetched_data = response.data[0]
                            if isinstance(fetched_data, dict):
                                fetched_data['source'] = source # Ensure source is set
                                return fetched_data
                            else:
                                print(f"Fetched data for ID {content_strip} is not a dict.")
                        else:
                            print(f"No tender found for ID {content_strip}")
                    except Exception as e:
                        print(f"Failed to fetch tender by ID '{content_strip}': {e}")
                    # Fall through to treat as text if ID fetch fails or returns nothing

                # Try to identify XML
                if content_strip.startswith('<?xml') or (content_strip.startswith('<') and content_strip.endswith('>')):
                    import xml.etree.ElementTree as ET
                    try:
                        root = ET.fromstring(content_strip)
                        xml_dict = self._xml_to_dict(root)
                        xml_dict['source'] = source
                        print("Parsed content as XML")
                        return xml_dict
                    except Exception as xml_e:
                        print(f"XML parsing failed (will treat as text): {xml_e}") # Don't stop, treat as text

                # Try to identify HTML
                if '<html' in content.lower() or '<body' in content.lower():
                    try:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(content, 'html.parser')
                        title = soup.title.string.strip() if soup.title and soup.title.string else f"HTML Content from {source}"
                        # Try to get meaningful body text
                        body_text = ""
                        main_tags = soup.find_all(['main', 'article', 'div'], {'class': ['content', 'main', 'body', 'article']})
                        if main_tags:
                            body_text = " ".join(tag.get_text(" ", strip=True) for tag in main_tags)
                        if not body_text and soup.body:
                            body_text = soup.body.get_text(" ", strip=True)
                        if not body_text:
                             body_text = soup.get_text(" ", strip=True) # Fallback to all text

                        print("Parsed content as HTML")
                        return {
                            'title': title,
                            'description': body_text[:5000], # Limit length
                            'source': source,
                            'raw_data_type': 'html'
                        }
                    except ImportError:
                        print("BeautifulSoup not installed, using basic HTML cleaning.")
                        # Basic cleaning is likely already done, treat as text
                    except Exception as html_e:
                        print(f"HTML parsing failed (will treat as text): {html_e}") # Don't stop, treat as text


                # Try parsing as JSON (if it looks like it)
                if (content_strip.startswith('{') and content_strip.endswith('}')) or \
                   (content_strip.startswith('[') and content_strip.endswith(']')):
                    try:
                        parsed = json.loads(content_strip)
                        if isinstance(parsed, dict):
                            parsed['source'] = source
                            print("Parsed content as JSON object")
                            return parsed
                        elif isinstance(parsed, list) and parsed:
                             # If list of dicts, maybe take the first? Or try to merge? For now, wrap it.
                             print("Parsed content as JSON list, wrapping.")
                             return {'title': f"List data from {source}", 'data_list': parsed, 'source': source, 'raw_data_type': 'json_list'}
                        # else: Fall through if empty list or non-dict/list JSON

                    except json.JSONDecodeError:
                        print("Content looks like JSON but failed to parse (will treat as text).")


                # If none of the above, treat as plain text
                print("Treating content as plain text.")
                return {
                    'title': f"Tender Text from {source}",
                    'description': content_strip[:5000], # Limit length
                    'source': source,
                    'raw_data_type': 'text'
                }

            # --- Handle Dict Content ---
            elif isinstance(content, dict):
                # Already a dictionary, just ensure source is set
                if 'source' not in content:
                    content['source'] = source
                print("Content is already a dictionary.")
                return content

            # --- Handle List Content ---
            elif isinstance(content, list):
                print("Content is a list.")
                if len(content) == 1 and isinstance(content[0], dict):
                     print("Using first item from list as it's a dict.")
                     item_dict = content[0]
                     if 'source' not in item_dict: item_dict['source'] = source
                     return item_dict
                elif content:
                     print("Wrapping list content.")
                     return {'title': f"List data from {source}", 'data_list': content, 'source': source, 'raw_data_type': 'list'}
                else:
                     print("Content is an empty list.")
                     return {'title': f"Empty List from {source}", 'source': source, 'description': ''} # Return minimal valid dict

            # --- Handle Other Types ---
            else:
                print(f"Content is an unsupported type: {type(content)}. Converting to string.")
                return {
                    'title': f"Data from {source}",
                    'description': str(content)[:5000],
                    'source': source,
                    'raw_data_type': type(content).__name__
                }

        except Exception as e:
            print(f"Error in _extract_structured_data: {e}")
            # Return a minimal structure indicating error
            return {
                'title': f"Error Processing Tender from {source}",
                'description': f"Failed to process content. Error: {e}",
                'source': source,
                'processing_error': True
            }

    def _xml_to_dict(self, element):
        """ Converts an XML element and its children into a dictionary. """
        d = {element.tag: {} if element.attrib else None}
        children = list(element)
        if children:
            dd = {}
            for dc in map(self._xml_to_dict, children):
                for k, v in dc.items():
                    if k in dd:
                        if not isinstance(dd[k], list):
                            dd[k] = [dd[k]]
                        dd[k].append(v)
                    else:
                        dd[k] = v
            d = {element.tag: dd}
        if element.attrib:
            d[element.tag].update(('@' + k, v) for k, v in element.attrib.items())
        if element.text:
            text = element.text.strip()
            if children or element.attrib:
                if text:
                  d[element.tag]['#text'] = text
            else:
                d[element.tag] = text
        return d

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content to extract plain text."""
        if not html_content:
            return ""
            
        # Try to use BeautifulSoup if available
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script_or_style in soup(["script", "style"]):
                script_or_style.extract()
                
            # Get text and clean whitespace
            text = soup.get_text(separator=" ", strip=True)
            
            # Clean up whitespace issues
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
        except ImportError:
            print("BeautifulSoup not available, using basic HTML cleaning")
            
        # Basic fallback cleaning if BeautifulSoup is not available
        import re
        
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # Remove extra whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        # Replace HTML entities
        entities = {
            '&nbsp;': ' ', '&lt;': '<', '&gt;': '>', '&amp;': '&',
            '&quot;': '"', '&apos;': "'", '&ndash;': '-', '&mdash;': '-',
            '&lsquo;': "'", '&rsquo;': "'", '&ldquo;': '"', '&rdquo;': '"'
        }
        for entity, replacement in entities.items():
            clean_text = clean_text.replace(entity, replacement)
            
        return clean_text

    def _normalize_tender(self, tender, source_name=None):
        """
        Rule-based tender normalization (fallback when LLM normalization fails).
        
        Args:
            tender: Dictionary containing tender data
            source_name: Name of the source (used for source-specific logic)
            
        Returns:
            Dictionary with normalized tender data or None if normalization fails
        """
        try:
            # Skip empty tenders
            if not tender:
                return None
                
            # Ensure tender is a dictionary
            if not isinstance(tender, dict):
                print(f"Cannot normalize non-dictionary tender: {type(tender)}")
                return None
                
            # Create a copy to avoid modifying the original
            normalized = {}
            
            # Get source name if not provided
            if not source_name:
                source_name = tender.get('source', self._current_source)
                if not source_name:
                    source_name = "unknown"
            
            # Add source to normalized data
            normalized['source'] = source_name
            
            # Map common fields
            field_mapping = {
                # Title
                'title': 'notice_title',
                'name': 'notice_title',
                'subject': 'notice_title',
                'noticeTitle': 'notice_title',
                'tender_title': 'notice_title',
                
                # Description
                'description': 'description',
                'details': 'description',
                'summary': 'description',
                'noticeDescription': 'description',
                'text': 'description',
                'content': 'description',
                'body': 'description',
                
                # Date Published
                'date_published': 'date_published',
                'datePublished': 'date_published',
                'publicationDate': 'date_published',
                'published': 'date_published',
                'publishedDate': 'date_published',
                'created_at': 'date_published',
                'createdAt': 'date_published',
                'publication_date': 'date_published',
                
                # Closing Date
                'closing_date': 'closing_date',
                'closeDate': 'closing_date',
                'deadline': 'closing_date',
                'deadlineDate': 'closing_date',
                'submissionDeadline': 'closing_date',
                'expiryDate': 'closing_date',
                'expiry_date': 'closing_date',
                'end_date': 'closing_date',
                'endDate': 'closing_date',
                
                # Tender Value
                'tender_value': 'tender_value',
                'value': 'tender_value',
                'amount': 'tender_value',
                'budget': 'tender_value',
                'estimatedValue': 'tender_value',
                'estimated_value': 'tender_value',
                'contractValue': 'tender_value',
                'contract_value': 'tender_value',
                
                # Currency
                'currency': 'currency',
                'currencyCode': 'currency',
                'currency_code': 'currency',
                
                # Location
                'location': 'location',
                'country': 'country',
                'region': 'location',
                'place': 'location',
                'placeOfPerformance': 'location',
                'place_of_performance': 'location',
                
                # Issuing Authority
                'issuing_authority': 'issuing_authority',
                'issuingAuthority': 'issuing_authority',
                'buyer': 'issuing_authority',
                'agency': 'issuing_authority',
                'organization': 'issuing_authority',
                'authority': 'issuing_authority',
                'contractingAuthority': 'issuing_authority',
                'contracting_authority': 'issuing_authority',
                'procuring_entity': 'issuing_authority',
                'procuringEntity': 'issuing_authority',
                
                # Tender Type
                'tender_type': 'notice_type',
                'type': 'notice_type',
                'noticeType': 'notice_type',
                'notice_type': 'notice_type',
                'procedureType': 'notice_type',
                'procedure_type': 'notice_type',
                
                # Notice ID / Reference
                'notice_id': 'notice_id',
                'id': 'notice_id',
                'reference': 'notice_id',
                'referenceNumber': 'notice_id',
                'reference_number': 'notice_id',
                'tenderReference': 'notice_id',
                'tender_reference': 'notice_id',
                
                # URL
                'url': 'url',
                'link': 'url',
                'tender_url': 'url',
                'tenderUrl': 'url',
                'noticeUrl': 'url',
                'notice_url': 'url',
                
                # Contact Information
                'contact': 'contact_information',
                'contactPerson': 'contact_information',
                'contact_person': 'contact_information',
                'contactEmail': 'contact_email',
                'contact_email': 'contact_email',
                'contactPhone': 'contact_phone',
                'contact_phone': 'contact_phone'
            }
            
            # Map fields from tender to normalized tender
            for source_field, target_field in field_mapping.items():
                if source_field in tender and tender[source_field] is not None:
                    # Clean text fields
                    if source_field in ['description', 'details', 'summary', 'text', 'content', 'body']:
                        # Format description
                        if isinstance(tender[source_field], str):
                            normalized[target_field] = self._clean_html(tender[source_field])
                    elif source_field in ['date_published', 'datePublished', 'publicationDate', 'published', 
                                        'publishedDate', 'created_at', 'createdAt', 'publication_date',
                                        'closing_date', 'closeDate', 'deadline', 'deadlineDate', 
                                        'submissionDeadline', 'expiryDate', 'expiry_date', 'end_date', 'endDate']:
                        # Parse dates
                        date_value = self._parse_date(tender[source_field])
                        if date_value:
                            normalized[target_field] = date_value
                    else:
                        # Copy other fields directly
                        normalized[target_field] = tender[source_field]
            
            # Try to extract raw_id if not already set
            if 'raw_id' not in normalized and 'notice_id' in normalized:
                normalized['raw_id'] = normalized['notice_id']
            elif 'raw_id' not in normalized and 'id' in tender:
                normalized['raw_id'] = tender['id']
                
            # Handle special case for title/name
            if 'notice_title' not in normalized:
                for title_field in ['heading', 'header', 'noticeHeader', 'notice_header', 'label']:
                    if title_field in tender and tender[title_field]:
                        normalized['notice_title'] = tender[title_field]
                        break
                        
            # Generate a generic title if none exists
            if 'notice_title' not in normalized or not normalized['notice_title']:
                normalized['notice_title'] = f"Tender from {source_name}"
                
            # Normalize dates
            for date_field in ['date_published', 'closing_date']:
                if date_field in normalized and not self._is_valid_date_format(normalized[date_field]):
                    parsed_date = self._parse_date(normalized[date_field])
                    if parsed_date:
                        normalized[date_field] = parsed_date
                    else:
                        normalized.pop(date_field, None)
                        
            # Extract tender value and currency if combined
            if 'tender_value' in normalized and isinstance(normalized['tender_value'], str):
                import re
                # Look for currency codes or symbols in the value
                value_str = normalized['tender_value']
                currency_pattern = r'([A-Z]{3}|\$|€|£|¥)'
                matches = re.findall(currency_pattern, value_str)
                if matches:
                    # Extract the first currency match
                    currency = matches[0]
                    # Convert symbols to codes
                    currency_map = {'$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY'}
                    if currency in currency_map:
                        currency = currency_map[currency]
                    normalized['currency'] = currency
                    
                    # Extract numeric value
                    numeric_part = re.sub(r'[^\d.]', '', value_str)
                    if numeric_part:
                        normalized['tender_value'] = numeric_part.strip()
                        
            # Clean up whitespace in text fields
            for field in ['notice_title', 'description', 'issuing_authority', 'location']:
                if field in normalized and isinstance(normalized[field], str):
                    normalized[field] = normalized[field].strip()
                    
            # Source-specific normalization
            if source_name == 'ungm':
                # UNGM-specific field mapping
                if 'deadline' in tender and not 'closing_date' in normalized:
                    normalized['closing_date'] = self._parse_date(tender['deadline'])
                if 'agency' in tender and not 'issuing_authority' in normalized:
                    normalized['issuing_authority'] = tender['agency']
                    
            elif source_name == 'ted_eu':
                # TED EU-specific field mapping
                if 'cpvs' in tender and not 'keywords' in normalized:
                    normalized['keywords'] = tender['cpvs']
                    
            elif source_name == 'wb' or source_name == 'worldbank':
                # World Bank specific field mapping
                if 'borrower' in tender and not 'issuing_authority' in normalized:
                    normalized['issuing_authority'] = tender['borrower']
                    
            return normalized
                
        except Exception as e:
            print(f"Error in rule-based normalization: {e}")
            traceback.print_exc()
            return None

    def _detect_potential_duplicate(self, tender, existing_tenders):
        """
        Detect if a tender is potentially a duplicate of existing tenders.
        Uses multiple heuristics to improve accuracy.
        
        Args:
            tender: The new tender to check
            existing_tenders: List of already processed tenders to check against
            
        Returns:
            bool: True if potential duplicate found, False otherwise
        """
        print(f"DEBUG: Checking for duplicates for tender: {str(tender.get('notice_title', ''))[:50]}")
        
        # If no title or ID, can't do duplicate detection
        if not tender.get('notice_title') and not tender.get('notice_id') and not tender.get('raw_id'):
            print("DEBUG: Can't check for duplicates - no title or ID")
            return False
            
        # Check by ID first
        tender_id = tender.get('notice_id') or tender.get('raw_id')
        if tender_id:
            for existing in existing_tenders:
                existing_id = existing.get('notice_id') or existing.get('raw_id')
                if existing_id and existing_id == tender_id:
                    print(f"DEBUG: Duplicate detected by ID: {tender_id}")
                    return True
                    
        # Check by title if available
        title = tender.get('notice_title')
        if title:
            # Skip generic titles like "Tender from source"
            if re.match(r'^Tender from', title):
                # If title is generic, need more evidence to detect duplicate
                location_match = False
                date_match = False
                
                # Try to match by location and date
                for existing in existing_tenders:
                    # Check location match
                    if (tender.get('location') and existing.get('location') and 
                        tender['location'] == existing['location']):
                        location_match = True
                        
                    # Check date match
                    if (tender.get('date_published') and existing.get('date_published') and 
                        tender['date_published'] == existing['date_published']):
                        date_match = True
                        
                    # If both location and date match, it's likely a duplicate
                    if location_match and date_match:
                        print("DEBUG: Generic title but location and date match - likely duplicate")
                        return True
                        
                # Otherwise, it's probably a different tender
                print("DEBUG: Generic title but not enough evidence for duplicate")
                return False
            
            # Normal title comparison
            for existing in existing_tenders:
                existing_title = existing.get('notice_title', '')
                if not existing_title:
                    continue
                    
                # Exact title match
                if title == existing_title:
                    print(f"DEBUG: Duplicate detected by exact title match: {title[:50]}")
                    return True
                    
                # Check for significant title similarity
                # Calculate title similarity ratio
                similarity = self._calculate_similarity(title, existing_title)
                if similarity > 0.85:  # High similarity threshold
                    print(f"DEBUG: Duplicate detected by title similarity ({similarity:.2f}): {title[:50]}")
                    return True
                    
        return False
        
    def _calculate_similarity(self, str1, str2):
        """Calculate string similarity using difflib."""
        import difflib
        return difflib.SequenceMatcher(None, str1, str2).ratio()

    def _validate_normalized_tender(self, tender):
        """
        Validate a normalized tender for completeness and correctness.
        
        Args:
            tender: Dictionary containing normalized tender data
            
        Returns:
            Tuple (is_valid, message) where:
                is_valid: Boolean indicating if the tender is valid
                message: Validation message or error
        """
        # Check if tender is a dictionary
        if not isinstance(tender, dict):
            return False, f"Tender is not a dictionary (type: {type(tender)})"
            
        # Check for required fields
        required_fields = ['notice_title', 'source']
        for field in required_fields:
            if field not in tender or not tender[field]:
                return False, f"Missing required field: {field}"
                
        # Check date formats if they exist
        for date_field in ['date_published', 'closing_date']:
            if date_field in tender and tender[date_field]:
                if not self._is_valid_date_format(tender[date_field]):
                    parsed_date = self._parse_date(tender[date_field])
                    if parsed_date:
                        tender[date_field] = parsed_date
                    else:
                        return False, f"Invalid date format for {date_field}: {tender[date_field]}"
                        
        # Check description length if it exists
        if 'description' in tender and tender['description']:
            if len(tender['description']) < 10:
                return False, f"Description too short: {tender['description']}"
                
        # Check title length
        if len(tender['notice_title']) < 5:
            return False, f"Title too short: {tender['notice_title']}"
            
        # Add basic metadata if missing
        if 'metadata' not in tender:
            tender['metadata'] = {
                'normalized_at': self._get_current_timestamp(),
                'normalization_method': 'rule_based'
            }
        elif not isinstance(tender['metadata'], dict):
            tender['metadata'] = {
                'normalized_at': self._get_current_timestamp(),
                'normalization_method': 'rule_based',
                'original_metadata': str(tender['metadata'])
            }
            
        return True, "Tender is valid"
        
    def _format_date(self, date_input):
        """Format a date input to ISO format (YYYY-MM-DD)."""
        if not date_input:
            return None
            
        # If already a valid ISO format, return as is
        if self._is_valid_date_format(date_input):
            return date_input
            
        # Use _parse_date for other formats
        return self._parse_date(date_input)

    def _get_default_source_schema(self, source_name=None):
        """
        Get a default schema for a source when no schema is found in the database.
        
        Args:
            source_name: The name of the source
            
        Returns:
            A default schema for the source
        """
        # Common field mappings for default schema
        common_mappings = {
            "title": {"type": "string", "maps_to": "title"},
            "description": {"type": "string", "maps_to": "description"},
            "notice_title": {"type": "string", "maps_to": "title"},
            "notice_id": {"type": "string", "maps_to": "raw_id"},
            "source": {"type": "string", "maps_to": "source"},
            "date_published": {"type": "date", "maps_to": "date_published"},
            "publication_date": {"type": "date", "maps_to": "date_published"},
            "closing_date": {"type": "date", "maps_to": "closing_date"},
            "deadline": {"type": "date", "maps_to": "closing_date"},
            "due_date": {"type": "date", "maps_to": "closing_date"},
            "tender_value": {"type": "monetary", "maps_to": "tender_value"},
            "currency": {"type": "string", "maps_to": "tender_currency"},
            "country": {"type": "string", "maps_to": "location"},
            "location": {"type": "string", "maps_to": "location"},
            "issuing_authority": {"type": "string", "maps_to": "issuing_authority"},
            "notice_type": {"type": "string", "maps_to": "tender_type"},
            "tender_type": {"type": "string", "maps_to": "tender_type"},
            "organization": {"type": "string", "maps_to": "issuing_authority"}
        }
        
        # Source-specific default schemas
        if source_name == "adb":
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "published_date": {"type": "date", "maps_to": "date_published"},
                "deadline": {"type": "date", "maps_to": "closing_date"},
                "budget": {"type": "monetary", "maps_to": "tender_value"},
                "location": common_mappings["location"],
                "authority": {"type": "string", "maps_to": "issuing_authority"},
                "notice_title": common_mappings["notice_title"],
                "publication_date": common_mappings["publication_date"],
                "due_date": common_mappings["due_date"]
            }
        elif source_name == "wb" or source_name == "worldbank":
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "publication_date": {"type": "date", "maps_to": "date_published"},
                "closing_date": {"type": "date", "maps_to": "closing_date"},
                "value": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "borrower": {"type": "string", "maps_to": "issuing_authority"}
            }
        elif source_name == "ungm":
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "published": {"type": "date", "maps_to": "date_published"},
                "deadline": {"type": "date", "maps_to": "closing_date"},
                "value": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "agency": {"type": "string", "maps_to": "issuing_authority"}
            }
        elif source_name == "ted_eu":
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "publicationDate": {"type": "date", "maps_to": "date_published"},
                "submissionDeadline": {"type": "date", "maps_to": "closing_date"},
                "estimatedValue": {"type": "monetary", "maps_to": "tender_value"},
                "country": {"type": "string", "maps_to": "location"},
                "contractingAuthority": {"type": "string", "maps_to": "issuing_authority"},
                "procedureType": {"type": "string", "maps_to": "tender_type"},
                "cpvCodes": {"type": "array", "maps_to": "keywords"}
            }
        elif source_name == "sam_gov":
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "posted_date": {"type": "date", "maps_to": "date_published"},
                "response_deadline": {"type": "date", "maps_to": "closing_date"},
                "estimated_value": {"type": "monetary", "maps_to": "tender_value"},
                "place_of_performance": {"type": "string", "maps_to": "location"},
                "agency": {"type": "string", "maps_to": "issuing_authority"},
                "notice_type": {"type": "string", "maps_to": "tender_type"},
                "solicitation_number": {"type": "string", "maps_to": "raw_id"}
            }
        elif source_name == "afdb":
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "publication_date": common_mappings["publication_date"],
                "closing_date": common_mappings["closing_date"],
                "estimated_value": {"type": "monetary", "maps_to": "tender_value"},
                "currency": common_mappings["currency"],
                "country": common_mappings["country"],
                "tender_type": common_mappings["tender_type"],
                "sector": {"type": "string", "maps_to": "sector"}
            }
        else:
            # Generic schema for any other source
            fields = {
                "title": common_mappings["title"],
                "description": common_mappings["description"],
                "date_published": {"type": "date", "maps_to": "date_published"},
                "publication_date": {"type": "date", "maps_to": "date_published"},
                "closing_date": {"type": "date", "maps_to": "closing_date"},
                "tender_value": {"type": "monetary", "maps_to": "tender_value"},
                "location": {"type": "string", "maps_to": "location"},
                "country": {"type": "string", "maps_to": "location"},
                "issuing_authority": {"type": "string", "maps_to": "issuing_authority"},
                "notice_type": {"type": "string", "maps_to": "tender_type"},
                "tender_type": {"type": "string", "maps_to": "tender_type"},
                "notice_id": {"type": "string", "maps_to": "raw_id"}
            }
        
        # Return schema with proper structure
        schema = {
            "source_name": source_name or "generic",
            "language": "en",
            "fields": fields
        }
        
        return schema
