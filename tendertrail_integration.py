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
        Initialize the integration layer.
        
        Args:
            normalizer: The TenderNormalizer instance (LLM-based)
            preprocessor: The TenderPreprocessor instance 
            supabase_url: Supabase URL
            supabase_key: Supabase API key
        """
        self.normalizer = normalizer
        self.preprocessor = preprocessor
        
        # Initialize Supabase client
        if supabase_url and supabase_key:
            try:
                from supabase import create_client
                self.supabase = create_client(supabase_url, supabase_key)
                print("Successfully initialized Supabase client")
            except ImportError:
                print("Supabase client not found. Install with: pip install supabase")
                raise
            except Exception as e:
                print(f"Error initializing Supabase client: {e}")
                raise
        
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
        
        try:
            # Store the current source name for use in normalization
            self._current_source = source_name
            
            # Use the enhanced processing pipeline
            normalized_tenders = await self._enhanced_process_raw_tenders(tenders, source_name)
            processed_count = len(normalized_tenders)
            
            # Insert all normalized tenders into the database
            inserted_count = 0
            error_count = 0
            
            if normalized_tenders:
                # Insert the normalized tenders
                inserted_count = await self._insert_normalized_tenders(normalized_tenders, create_tables)
                print(f"Inserted {inserted_count} tenders from source: {source_name}")
                
                # Calculate error count
                error_count = processed_count - inserted_count
            
            # Clear the current source when done
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
    
    def _get_source_schema(self, source_name):
        """
        Get the schema for a source. 
        First tries to retrieve from database, then falls back to defaults.
        
        Args:
            source_name: Name of the source
            
        Returns:
            Dictionary representing the source schema
        """
        print(f"DEBUG: Calling _get_source_schema with source_name='{source_name}'")
        
        if not source_name:
            print("DEBUG: source_name is None or empty, using default schema")
            return self._get_default_source_schema()
            
        try:
            # Try to get schema from database
            response = self.supabase.table('source_schemas').select('schema').eq('name', source_name).execute()
            if hasattr(response, 'data') and response.data and len(response.data) > 0:
                schema = response.data[0].get('schema')
                if schema:
                    if isinstance(schema, str):
                        return json.loads(schema)
                    elif isinstance(schema, dict):
                        return schema
        except Exception as e:
            print(f"Error retrieving source schema from database: {e}")
            
        # Fallback to defaults based on source name
        print(f"Using default schema for source: {source_name}")
        
        # Common field mappings for default schema
        common_mappings = {
            "title": {"type": "string", "maps_to": "title"},
            "description": {"type": "string", "maps_to": "description"},
            "notice_title": {"type": "string", "maps_to": "title"},
            "notice_id": {"type": "string", "maps_to": "raw_id"},
            "source": {"type": "string", "maps_to": "source"},
            "date_published": {"type": "date", "maps_to": "date_published"},
            "closing_date": {"type": "date", "maps_to": "closing_date"},
            "tender_value": {"type": "monetary", "maps_to": "tender_value"},
            "currency": {"type": "string", "maps_to": "tender_currency"},
            "country": {"type": "string", "maps_to": "location"},
            "location": {"type": "string", "maps_to": "location"},
            "issuing_authority": {"type": "string", "maps_to": "issuing_authority"},
            "notice_type": {"type": "string", "maps_to": "tender_type"},
            "organization": {"type": "string", "maps_to": "issuing_authority"}
        }
        
        # Source-specific default schemas
        if source_name == "adb":
            schema = {
                "source_name": "adb",
                "fields": {
                    "title": common_mappings["title"],
                    "description": common_mappings["description"],
                    "published_date": {"type": "date", "maps_to": "date_published"},
                    "deadline": {"type": "date", "maps_to": "closing_date"},
                    "budget": {"type": "monetary", "maps_to": "tender_value"},
                    "location": common_mappings["location"],
                    "authority": {"type": "string", "maps_to": "issuing_authority"}
                },
                "language": "en"
            }
        elif source_name == "wb" or source_name == "worldbank":
            schema = {
                "source_name": "wb",
                "fields": {
                    "title": common_mappings["title"],
                    "description": common_mappings["description"],
                    "publication_date": {"type": "date", "maps_to": "date_published"},
                    "closing_date": {"type": "date", "maps_to": "closing_date"},
                    "value": {"type": "monetary", "maps_to": "tender_value"},
                    "country": {"type": "string", "maps_to": "location"},
                    "borrower": {"type": "string", "maps_to": "issuing_authority"}
                },
                "language": "en"
            }
        elif source_name == "ungm":
            schema = {
                "source_name": "ungm",
                "fields": {
                    "title": common_mappings["title"],
                    "description": common_mappings["description"],
                    "published": {"type": "date", "maps_to": "date_published"},
                    "deadline": {"type": "date", "maps_to": "closing_date"},
                    "value": {"type": "monetary", "maps_to": "tender_value"},
                    "country": {"type": "string", "maps_to": "location"},
                    "agency": {"type": "string", "maps_to": "issuing_authority"}
                },
                "language": "en"
            }
        elif source_name == "ted_eu":
            schema = {
                "source_name": "ted_eu",
                "fields": {
                    "title": common_mappings["title"],
                    "description": common_mappings["description"],
                    "publicationDate": {"type": "date", "maps_to": "date_published"},
                    "submissionDeadline": {"type": "date", "maps_to": "closing_date"},
                    "estimatedValue": {"type": "monetary", "maps_to": "tender_value"},
                    "country": {"type": "string", "maps_to": "location"},
                    "contractingAuthority": {"type": "string", "maps_to": "issuing_authority"},
                    "procedureType": {"type": "string", "maps_to": "tender_type"},
                    "cpvCodes": {"type": "array", "maps_to": "keywords"}
                },
                "language": "en"
            }
        elif source_name == "sam_gov":
            schema = {
                "source_name": "sam_gov",
                "fields": {
                    "title": common_mappings["title"],
                    "description": common_mappings["description"],
                    "posted_date": {"type": "date", "maps_to": "date_published"},
                    "response_deadline": {"type": "date", "maps_to": "closing_date"},
                    "estimated_value": {"type": "monetary", "maps_to": "tender_value"},
                    "place_of_performance": {"type": "string", "maps_to": "location"},
                    "agency": {"type": "string", "maps_to": "issuing_authority"},
                    "notice_type": {"type": "string", "maps_to": "tender_type"},
                    "solicitation_number": {"type": "string", "maps_to": "raw_id"}
                },
                "language": "en"
            }
        else:
            # Generic schema for any other source
            schema = {
                "source_name": source_name,
                "fields": {
                    "title": common_mappings["title"],
                    "description": common_mappings["description"],
                    "date_published": {"type": "date", "maps_to": "date_published"},
                    "closing_date": {"type": "date", "maps_to": "closing_date"},
                    "tender_value": {"type": "monetary", "maps_to": "tender_value"},
                    "location": {"type": "string", "maps_to": "location"},
                    "issuing_authority": {"type": "string", "maps_to": "issuing_authority"},
                    "notice_type": {"type": "string", "maps_to": "tender_type"},
                    "notice_id": {"type": "string", "maps_to": "raw_id"}
                },
                "language": "en"
            }
            
        return schema
    
    def _get_target_schema(self):
        """
        Get the target schema for normalization.
        First tries to retrieve from database, then falls back to defaults.
        
        Returns:
            Dictionary representing the target schema
        """
        try:
            # Try to get schema from database
            response = self.supabase.table('target_schema').select('schema').limit(1).execute()
            if hasattr(response, 'data') and response.data and len(response.data) > 0:
                schema = response.data[0].get('schema')
                if schema:
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
    
    async def _get_raw_tenders(self, source_name: str, batch_size: int) -> List[Dict[str, Any]]:
        """Get raw tenders from the database for a source."""
        try:
            # Use run_in_executor to run Supabase client calls asynchronously
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.supabase.table(source_name).select('*').order('created_at', desc=True).limit(batch_size).execute()
            )
            
            # Check if the response contains data
            if hasattr(response, 'data'):
                return response.data
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
                
            except Exception as e:
                print(f"Error processing raw tender item: {e}")
                # Add the raw item anyway, we'll try to handle it in process_source
                processed_tenders.append(item)
        
        return processed_tenders
    
    async def _insert_normalized_tenders(self, normalized_tenders: List[Dict[str, Any]], create_tables=True) -> int:
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
            
            # Try to load deep-translator if available
            translator = None
            try:
                from deep_translator import GoogleTranslator
                translator = GoogleTranslator(source='auto', target='en')
                print("Translation capability is available")
            except ImportError:
                print("deep-translator not available, text translation will be skipped")

            # Check if unified_tenders table exists and create it if needed
            if create_tables:
                await self._create_unified_tenders_table()
                
            # Check if metadata column exists
            metadata_column_exists = False
            try:
                # Perform a simple query that tries to access the metadata column
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.supabase.table('unified_tenders').select('metadata').limit(1).execute()
                )
                if hasattr(response, 'data'):
                    metadata_column_exists = True
                    print("Metadata column exists in unified_tenders table")
            except Exception as e:
                if "column" in str(e) and "does not exist" in str(e):
                    print("Metadata column does not exist in unified_tenders table")
                else:
                    print(f"Error checking metadata column: {e}")
            
            # Process each tender
            batch_size = 50
            for i in range(0, len(normalized_tenders), batch_size):
                batch = []
                
                # Process tenders in the current batch
                for tender in normalized_tenders[i:i+batch_size]:
                    try:
                        # Skip empty tenders
                        if not tender:
                            continue
                            
                        # Create cleaned tender with correct field names for the database
                        cleaned_tender = {}
                        metadata = tender.get("metadata", {}) if isinstance(tender.get("metadata"), dict) else {}
                        
                        # Map fields from normalized tender to database fields
                        for norm_field, db_field in field_mapping.items():
                            # Only map field if it exists in the tender
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
                                    
                                    if needs_translation:
                                        try:
                                            # Use run_in_executor to run translation in a thread
                                            loop = asyncio.get_event_loop()
                                            translated = await loop.run_in_executor(
                                                None,
                                                lambda: translator.translate(text)
                                            )
                                            print(f"Translated text from '{text[:30]}...' to '{translated[:30]}...'")
                                            cleaned_tender[db_field] = translated[:2000]
                                        except Exception as te:
                                            print(f"Translation error: {te}")
                                            cleaned_tender[db_field] = text[:2000]
                                    else:
                                        cleaned_tender[db_field] = text[:2000]
                                elif db_field == "contact_information":
                                    # Combine contact email and phone if both exist
                                    if norm_field == "contact_email" and "contact_email" in cleaned_tender:
                                        # Skip if we've already set this from email
                                        continue
                                    
                                    if norm_field == "contact_phone" and "contact_phone" in cleaned_tender:
                                        # If email exists, append phone
                                        if "contact_information" in cleaned_tender:
                                            cleaned_tender["contact_information"] += f", Phone: {tender[norm_field]}"
                                        else:
                                            cleaned_tender["contact_information"] = f"Phone: {tender[norm_field]}"
                                    elif norm_field == "contact_email":
                                        if "contact_information" in cleaned_tender:
                                            cleaned_tender["contact_information"] += f", Email: {tender[norm_field]}"
                                        else:
                                            cleaned_tender["contact_information"] = f"Email: {tender[norm_field]}"
                                    else:
                                        # Regular contact information
                                        cleaned_tender["contact_information"] = str(tender[norm_field])[:500]
                                elif db_field == "date_published" or db_field == "closing_date":
                                    # Ensure dates are in proper ISO format
                                    try:
                                        # Check if it's already a datetime or ISO date string
                                        if isinstance(tender[norm_field], (datetime.datetime, datetime.date)):
                                            cleaned_tender[db_field] = tender[norm_field].isoformat()[:10]
                                        else:
                                            # Try various date formats
                                            date_val = self._format_date(tender[norm_field])
                                            if date_val:
                                                cleaned_tender[db_field] = date_val
                                    except Exception as de:
                                        print(f"Date parsing error for {db_field}: {de}")
                                elif isinstance(tender[norm_field], (dict, list)):
                                    # Convert complex objects to JSON string
                                    if db_field == "keywords" and isinstance(tender[norm_field], list):
                                        # Join list of keywords with commas for better display
                                        cleaned_tender[db_field] = ", ".join(str(k) for k in tender[norm_field][:20])
                                    else:
                                        cleaned_tender[db_field] = json.dumps(tender[norm_field])[:2000]
                                else:
                                    cleaned_tender[db_field] = str(tender[norm_field])[:2000]
                        
                        # Ensure required fields exist with defaults
                        if "title" not in cleaned_tender or not cleaned_tender["title"]:
                            if "notice_title" in tender and tender["notice_title"]:
                                cleaned_tender["title"] = tender["notice_title"]
                            else:
                                cleaned_tender["title"] = f"Tender from {tender.get('source', 'Unknown')}"
                        
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
                                cleaned_tender["description"] = "No detailed information available"
                        
                        # Add raw_id if not present
                        if "raw_id" not in cleaned_tender:
                            # Try to get raw_id from tender
                            if "raw_id" in tender:
                                cleaned_tender["raw_id"] = str(tender["raw_id"])
                            elif "notice_id" in tender:
                                cleaned_tender["raw_id"] = str(tender["notice_id"])
                            else:
                                # Generate a unique ID
                                cleaned_tender["raw_id"] = str(uuid.uuid4())
                        
                        # Add processed_at if not present
                        if "processed_at" not in cleaned_tender:
                            cleaned_tender["processed_at"] = self._get_current_timestamp()
                        
                        # Add metadata column if it exists in the database and we have metadata
                        if metadata_column_exists and metadata:
                            cleaned_tender["metadata"] = json.dumps(metadata)
                            
                        # Add tender to batch
                        batch.append(cleaned_tender)
                    except Exception as e:
                        print(f"Error processing tender: {e}")
                
                if not batch:
                    continue
                    
                # Insert batch
                print(f"Inserting batch {i//batch_size + 1}/{ (len(normalized_tenders) + batch_size - 1) // batch_size}")
                
                try:
                    # Insert tender batch using run_in_executor
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: self.supabase.table('unified_tenders').insert(batch).execute()
                    )
                    
                    # Check response
                    if hasattr(response, 'data'):
                        inserted_count += len(response.data)
                    else:
                        print("Warning: No data returned from insert operation")
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                    # Continue with next batch
            
            return inserted_count
            
        except Exception as e:
            print(f"Error in _insert_normalized_tenders: {e}")
            return 0
    
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
        source_schema = self._get_source_schema(source_name)
        target_schema = self._get_target_schema()
        
        # Second pass to normalize and validate
        for tender in cleaned_data:
            try:
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
