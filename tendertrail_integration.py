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
        # Ensure required tables exist
        self._create_unified_tenders_table()
        self._create_errors_table()
        self._create_target_schema_table()
    
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
                # Make sure tender is a dict, not a string
                if isinstance(tender, str):
                    try:
                        tender = json.loads(tender)
                    except Exception as e:
                        raise ValueError(f"Tender is a string and cannot be parsed as JSON: {e}")
                
                # Ensure tender is a dictionary
                if not isinstance(tender, dict):
                    raise ValueError(f"Tender is not a dictionary, but a {type(tender)}")
                
                # Preprocess tender
                preprocessed_tender = self.preprocessor.preprocess(tender, source_schema)
                
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
                tender_id = None
                if isinstance(tender, dict):
                    tender_id = tender.get('id')
                elif isinstance(tender, str):
                    try:
                        parsed = json.loads(tender)
                        if isinstance(parsed, dict):
                            tender_id = parsed.get('id')
                    except:
                        pass
                
                if tender_id is None:
                    tender_id = processed_count
                
                errors.append({
                    'tender_id': str(tender_id),
                    'error': str(e),
                    'source': source_name
                })
                print(f"Error processing tender {tender_id}: {e}")
            
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
            # Try to use REST API first before falling back to RPC
            try:
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
                    self.supabase.rpc('exec_sql', {'sql': sql}).execute()
                except Exception as e:
                    print(f"RPC method failed, falling back to direct query: {e}")
                    # If RPC fails, try running the SQL through a custom function
                    self._run_sql_directly(sql)
                
                # Check if there's data in the table
                response = self.supabase.table('target_schema').select('*').execute()
                if not response.data:
                    # Insert default schema
                    default_schema = self._get_default_target_schema()
                    self.supabase.table('target_schema').insert({
                        'schema': default_schema
                    }).execute()
            except Exception as e:
                print(f"Error creating target_schema table: {e}")
        except Exception as e:
            print(f"Error in _create_target_schema_table: {e}")
    
    def _run_sql_directly(self, sql: str) -> None:
        """Run SQL directly using psycopg2 connection if available."""
        try:
            # This is a fallback method if RPC is not available
            # It will attempt to use psycopg2 directly if available
            import psycopg2
            from urllib.parse import urlparse
            
            # Parse Supabase URL to get connection details
            # Format is typically: https://[project-id].supabase.co
            parsed_url = urlparse(self.supabase._url)
            host = parsed_url.netloc.split('.')[0]
            
            # Connect directly to PostgreSQL
            conn = psycopg2.connect(
                host=f"{host}.supabase.co",
                database="postgres",
                user="postgres",
                password=self.supabase._key
            )
            
            # Execute SQL
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Failed to run SQL directly: {e}")
    
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
            # First, try the source_tenders table format
            table_name = f"{source_name}_tenders"
            try:
                response = self.supabase.table(table_name).select('*').limit(batch_size).execute()
                if response.data:
                    # Parse any string tenders into dictionaries
                    processed_tenders = []
                    for tender in response.data:
                        if isinstance(tender, str):
                            try:
                                tender = json.loads(tender)
                            except:
                                # Keep as is if parsing fails
                                pass
                        processed_tenders.append(tender)
                    return processed_tenders
            except Exception as e:
                print(f"Table {table_name} not found, trying direct source table {source_name}")
            
            # Try direct source name as table
            try:
                # First try with processed field
                try:
                    response = self.supabase.table(source_name).select('*').eq('processed', False).limit(batch_size).execute()
                    # Mark processed records
                    if response.data:
                        ids = [item['id'] for item in response.data if 'id' in item]
                        if ids:
                            self.supabase.table(source_name).update({'processed': True}).in_('id', ids).execute()
                        
                        # Parse any string tenders into dictionaries
                        processed_tenders = []
                        for tender in response.data:
                            if isinstance(tender, str):
                                try:
                                    tender = json.loads(tender)
                                except:
                                    # Keep as is if parsing fails
                                    pass
                            processed_tenders.append(tender)
                        return processed_tenders
                except Exception as e:
                    print(f"Error querying with processed field, trying without: {e}")
                    # Try without processed field
                    response = self.supabase.table(source_name).select('*').limit(batch_size).execute()
                    if response.data:
                        # Parse any string tenders into dictionaries
                        processed_tenders = []
                        for tender in response.data:
                            if isinstance(tender, str):
                                try:
                                    tender = json.loads(tender)
                                except:
                                    # Keep as is if parsing fails
                                    pass
                            processed_tenders.append(tender)
                        return processed_tenders
            except Exception as e:
                print(f"Error querying table {source_name}: {e}")
            
            # No valid data found
            return []
        except Exception as e:
            print(f"Error getting raw tenders: {e}")
            return []
    
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
                self.supabase.rpc('exec_sql', {'sql': sql}).execute()
            except Exception as e:
                print(f"RPC method failed, falling back to direct query: {e}")
                # If RPC fails, try running the SQL through a custom function
                self._run_sql_directly(sql)
        except Exception as e:
            print(f"Error creating unified_tenders table: {e}")
    
    def _log_errors(self, errors: List[Dict[str, Any]]) -> None:
        """Log processing errors to database."""
        try:
            # Create errors table if it doesn't exist
            try:
                self._create_errors_table()
            except Exception as e:
                print(f"Error creating normalization_errors table: {e}")
                
            response = self.supabase.table('normalization_errors').insert(errors).execute()
            return response.data
        except Exception as e:
            print(f"Error logging errors: {e}")
            return None
    
    def _create_errors_table(self):
        """Create normalization_errors table if it doesn't exist."""
        try:
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
                self.supabase.rpc('exec_sql', {'sql': sql}).execute()
            except Exception as e:
                print(f"RPC method failed, falling back to direct query: {e}")
                # If RPC fails, try running the SQL through a custom function
                self._run_sql_directly(sql)
        except Exception as e:
            print(f"Error creating normalization_errors table: {e}")
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.utcnow().isoformat()
