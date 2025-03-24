-- Setup for TenderTrail Normalizer Database

-- Create tables for each source
CREATE TABLE IF NOT EXISTS adb_tenders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT,
    description TEXT,
    published_date DATE,
    deadline DATE,
    budget TEXT,
    location TEXT,
    authority TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wb_tenders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT,
    description TEXT,
    publication_date DATE,
    closing_date DATE,
    value TEXT,
    country TEXT,
    borrower TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ungm_tenders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT,
    description TEXT,
    published DATE,
    deadline DATE,
    value TEXT,
    country TEXT,
    agency TEXT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Create unified tenders table for normalized data
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
);

-- Create normalization errors table
CREATE TABLE IF NOT EXISTS normalization_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tender_id TEXT,
    error TEXT,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Create source schemas table
CREATE TABLE IF NOT EXISTS source_schemas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    schema JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Create target schema table
CREATE TABLE IF NOT EXISTS target_schema (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Helper function to get tables with a specific suffix
CREATE OR REPLACE FUNCTION public.get_tables_with_suffix(suffix text)
RETURNS SETOF text AS $$
BEGIN
  RETURN QUERY
  SELECT table_name::text
  FROM information_schema.tables
  WHERE table_schema = 'public'
    AND table_name LIKE '%' || suffix
    AND table_type = 'BASE TABLE';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Insert default source schemas
INSERT INTO source_schemas (name, schema)
VALUES
('adb', '{
    "source_name": "adb",
    "title": {"type": "string", "maps_to": "title"},
    "description": {"type": "string", "maps_to": "description"},
    "published_date": {"type": "date", "maps_to": "date_published"},
    "deadline": {"type": "date", "maps_to": "closing_date"},
    "budget": {"type": "monetary", "maps_to": "tender_value"},
    "location": {"type": "string", "maps_to": "location"},
    "authority": {"type": "string", "maps_to": "issuing_authority"},
    "language": "en"
}'::jsonb),
('wb', '{
    "source_name": "wb",
    "title": {"type": "string", "maps_to": "title"},
    "description": {"type": "string", "maps_to": "description"},
    "publication_date": {"type": "date", "maps_to": "date_published"},
    "closing_date": {"type": "date", "maps_to": "closing_date"},
    "value": {"type": "monetary", "maps_to": "tender_value"},
    "country": {"type": "string", "maps_to": "location"},
    "borrower": {"type": "string", "maps_to": "issuing_authority"},
    "language": "en"
}'::jsonb),
('ungm', '{
    "source_name": "ungm",
    "title": {"type": "string", "maps_to": "title"},
    "description": {"type": "string", "maps_to": "description"},
    "published": {"type": "date", "maps_to": "date_published"},
    "deadline": {"type": "date", "maps_to": "closing_date"},
    "value": {"type": "monetary", "maps_to": "tender_value"},
    "country": {"type": "string", "maps_to": "location"},
    "agency": {"type": "string", "maps_to": "issuing_authority"},
    "language": "en"
}'::jsonb)
ON CONFLICT (name) DO UPDATE 
SET schema = EXCLUDED.schema, updated_at = now();

-- Insert target schema
INSERT INTO target_schema (schema)
VALUES ('{
    "title": {
        "type": "string",
        "description": "Title of the tender",
        "format": "Title case, max 200 characters"
    },
    "description": {
        "type": "string",
        "description": "Detailed description of the tender",
        "format": "Plain text, max 2000 characters",
        "requires_translation": true
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
}'::jsonb)
ON CONFLICT ON CONSTRAINT target_schema_pkey 
DO UPDATE SET schema = EXCLUDED.schema, updated_at = now();

-- Sample data for testing
INSERT INTO adb_tenders (title, description, published_date, deadline, budget, location, authority)
VALUES 
('Sample ADB Tender', 'This is a sample tender for testing', '2025-03-01', '2025-06-30', '1,000,000 USD', 'Manila, Philippines', 'Asian Development Bank');

INSERT INTO wb_tenders (title, description, publication_date, closing_date, value, country, borrower)
VALUES 
('Sample World Bank Tender', 'This is a sample tender for testing', '2025-03-02', '2025-06-25', '500,000 EUR', 'Kenya', 'Government of Kenya');

INSERT INTO ungm_tenders (title, description, published, deadline, value, country, agency)
VALUES 
('Sample UN Tender', 'This is a sample tender for testing', '2025-03-03', '2025-07-01', '750,000 USD', 'New York, USA', 'UNDP'); 