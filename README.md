# PyNormalizer3: Tender Data Normalization Framework

PyNormalizer3 is a robust data normalization framework designed for procurement and tender data from various international sources, including AFD, SAM.gov, UNGM, TED EU, and others. It transforms heterogeneous tender data into a unified, standardized format for consistent analysis and processing.

## Key Features

- **Source-Specific Normalization**: Specialized handling for multiple tender sources with source-specific field mappings
- **Robust Error Handling**: Comprehensive error tracking and reporting system
- **Database Integration**: Built-in support for Supabase with automatic table creation
- **Translation Capabilities**: Automatic detection and translation of non-English content using Google Translate API
- **Metadata Preservation**: Capture of source-specific fields that don't map to the standard schema
- **Data Validation**: Ensures data consistency and required fields with sensible defaults
- **Flexible Input Handling**: Support for various input formats (JSON, dictionaries, strings)
- **Batch Processing**: Efficient handling of large datasets with batch insertion

## Installation

```bash
# Clone the repository
git clone https://github.com/username/pynormalizer3.git
cd pynormalizer3

# Install dependencies
pip install -r requirements.txt
```

## Dependencies

- **supabase-py**: Backend database integration
- **psycopg2-binary**: PostgreSQL direct connection capabilities
- **python-dateutil**: Advanced date parsing
- **deep-translator** (optional): Translation of tender content to English

## Quick Start

```python
from tendertrail_integration import TenderTrailIntegration

# Initialize the integration layer
integration = TenderTrailIntegration(
    normalizer=None,  # Optional custom normalizer
    preprocessor=None,  # Optional custom preprocessor
    supabase_url='YOUR_SUPABASE_URL',
    supabase_key='YOUR_SUPABASE_KEY'
)

# Process AFD tenders from JSON data
with open('afd_tenders.json', 'r') as f:
    afd_data = json.load(f)

processed, errors = integration.process_json_tenders(afd_data, 'afd')
print(f"Processed {processed} tenders with {errors} errors")
```

## Normalization Process

The framework follows these steps to normalize tender data:

1. **Direct Normalization**: Attempts to normalize using source-specific logic
2. **Fallback Processing**: If direct normalization fails, uses preprocessor and normalizer if available
3. **Data Validation**: Ensures all required fields are present and correctly formatted
4. **Translation**: Detects and translates non-English content
5. **Metadata Collection**: Captures additional fields as metadata
6. **Database Insertion**: Inserts normalized tenders into the unified database

## Source-Specific Normalization

Different tender sources have their own format and field naming conventions. PyNormalizer3 includes specialized handling for:

- **AFD** (Agence Française de Développement)
- **SAM.gov** (U.S. Government Procurement)
- **UNGM** (United Nations Global Marketplace)
- **TED EU** (Tenders Electronic Daily - European Union)
- Generic formats with intelligent field mapping

Each source module maps specific fields to a standardized schema and extracts additional metadata.

## Target Schema

The normalized tenders follow this standardized schema:

| Field              | Description                                  | Type        |
|--------------------|----------------------------------------------|-------------|
| notice_id          | Unique identifier for the tender             | String      |
| notice_title       | Title of the tender                          | String      |
| notice_type        | Type of tender (Works, Services, etc.)       | String      |
| description        | Detailed description of the tender           | String      |
| country            | Country where tender is implemented          | String      |
| location           | Specific location within country             | String      |
| issuing_authority  | Organization issuing the tender              | String      |
| date_published     | Publication date                             | Date        |
| closing_date       | Deadline for submissions                     | Date        |
| value              | Estimated value of the tender                | Numeric     |
| currency           | Currency of the tender value                 | String      |
| cpvs               | Common Procurement Vocabulary codes          | List        |
| buyer              | Organization making the purchase             | String      |
| email              | Contact email                                | String      |
| source             | Source of the tender data                    | String      |
| url                | URL to the original tender                   | String      |
| tag                | Categories or tags                           | List        |
| metadata           | Additional source-specific information       | JSON        |

## Database Tables

PyNormalizer3 creates and manages several tables in your Supabase project:

- **unified_tenders**: Stores normalized tender data
- **normalization_errors**: Tracks errors during the normalization process
- **target_schema**: Stores the schema definition for normalized tenders

## Advanced Usage

### Processing Tenders from Different Sources

```python
# Process SAM.gov tenders
sam_processed, sam_errors = integration.process_json_tenders(sam_data, 'sam_gov')

# Process UNGM tenders
ungm_processed, ungm_errors = integration.process_json_tenders(ungm_data, 'ungm')
```

### Manual Normalization

```python
# Normalize a tender directly
tender = {
    "title": "Construction Project",
    "description": "Building a bridge",
    "country": "France",
    "publication_date": "2024-05-15"
}

normalized = integration._normalize_tender(tender, source="custom")
```

## Testing

PyNormalizer3 includes a comprehensive test suite to verify the normalization process:

```bash
# Run all tests
python -m unittest discover

# Run specific test file
python test_normalization.py
```

## Recent Improvements

- **Enhanced Translation**: Automatic detection of non-English text with original text preservation
- **Improved Date Parsing**: Robust date handling with support for various formats and validation
- **Metadata Column Addition**: Automatic creation of metadata column if it doesn't exist
- **Value Extraction**: Better handling of monetary values with different formats
- **Batch Processing**: More efficient database operations with batch inserts
- **Error Recovery**: Fallback to individual inserts if batch operations fail

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 