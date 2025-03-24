import os
import json
import unittest
from tendertrail_integration import TenderTrailIntegration

# Mock Supabase client for testing
class MockSupabase:
    def __init__(self):
        self.tables = {}
        self.rpcs = {}
        self.url = "https://mockproject.supabase.co"
        self.key = "mock_key"
    
    def table(self, name):
        if name not in self.tables:
            self.tables[name] = MockTable(name)
        return self.tables[name]
    
    def rpc(self, name, params=None):
        # Mock successful exec_sql response
        if name == 'exec_sql':
            # Simple table existence check
            if "SELECT EXISTS" in params.get('sql', ''):
                return MockResponse(data=[{"exists": True}])
            # Column check in information_schema
            if "information_schema.columns" in params.get('sql', ''):
                return MockResponse(data=[{"column_name": "metadata"}])
            return MockResponse(data=[{"result": "success"}])
        return MockResponse(data=[])

class MockTable:
    def __init__(self, name):
        self.name = name
        self.data = []
    
    def select(self, fields):
        return self
    
    def insert(self, record):
        self.data.append(record)
        return self
    
    def limit(self, n):
        return self
    
    def execute(self):
        return MockResponse(data=self.data)

class MockResponse:
    def __init__(self, data=None, error=None):
        self.data = data or []
        self.error = error

# Mock normalizer and preprocessor for testing
class MockNormalizer:
    def normalize_tender(self, tender):
        return {
            "notice_id": "mock_id",
            "notice_title": "Mock Tender",
            "description": "This is a mock tender for testing",
            "source": "test_source"
        }

class MockPreprocessor:
    def preprocess_tender(self, tender):
        return tender

class TestTenderTrailIntegration(unittest.TestCase):
    def setUp(self):
        # Create mock objects
        self.normalizer = MockNormalizer()
        self.preprocessor = MockPreprocessor()
        self.supabase = MockSupabase()
        
        # Create integration layer with mock objects
        self.integration = TenderTrailIntegration(
            normalizer=self.normalizer,
            preprocessor=self.preprocessor,
            supabase_url="https://mockproject.supabase.co",
            supabase_key="mock_key"
        )
        
        # Replace the supabase client with our mock
        self.integration.supabase = self.supabase
        
    def test_normalize_tender_afd(self):
        """Test normalization of an AFD tender."""
        # Sample AFD tender
        afd_tender = {
            "afd_id": "AFD-123456",
            "reference": "REF-789",
            "title": "Construction d'une école à Abidjan",
            "tender_type": "Works",
            "description": "Projet de construction d'une école primaire à Abidjan avec 10 salles de classe",
            "country": "Côte d'Ivoire",
            "published_date": "2024-05-15",
            "closing_date": "2024-07-30",
            "url": "https://afd.dgmarket.com/tender/123456",
            "value": "1500000",
            "currency": "EUR"
        }
        
        # Normalize the tender
        normalized = self.integration._normalize_tender(afd_tender, source="afd")
        
        # Check basic fields
        self.assertEqual(normalized["notice_id"], "REF-789")
        self.assertEqual(normalized["notice_title"], "Construction d'une école à Abidjan")
        self.assertEqual(normalized["notice_type"], "Works")
        self.assertEqual(normalized["description"], "Projet de construction d'une école primaire à Abidjan avec 10 salles de classe")
        self.assertEqual(normalized["country"], "Côte d'Ivoire")
        self.assertEqual(normalized["issuing_authority"], "Agence Française de Développement")
        self.assertEqual(normalized["date_published"], "2024-05-15")
        self.assertEqual(normalized["closing_date"], "2024-07-30")
        self.assertEqual(normalized["url"], "https://afd.dgmarket.com/tender/123456")
        self.assertEqual(normalized["source"], "afd")
        
        # Check that metadata is JSON string
        self.assertTrue(isinstance(normalized.get("metadata"), str))
        metadata = json.loads(normalized.get("metadata", "{}"))
        self.assertTrue("value" in metadata)
        self.assertTrue("currency" in metadata)
        
    def test_date_parsing(self):
        """Test the date parsing functionality."""
        # Test various date formats
        test_dates = {
            "2024-05-15": "2024-05-15",  # ISO format
            "15/05/2024": "2024-05-15",  # DD/MM/YYYY
            "05/15/2024": "2024-05-15",  # MM/DD/YYYY
            "15-May-2024": "2024-05-15",  # DD-Mon-YYYY
            "May 15, 2024": "2024-05-15",  # Month DD, YYYY
            "2024.05.15": "2024-05-15",  # YYYY.MM.DD
            "15.05.2024": "2024-05-15",  # DD.MM.YYYY
            "2024-05-15T14:30:00Z": "2024-05-15",  # ISO with time
            1713441600: "2024-04-18"  # Unix timestamp (approximate)
        }
        
        for input_date, expected in test_dates.items():
            parsed = self.integration._parse_date(input_date)
            self.assertEqual(parsed, expected, f"Failed parsing {input_date}")
    
    def test_value_parsing(self):
        """Test value extraction and cleaning for tender_value field."""
        # Create a test method to simulate the value parsing part of _insert_normalized_tenders
        def parse_value(value):
            if isinstance(value, (int, float)):
                return str(value)
            elif isinstance(value, str):
                import re
                numeric_str = re.sub(r'[^\d.]', '', value)
                if numeric_str:
                    return str(float(numeric_str))
            return None
        
        test_values = {
            "1500000": "1500000.0",
            "1,500,000": "1500000.0",
            "1 500 000": "1500000.0",
            "1.5M": "1.5",
            "€1,500,000.00": "1500000.00",
            "$1,500,000": "1500000.0",
            1500000: "1500000",
            1500000.50: "1500000.5"
        }
        
        for input_value, expected in test_values.items():
            parsed = parse_value(input_value)
            self.assertEqual(parsed, expected, f"Failed parsing {input_value}")
    
    def test_process_json_tenders(self):
        """Test processing JSON tenders directly."""
        # Sample JSON data with multiple tenders
        json_data = [
            {
                "afd_id": "AFD-123",
                "title": "Project 1",
                "description": "Description 1",
                "country": "France",
                "published_date": "2024-05-01"
            },
            {
                "afd_id": "AFD-456",
                "title": "Project 2",
                "description": "Description 2",
                "country": "Senegal",
                "published_date": "2024-05-02"
            }
        ]
        
        # Process the JSON data
        processed, errors = self.integration.process_json_tenders(json_data, "afd")
        
        # Check if processing was successful
        self.assertEqual(processed, 2)
        self.assertEqual(errors, 0)
        
        # Check that normalized tenders were inserted
        self.assertTrue(hasattr(self.supabase.tables.get('unified_tenders'), 'data'))
        self.assertGreater(len(self.supabase.tables.get('unified_tenders').data), 0)

    def test_validate_normalized_tender(self):
        """Test validation of normalized tenders."""
        # Test with missing required fields
        tender = {
            "description": "Test description",
            "country": "Test country"
        }
        
        validated = self.integration._validate_normalized_tender(tender)
        
        # Check that required fields were added
        self.assertTrue("notice_id" in validated)
        self.assertTrue("notice_title" in validated)
        self.assertEqual(validated["source"], "Unknown")
        
        # Test data type validation
        tender = {
            "notice_id": "123",
            "notice_title": 12345,  # Should be converted to string
            "description": ["Not", "a", "string"],  # Should be converted to string
            "cpvs": "Single CPV",  # Should be converted to list
            "tag": "Single tag"  # Should be converted to list
        }
        
        validated = self.integration._validate_normalized_tender(tender)
        
        self.assertTrue(isinstance(validated["notice_title"], str))
        self.assertTrue(isinstance(validated["description"], str))
        self.assertTrue(isinstance(validated["cpvs"], list))
        self.assertTrue(isinstance(validated["tag"], list))
        self.assertEqual(validated["cpvs"], ["Single CPV"])

if __name__ == "__main__":
    unittest.main() 