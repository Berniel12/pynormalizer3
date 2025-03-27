import os
import json
from tendertrail_integration import TenderTrailIntegration

def main():
    # Get environment variables for Supabase connection
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Supabase credentials not found in environment variables.")
        print("Please set SUPABASE_URL and SUPABASE_KEY environment variables.")
        return
    
    # Initialize TenderTrailIntegration
    integration = TenderTrailIntegration(supabase_url=supabase_url, supabase_key=supabase_key)
    
    # Check if connected to Supabase
    try:
        # Simple query to check connection
        response = integration.supabase.table('unified_tenders').select('id').limit(1).execute()
        print("Successfully connected to Supabase")
    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")
        return
    
    # Check unified_tenders table
    try:
        response = integration.supabase.table('unified_tenders').select('*').order('created_at', desc=True).limit(5).execute()
        if response.data:
            print(f"Found {len(response.data)} tenders in unified_tenders table")
            
            # Display the tenders
            for i, tender in enumerate(response.data):
                print(f"\n--- Tender {i+1} ---")
                
                # Display key fields
                key_fields = ['title', 'description', 'issuing_authority', 'source', 'tender_type', 'tender_value', 'created_at']
                for field in key_fields:
                    if field in tender:
                        value = tender[field]
                        # Truncate long values
                        if isinstance(value, str) and len(value) > 100:
                            value = value[:100] + "..."
                        print(f"{field}: {value}")
                
                # Check for metadata
                if 'metadata' in tender and tender['metadata']:
                    print("\nMetadata fields:")
                    if isinstance(tender['metadata'], str):
                        try:
                            metadata = json.loads(tender['metadata'])
                            print(f"  Keys: {', '.join(metadata.keys())}")
                        except:
                            print("  Invalid JSON metadata")
                    elif isinstance(tender['metadata'], dict):
                        print(f"  Keys: {', '.join(tender['metadata'].keys())}")
        else:
            print("No tenders found in the unified_tenders table")
    except Exception as e:
        print(f"Error querying unified_tenders: {e}")

if __name__ == "__main__":
    main() 