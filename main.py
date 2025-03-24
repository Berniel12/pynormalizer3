import os
import json
from apify import Actor
from tender_normalizer import LLMProviderFactory, TenderNormalizer
from tender_preprocessor import TenderPreprocessor
from tendertrail_integration import TenderTrailIntegration

async def main():
    # Initialize the Actor
    async with Actor:
        # Get input
        actor_input = await Actor.get_input() or {}
        
        # Get required parameters
        source_name = actor_input.get('sourceName')
        process_all_sources = actor_input.get('processAllSources', True)
        openai_api_key = actor_input.get('openaiApiKey') or os.environ.get('OPENAI_API_KEY')
        supabase_url = actor_input.get('supabaseUrl') or os.environ.get('SUPABASE_URL')
        supabase_key = actor_input.get('supabaseKey') or os.environ.get('SUPABASE_KEY')
        batch_size = actor_input.get('batchSize', 100)
        
        # Validate required parameters
        if not openai_api_key:
            raise ValueError("OpenAI API key is required")
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase credentials are required")
        
        # Set up cache directory
        cache_dir = os.path.join(os.getcwd(), 'storage', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Initialize components
        provider = LLMProviderFactory.create_provider("gpt4o-mini", openai_api_key)
        normalizer = TenderNormalizer(provider, cache_dir)
        preprocessor = TenderPreprocessor()
        
        # Initialize integration
        integration = TenderTrailIntegration(
            normalizer=normalizer,
            preprocessor=preprocessor,
            supabase_url=supabase_url,
            supabase_key=supabase_key
        )
        
        # Process tenders
        all_results = []
        
        if process_all_sources or not source_name:
            # Get all available sources
            sources = get_available_sources(integration.supabase)
            await Actor.log.info(f"Processing all available sources: {', '.join(sources)}")
            
            for source in sources:
                await Actor.log.info(f"Starting normalization for source: {source}")
                result = integration.process_source(source, batch_size)
                all_results.append(result)
                await Actor.log.info(f"Completed normalization for {source}. Processed {result['processed_count']} tenders.")
        else:
            # Process single source
            await Actor.log.info(f"Starting normalization for source: {source_name}")
            result = integration.process_source(source_name, batch_size)
            all_results.append(result)
            await Actor.log.info(f"Normalization completed. Processed {result['processed_count']} tenders.")
        
        # Combine results
        combined_result = {
            "sources_processed": len(all_results),
            "total_processed": sum(r["processed_count"] for r in all_results),
            "total_success": sum(r["success_count"] for r in all_results),
            "total_errors": sum(r["error_count"] for r in all_results),
            "details": all_results
        }
        
        # Save result to default dataset
        await Actor.push_data(combined_result)
        
        await Actor.log.info(f"All normalization completed. Processed {combined_result['total_processed']} tenders across {combined_result['sources_processed']} sources.")

def get_available_sources(supabase):
    """Get all available tender sources from the database."""
    # First, try to get sources from the source_schemas table
    try:
        response = supabase.table('source_schemas').select('name').execute()
        if response.data:
            return [source['name'] for source in response.data]
    except Exception as e:
        print(f"Error getting sources from source_schemas: {e}")
    
    # Fallback to querying tables that end with _tenders
    try:
        response = supabase.rpc('get_tables_with_suffix', {'suffix': '_tenders'}).execute()
        if response.data:
            # Extract source names by removing the _tenders suffix
            return [table_name.replace('_tenders', '') for table_name in response.data]
    except Exception as e:
        print(f"Error getting tables with _tenders suffix: {e}")
    
    # Final fallback to default sources
    return ["adb", "wb", "ungm"]

if __name__ == "__main__":
    Actor.run(main)
