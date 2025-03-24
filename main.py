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
        openai_api_key = actor_input.get('openaiApiKey') or os.environ.get('OPENAI_API_KEY')
        supabase_url = actor_input.get('supabaseUrl') or os.environ.get('SUPABASE_URL')
        supabase_key = actor_input.get('supabaseKey') or os.environ.get('SUPABASE_KEY')
        batch_size = actor_input.get('batchSize', 100)
        
        # Validate required parameters
        if not source_name:
            raise ValueError("Source name is required")
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
        await Actor.log.info(f"Starting normalization for source: {source_name}")
        result = integration.process_source(source_name, batch_size)
        
        # Save result to default dataset
        await Actor.push_data(result)
        
        await Actor.log.info(f"Normalization completed. Processed {result['processed_count']} tenders.")

if __name__ == "__main__":
    Actor.main(main)
