import os
import json
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
import time

class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    @abstractmethod
    def normalize_field(self, field_name: str, field_value: str, target_schema: Dict[str, Any]) -> str:
        """Normalize a field value according to the target schema."""
        pass
    
    @abstractmethod
    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate text from source language to target language."""
        pass
    
    @abstractmethod
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from text according to schema."""
        pass

class OpenAIProvider(LLMProvider):
    """OpenAI LLM provider implementation."""
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.openai.com/v1/chat/completions"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
    def normalize_field(self, field_name: str, field_value: str, target_schema: Dict[str, Any]) -> str:
        """Normalize a field value according to the target schema using OpenAI."""
        field_description = target_schema.get(field_name, {}).get("description", "")
        field_format = target_schema.get(field_name, {}).get("format", "")
        
        prompt = f"""Normalize the following {field_name} value according to this format:
        
Description: {field_description}
Format: {field_format}

Original value: {field_value}

Normalized value:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate text from source language to target language using OpenAI."""
        prompt = f"""Translate the following text from {source_lang} to {target_lang}:

{text}

Translation:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from text according to schema using OpenAI."""
        schema_str = json.dumps(schema, indent=2)
        
        prompt = f"""Extract structured data from the following text according to this schema:
        
{schema_str}

Text:
{text}

Extracted data (JSON format):"""
        
        response = self._call_api(prompt)
        
        try:
            # Try to parse the response as JSON
            return json.loads(response)
        except json.JSONDecodeError:
            # If parsing fails, return a minimal valid response
            return {field: "" for field in schema.keys()}
    
    def _call_api(self, prompt: str) -> str:
        """Call the OpenAI API with the given prompt."""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 1000
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            print(f"Error calling OpenAI API: {e}")
            return ""

class GPT4oMiniProvider(LLMProvider):
    """GPT-4o mini provider implementation - most cost-effective OpenAI option."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.model = "gpt-4o-mini"  # Using GPT-4o mini model
        self.base_url = "https://api.openai.com/v1/chat/completions"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
    def normalize_field(self, field_name: str, field_value: str, target_schema: Dict[str, Any]) -> str:
        """Normalize a field value according to the target schema using GPT-4o mini."""
        field_description = target_schema.get(field_name, {}).get("description", "")
        field_format = target_schema.get(field_name, {}).get("format", "")
        
        prompt = f"""Normalize the following {field_name} value according to this format:
        
Description: {field_description}
Format: {field_format}

Original value: {field_value}

Normalized value:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate text from source language to target language using GPT-4o mini."""
        prompt = f"""Translate the following text from {source_lang} to {target_lang}:

{text}

Translation:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from text according to schema using GPT-4o mini."""
        schema_str = json.dumps(schema, indent=2)
        
        prompt = f"""Extract structured data from the following text according to this schema:
        
{schema_str}

Text:
{text}

Extracted data (JSON format):"""
        
        response = self._call_api(prompt)
        
        try:
            # Try to parse the response as JSON
            return json.loads(response)
        except json.JSONDecodeError:
            # If parsing fails, return a minimal valid response
            return {field: "" for field in schema.keys()}
    
    def _call_api(self, prompt: str) -> str:
        """Call the OpenAI API with GPT-4o mini model."""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 1000
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            print(f"Error calling OpenAI API: {e}")
            return ""

class CohereProvider(LLMProvider):
    """Cohere LLM provider implementation - most cost-effective option."""
    
    def __init__(self, api_key: str, model: str = "command"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.cohere.ai/v1/generate"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
    def normalize_field(self, field_name: str, field_value: str, target_schema: Dict[str, Any]) -> str:
        """Normalize a field value according to the target schema using Cohere."""
        field_description = target_schema.get(field_name, {}).get("description", "")
        field_format = target_schema.get(field_name, {}).get("format", "")
        
        prompt = f"""Normalize the following {field_name} value according to this format:
        
Description: {field_description}
Format: {field_format}

Original value: {field_value}

Normalized value:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate text from source language to target language using Cohere."""
        prompt = f"""Translate the following text from {source_lang} to {target_lang}:

{text}

Translation:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from text according to schema using Cohere."""
        schema_str = json.dumps(schema, indent=2)
        
        prompt = f"""Extract structured data from the following text according to this schema:
        
{schema_str}

Text:
{text}

Extracted data (JSON format):"""
        
        response = self._call_api(prompt)
        
        try:
            # Try to parse the response as JSON
            return json.loads(response)
        except json.JSONDecodeError:
            # If parsing fails, return a minimal valid response
            return {field: "" for field in schema.keys()}
    
    def _call_api(self, prompt: str) -> str:
        """Call the Cohere API with the given prompt."""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "temperature": 0.3,
            "max_tokens": 1000
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()["generations"][0]["text"]
        except Exception as e:
            print(f"Error calling Cohere API: {e}")
            return ""

class MistralProvider(LLMProvider):
    """Mistral AI provider implementation - good self-hosted option."""
    
    def __init__(self, api_key: str, model: str = "mistral-small"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.mistral.ai/v1/chat/completions"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
    def normalize_field(self, field_name: str, field_value: str, target_schema: Dict[str, Any]) -> str:
        """Normalize a field value according to the target schema using Mistral."""
        field_description = target_schema.get(field_name, {}).get("description", "")
        field_format = target_schema.get(field_name, {}).get("format", "")
        
        prompt = f"""Normalize the following {field_name} value according to this format:
        
Description: {field_description}
Format: {field_format}

Original value: {field_value}

Normalized value:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate text from source language to target language using Mistral."""
        prompt = f"""Translate the following text from {source_lang} to {target_lang}:

{text}

Translation:"""
        
        response = self._call_api(prompt)
        return response.strip()
    
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from text according to schema using Mistral."""
        schema_str = json.dumps(schema, indent=2)
        
        prompt = f"""Extract structured data from the following text according to this schema:
        
{schema_str}

Text:
{text}

Extracted data (JSON format):"""
        
        response = self._call_api(prompt)
        
        try:
            # Try to parse the response as JSON
            return json.loads(response)
        except json.JSONDecodeError:
            # If parsing fails, return a minimal valid response
            return {field: "" for field in schema.keys()}
    
    def _call_api(self, prompt: str) -> str:
        """Call the Mistral API with the given prompt."""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 1000
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            print(f"Error calling Mistral API: {e}")
            return ""

class LLMProviderFactory:
    """Factory for creating LLM providers."""
    
    @staticmethod
    def create_provider(provider_type: str, api_key: str, model: Optional[str] = None) -> LLMProvider:
        """Create an LLM provider of the specified type."""
        if provider_type.lower() == "openai":
            return OpenAIProvider(api_key, model or "gpt-3.5-turbo")
        elif provider_type.lower() == "gpt4o-mini":
            return GPT4oMiniProvider(api_key)
        elif provider_type.lower() == "cohere":
            return CohereProvider(api_key, model or "command")
        elif provider_type.lower() == "mistral":
            return MistralProvider(api_key, model or "mistral-small")
        else:
            raise ValueError(f"Unsupported provider type: {provider_type}")

class TenderNormalizer:
    """Main class for normalizing tender data using LLMs."""
    
    def __init__(self, provider: LLMProvider, cache_dir: str = "./cache"):
        self.provider = provider
        self.cache_dir = cache_dir
        self.translation_cache = {}
        self.normalization_cache = {}
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        
        # Load caches from disk if they exist
        self._load_caches()
    
    def normalize_tender(self, tender_data: Dict[str, Any], source_schema: Dict[str, Any], 
                         target_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize tender data from source schema to target schema."""
        normalized_tender = {}
        
        # Process each field in the target schema
        for field_name, field_info in target_schema.items():
            # Check if field exists in mapping
            source_field = self._get_source_field(field_name, source_schema)
            
            if source_field and source_field in tender_data:
                # Get the value from source data
                value = tender_data[source_field]
                
                # Check if translation is needed
                if field_info.get("requires_translation", False) and value:
                    source_lang = source_schema.get("language", "en")
                    target_lang = target_schema.get("language", "en")
                    value = self.translate_text(value, source_lang, target_lang)
                
                # Normalize the field value
                normalized_value = self.normalize_field(field_name, value, target_schema)
                normalized_tender[field_name] = normalized_value
            else:
                # Try to extract from other fields or set default
                normalized_tender[field_name] = self._extract_or_default(field_name, tender_data, target_schema)
        
        return normalized_tender
    
    def normalize_field(self, field_name: str, field_value: str, target_schema: Dict[str, Any]) -> str:
        """Normalize a field value according to the target schema."""
        # Check cache first
        cache_key = f"{field_name}:{field_value}"
        if cache_key in self.normalization_cache:
            return self.normalization_cache[cache_key]
        
        # Use the provider to normalize the field
        normalized_value = self.provider.normalize_field(field_name, field_value, target_schema)
        
        # Cache the result
        self.normalization_cache[cache_key] = normalized_value
        self._save_cache("normalization")
        
        return normalized_value
    
    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate text from source language to target language."""
        # Skip translation if languages are the same
        if source_lang == target_lang:
            return text
        
        # Check cache first
        cache_key = f"{source_lang}:{target_lang}:{text}"
        if cache_key in self.translation_cache:
            return self.translation_cache[cache_key]
        
        # Use the provider to translate the text
        translated_text = self.provider.translate_text(text, source_lang, target_lang)
        
        # Cache the result
        self.translation_cache[cache_key] = translated_text
        self._save_cache("translation")
        
        return translated_text
    
    def extract_structured_data(self, text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from text according to schema."""
        return self.provider.extract_structured_data(text, schema)
    
    def _get_source_field(self, target_field: str, source_schema: Dict[str, Any]) -> Optional[str]:
        """Get the corresponding source field for a target field."""
        # Check if there's a direct mapping
        for source_field, field_info in source_schema.items():
            if field_info.get("maps_to") == target_field:
                return source_field
        
        # Check for field name match
        if target_field in source_schema:
            return target_field
        
        # No matching field found
        return None
    
    def _extract_or_default(self, field_name: str, tender_data: Dict[str, Any], 
                           target_schema: Dict[str, Any]) -> Any:
        """Extract field value from other fields or set default value."""
        field_info = target_schema.get(field_name, {})
        
        # Check if there's a default value
        if "default" in field_info:
            return field_info["default"]
        
        # Try to extract from other fields
        if "extract_from" in field_info:
            extract_info = field_info["extract_from"]
            source_field = extract_info.get("field")
            
            if source_field and source_field in tender_data:
                # Extract using pattern or AI
                if "pattern" in extract_info:
                    # TODO: Implement pattern-based extraction
                    pass
                else:
                    # Use AI to extract
                    schema = {field_name: field_info}
                    result = self.extract_structured_data(tender_data[source_field], schema)
                    return result.get(field_name, "")
        
        # Return empty string as fallback
        return ""
    
    def _load_caches(self):
        """Load caches from disk."""
        translation_cache_path = os.path.join(self.cache_dir, "translation_cache.json")
        normalization_cache_path = os.path.join(self.cache_dir, "normalization_cache.json")
        
        if os.path.exists(translation_cache_path):
            try:
                with open(translation_cache_path, "r") as f:
                    self.translation_cache = json.load(f)
            except Exception as e:
                print(f"Error loading translation cache: {e}")
        
        if os.path.exists(normalization_cache_path):
            try:
                with open(normalization_cache_path, "r") as f:
                    self.normalization_cache = json.load(f)
            except Exception as e:
                print(f"Error loading normalization cache: {e}")
    
    def _save_cache(self, cache_type: str):
        """Save cache to disk."""
        if cache_type == "translation":
            cache_path = os.path.join(self.cache_dir, "translation_cache.json")
            cache_data = self.translation_cache
        elif cache_type == "normalization":
            cache_path = os.path.join(self.cache_dir, "normalization_cache.json")
            cache_data = self.normalization_cache
        else:
            return
        
        try:
            with open(cache_path, "w") as f:
                json.dump(cache_data, f)
        except Exception as e:
            print(f"Error saving {cache_type} cache: {e}")

# Example usage
if __name__ == "__main__":
    # Example configuration
    config = {
        "provider": "gpt4o-mini",  # Using GPT-4o mini as recommended
        "api_key": "your_api_key_here",
        "cache_dir": "./cache"
    }
    
    # Create provider and normalizer
    provider = LLMProviderFactory.create_provider(
        config["provider"], 
        config["api_key"]
    )
    normalizer = TenderNormalizer(provider, config["cache_dir"])
    
    # Example tender data
    tender_data = {
        "title": "Construction of water treatment plant",
        "description": "Project involves building a water treatment facility with capacity of 10,000 m3/day",
        "published_date": "2025-01-15",
        "deadline": "2025-03-30",
        "budget": "USD 5,000,000",
        "location": "Nairobi, Kenya",
        "authority": "Ministry of Water and Sanitation"
    }
    
    # Example source schema
    source_schema = {
        "title": {"type": "string", "maps_to": "title"},
        "description": {"type": "string", "maps_to": "description"},
        "published_date": {"type": "string", "maps_to": "date_published"},
        "deadline": {"type": "string", "maps_to": "closing_date"},
        "budget": {"type": "string", "maps_to": "tender_value"},
        "location": {"type": "string", "maps_to": "location"},
        "authority": {"type": "string", "maps_to": "issuing_authority"},
        "language": "en"
    }
    
    # Example target schema
    target_schema = {
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
                "field": "budget"
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
        "language": "en"
    }
    
    # Normalize tender data
    normalized_tender = normalizer.normalize_tender(tender_data, source_schema, target_schema)
    
    # Print result
    print(json.dumps(normalized_tender, indent=2))
