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
    
    def normalize_tender(self, tender_data: Dict[str, Any], source_schema: Dict[str, Any] = None, target_schema: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Normalize a tender using the LLM.
        
        Args:
            tender_data: The tender data to normalize
            source_schema: Schema describing the source data format
            target_schema: Schema describing the target data format
            
        Returns:
            Normalized tender data
        """
        try:
            # Debug logging
            print(f"DEBUG: Normalizing tender with keys: {list(tender_data.keys()) if isinstance(tender_data, dict) else type(tender_data)}")
            
            # Validate input types
            if not isinstance(tender_data, dict):
                print(f"ERROR: tender_data is not a dictionary: {type(tender_data)}")
                return None
                
            # Validate source_schema if provided
            if source_schema is not None and not isinstance(source_schema, dict):
                print(f"WARNING: source_schema is not a dictionary: {type(source_schema)}")
                source_schema = {}  # Use empty dict as fallback
                
            # Validate target_schema if provided
            if target_schema is not None and not isinstance(target_schema, dict):
                print(f"WARNING: target_schema is not a dictionary: {type(target_schema)}")
                target_schema = {}  # Use empty dict as fallback
            
            # Construct messages for the LLM
            messages = self._construct_messages(tender_data, source_schema, target_schema)
            
            # Cache key for this request
            cache_key = self._generate_cache_key(tender_data, messages)
            
            # Check cache first
            cached_response = self._check_cache(cache_key)
            if cached_response:
                print("DEBUG: Using cached response")
                return cached_response
                
            # Call the LLM API
            completion = self._call_api(messages)
            
            # If we have an error, return None
            if not completion or 'error' in completion:
                print(f"Error in LLM API call: {completion.get('error', 'Unknown error') if isinstance(completion, dict) else 'No completion'}")
                return None
                
            # Parse and validate the response
            normalized_tender = self._parse_response(completion)
            
            # Cache the result if valid
            if normalized_tender and isinstance(normalized_tender, dict):
                self._update_cache(cache_key, normalized_tender)
            
            return normalized_tender
            
        except Exception as e:
            import traceback
            print(f"Error in normalize_tender: {e}")
            traceback.print_exc()
            return None
            
    def _construct_messages(self, tender_data: Dict[str, Any], source_schema: Dict[str, Any] = None, target_schema: Dict[str, Any] = None) -> List[Dict[str, str]]:
        """
        Construct the messages for the LLM.
        
        Args:
            tender_data: The tender data to normalize
            source_schema: Schema describing the source data format
            target_schema: Schema describing the target data format
            
        Returns:
            List of messages for the LLM
        """
        # Ensure safe copying of dictionaries
        tender_data_safe = self._safe_copy(tender_data)
        source_schema_safe = self._safe_copy(source_schema) if source_schema else {}
        target_schema_safe = self._safe_copy(target_schema) if target_schema else {}
        
        import json
        
        # Prepare the system message
        system_message = {
            "role": "system",
            "content": """You are a specialized tender normalization assistant. Your task is to take raw tender data and normalize it according to a target schema.
Extract the relevant information from the input data and structure it according to the target schema.
If a field is not present in the input, leave it empty in the output.
Your output must be a valid JSON object that follows the target schema exactly.
"""
        }
        
        # Prepare the user message
        user_content = "Please normalize the following tender data:"
        user_content += f"\n\nInput tender data:\n{json.dumps(tender_data_safe, indent=2)}"
        
        if source_schema_safe:
            user_content += f"\n\nSource schema:\n{json.dumps(source_schema_safe, indent=2)}"
            
        if target_schema_safe:
            user_content += f"\n\nTarget schema:\n{json.dumps(target_schema_safe, indent=2)}"
        else:
            # Provide a default target schema if none is provided
            default_schema = {
                "title": "Tender title",
                "description": "Tender description",
                "date_published": "Publication date (YYYY-MM-DD)",
                "closing_date": "Closing date (YYYY-MM-DD)",
                "tender_type": "Type of tender",
                "tender_value": "Monetary value of the tender",
                "tender_currency": "Currency code (e.g., USD)",
                "location": "Geographical location",
                "issuing_authority": "Organization issuing the tender",
                "keywords": "Keywords or tags",
                "contact_information": "Contact details",
                "source": "Source of the tender"
            }
            user_content += f"\n\nTarget schema:\n{json.dumps(default_schema, indent=2)}"
        
        user_content += "\n\nPlease output only the normalized JSON without any additional text."
        
        user_message = {
            "role": "user",
            "content": user_content
        }
        
        return [system_message, user_message]
        
    def _safe_copy(self, obj):
        """
        Create a safe copy of nested structures, converting to serializable types.
        """
        if isinstance(obj, dict):
            return {k: self._safe_copy(v) for k, v in obj.items() if k is not None}
        elif isinstance(obj, list):
            return [self._safe_copy(item) for item in obj]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        else:
            # Convert non-serializable types to string
            return str(obj)
        
    def _parse_response(self, completion):
        """
        Parse the completion from the LLM.
        
        Args:
            completion: The completion from the LLM
            
        Returns:
            The parsed normalized tender data
        """
        try:
            # Debug logging
            print(f"DEBUG: Parsing LLM response type: {type(completion)}")
            
            # Handle different response formats based on provider
            if isinstance(completion, dict):
                # OpenAI API format
                if 'choices' in completion and len(completion['choices']) > 0:
                    if 'message' in completion['choices'][0]:
                        content = completion['choices'][0]['message'].get('content', '')
                    else:
                        content = completion['choices'][0].get('text', '')
                # Anthropic API format
                elif 'content' in completion:
                    # For Claude API that returns directly
                    content = completion['content']
                else:
                    print(f"Unexpected API response format: {completion}")
                    return None
            elif isinstance(completion, str):
                # Direct string content
                content = completion
            else:
                print(f"Unexpected completion type: {type(completion)}")
                return None
                
            if not content:
                print("Empty content in LLM response")
                return None
                
            # Extract JSON from the response
            import json
            import re
            
            # Find JSON in the content (in case there's surrounding text)
            json_matches = re.findall(r'```(?:json)?\s*([\s\S]*?)```', content)
            
            if json_matches:
                # Use the first JSON block found
                json_str = json_matches[0]
            else:
                # Try to find anything that looks like JSON
                if content.strip().startswith('{') and content.strip().endswith('}'):
                    json_str = content.strip()
                else:
                    # One more attempt to extract just the JSON object
                    json_match = re.search(r'({[\s\S]*})', content)
                    if json_match:
                        json_str = json_match.group(1)
                    else:
                        print(f"No JSON found in response: {content[:100]}...")
                        return None
                        
            # Parse the JSON
            try:
                normalized_tender = json.loads(json_str)
                if not isinstance(normalized_tender, dict):
                    print(f"Parsed JSON is not a dictionary: {type(normalized_tender)}")
                    return None
                return normalized_tender
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                print(f"JSON string: {json_str[:100]}...")
                return None
                
        except Exception as e:
            print(f"Error parsing LLM response: {e}")
            return None
    
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
