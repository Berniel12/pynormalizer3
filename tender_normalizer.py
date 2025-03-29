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
            "max_tokens": 4096
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
        Construct the messages to send to the LLM.
        
        Args:
            tender_data: The tender data to normalize
            source_schema: The source schema (default: None)
            target_schema: The target schema (default: None)
            
        Returns:
            The messages to send to the LLM
        """
        try:
            # Ensure we have at least empty dicts for schemas
            source_schema = source_schema or {}
            target_schema = target_schema or {}
            
            # Create a system prompt that explains the task
            system_message = {
                "role": "system",
                "content": """You are a highly accurate data normalization assistant. Your task is to extract structured data from tender notices and convert it to a standardized format.

IMPORTANT INSTRUCTION ABOUT JSON OUTPUT:
1. You MUST return ONLY valid JSON that matches the target schema exactly.
2. All string values MUST be properly escaped. Ensure all quote marks ("), backslashes (\\), and control characters within strings are properly escaped.
3. Do not truncate string values - provide the complete text.
4. Ensure all JSON objects are properly closed with matching braces.
5. Only use double quotes (") for JSON property names and string values, never single quotes (').
6. DO NOT include any explanation text outside the JSON object.
7. Your entire response must be a single valid parseable JSON object.

IMPORTANT DATE HANDLING INSTRUCTIONS:
1. All date fields (date_published, closing_date, etc.) MUST be in ISO 8601 format: YYYY-MM-DD or YYYY-MM-DDThh:mm:ssZ
2. NEVER return "Unknown" for date fields. If a date is missing or cannot be determined:
   - Look for date information in the title, description, or other fields
   - If still no date is found, default to the current year and month with a logical day (e.g., "2025-03-01")
   - As a last resort, use the string "2025-01-01" as a placeholder, BUT NEVER use "Unknown"

Example of valid response format:
```json
{
  "title": "Example tender title",
  "description": "This is a \"quoted\" description with properly escaped quotes.",
  "date_published": "2025-04-15",
  "closing_date": "2025-06-30"
}
```"""
            }
            
            # Use a simplified version of tender_data to avoid huge strings
            safe_tender = self._safe_copy(tender_data)
            
            # Create a user message that includes the tender data and schemas
            user_message = {
                "role": "user",
                "content": f"""Input tender data:
{json.dumps(safe_tender, indent=2, ensure_ascii=False)}

Source schema:
{json.dumps(source_schema, indent=2, ensure_ascii=False)}

Target schema:
{json.dumps(target_schema, indent=2, ensure_ascii=False)}

Convert the input tender data from its original format to match the target schema format.
Use the source schema to understand the input data structure.
Return ONLY the JSON object with the normalized data."""
            }
            
            return [system_message, user_message]
        except Exception as e:
            print(f"Error constructing messages: {e}")
            # Return a simplified fallback message
            return [
                {"role": "system", "content": "Normalize the tender data to match the target schema."},
                {"role": "user", "content": f"Tender: {str(tender_data)[:1000]}\nTarget: {str(target_schema)[:500]}"}
            ]
        
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
            content = None # Initialize content
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
            
            # --- Added Logging ---
            print(f"DEBUG: Raw LLM content received (first 500 chars):\\n{content[:500]}")
            # --- End Added Logging ---

            # Extract JSON from the response
            import json
            import re

            json_str = None
            normalized_tender = None

            try: # Wrap main parsing logic
                # Attempt to strip markdown code fences first
                if content.strip().startswith("```json"):
                    json_str = content.strip()[7:].strip() # Remove ```json and surrounding whitespace
                    if json_str.endswith("```"):
                        json_str = json_str[:-3].strip() # Remove trailing ``` and surrounding whitespace
                elif content.strip().startswith("```"):
                     json_str = content.strip()[3:].strip() # Remove ``` and surrounding whitespace
                     if json_str.endswith("```"):
                        json_str = json_str[:-3].strip() # Remove trailing ``` and surrounding whitespace

                # If stripping worked, try parsing directly
                if json_str:
                     try:
                         normalized_tender = json.loads(json_str)
                         if not isinstance(normalized_tender, dict):
                             print(f"Parsed JSON from stripped content is not a dictionary: {type(normalized_tender)}")
                             json_str = None # Mark as failed to allow regex fallback
                             normalized_tender = None # Reset
                         else:
                             print("DEBUG: Successfully parsed JSON after stripping fences.")
                             return normalized_tender # Success!
                     except json.JSONDecodeError as e:
                         print(f"DEBUG: Error decoding JSON after stripping fences: {e}")
                         # --- Improved Logging ---
                         print(f"DEBUG: Faulty JSON string (stripped) was: {json_str[:500]}...")
                         # --- End Improved Logging ---
                         json_str = None # Mark as failed to allow regex fallback
                         normalized_tender = None # Reset

                # If stripping didn't work or failed, fall back to regex
                if not normalized_tender: # Check if we already succeeded
                    print("DEBUG: Falling back to regex/direct parsing for JSON extraction.")
                    json_matches = re.findall(r'```(?:json)?\\s*([\\s\\S]*?)```', content)

                    if json_matches:
                        # Use the first JSON block found
                        json_str = json_matches[0].strip()
                    else:
                        # Try to find anything that looks like JSON
                        if content.strip().startswith('{') and content.strip().endswith('}'):
                            json_str = content.strip()
                        else:
                            # One more attempt to extract just the JSON object
                            json_match = re.search(r'({[\\s\\S]*})', content)
                            if json_match:
                                json_str = json_match.group(1).strip()
                            else:
                                print(f"No JSON found in response (after regex/fallback): {content[:100]}...")
                                return None # Give up if no JSON structure is found

                    # Parse the JSON found via regex or other fallbacks
                    if json_str:
                        try:
                            normalized_tender = json.loads(json_str)
                            if not isinstance(normalized_tender, dict):
                                print(f"Parsed JSON from regex/fallback is not a dictionary: {type(normalized_tender)}")
                                return None # Fail if it's not a dict
                            print("DEBUG: Successfully parsed JSON using regex/fallback.")
                            return normalized_tender # Success!
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON (from regex/fallback): {e}")
                             # --- Improved Logging ---
                            print(f"DEBUG: Faulty JSON string (regex/fallback) was: {json_str[:500]}...")
                             # --- End Improved Logging ---
                            return None # Fail parsing
                    else:
                         print("DEBUG: Could not extract a JSON string using regex/fallback methods.")
                         return None # Failed to extract any JSON string

            # --- Refined Exception Handling ---
            except json.JSONDecodeError as e: # Catch JSON errors specifically from the last attempt
                print(f"FINAL PARSING ERROR: Failed to decode JSON. Error: {e}")
                print(f"DEBUG: Final faulty JSON string attempted: {json_str[:500]}...")
                return None
            except Exception as e: # Catch any other unexpected errors during parsing
                 print(f"UNEXPECTED PARSING ERROR: An error occurred during JSON extraction/parsing: {e}")
                 print(f"DEBUG: Original raw content was: {content[:500]}...")
                 return None
            # --- End Refined Exception Handling ---

        except Exception as e: # Catch errors *before* parsing starts (e.g., getting content)
            print(f"Error preparing content for LLM response parsing: {e}")
            return None
            
    def _generate_cache_key(self, tender_data: Dict[str, Any], messages: List[Dict[str, str]]) -> str:
        """
        Generate a unique cache key for this normalization request.
        
        Args:
            tender_data: The tender data to normalize
            messages: The messages for the LLM
            
        Returns:
            A unique cache key as a string
        """
        import hashlib
        import json
        
        # Create a deterministic representation of the input
        safe_tender = self._safe_copy(tender_data)
        
        # Create a combined representation for hashing
        cache_data = {
            "tender": safe_tender,
            "messages": [msg.get("content", "") for msg in messages]  # Just use message content for simpler hashing
        }
        
        # Convert to JSON and hash
        try:
            json_str = json.dumps(cache_data, sort_keys=True)
            return hashlib.md5(json_str.encode('utf-8')).hexdigest()
        except Exception as e:
            print(f"Error generating cache key: {e}")
            # Fallback to a timestamp-based key if JSON serialization fails
            import time
            return f"fallback-{int(time.time())}"
    
    def _check_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Check if a cached response exists for this cache key.
        
        Args:
            cache_key: The cache key to check
            
        Returns:
            The cached normalized tender data, or None if not found
        """
        return self.normalization_cache.get(cache_key)
    
    def _update_cache(self, cache_key: str, normalized_tender: Dict[str, Any]) -> None:
        """
        Update the cache with a new normalized tender.
        
        Args:
            cache_key: The cache key to update
            normalized_tender: The normalized tender data to cache
        """
        self.normalization_cache[cache_key] = normalized_tender
        self._save_cache("normalization")
    
    def _call_api(self, messages: List[Dict[str, str]]) -> Union[Dict[str, Any], str]:
        """
        Call the LLM API with the given messages.
        
        Args:
            messages: The messages to send to the LLM
            
        Returns:
            The response from the LLM API
        """
        try:
            # Use the provider's API call method if it supports message format
            if hasattr(self.provider, '_call_api') and callable(getattr(self.provider, '_call_api')):
                system_message = messages[0]['content'] if messages and messages[0]['role'] == 'system' else ""
                user_message = messages[1]['content'] if len(messages) > 1 and messages[1]['role'] == 'user' else ""
                
                # Combine system and user messages if needed
                prompt = f"{system_message}\n\n{user_message}" if system_message else user_message
                return self.provider._call_api(prompt)
            else:
                # Fallback for providers without direct message support
                print("Provider does not support direct API calls, using extract_structured_data instead")
                # Extract schema from messages
                schema = {}
                for msg in messages:
                    if msg['role'] == 'user' and 'Target schema' in msg['content']:
                        import re
                        import json
                        schema_match = re.search(r'Target schema:\s*(\{.*\})', msg['content'], re.DOTALL)
                        if schema_match:
                            try:
                                schema = json.loads(schema_match.group(1))
                            except:
                                pass
                
                # Extract tender data from messages
                tender_text = ""
                for msg in messages:
                    if msg['role'] == 'user' and 'Input tender data' in msg['content']:
                        tender_text = msg['content']
                        break
                
                # Use extract_structured_data as fallback
                return self.provider.extract_structured_data(tender_text, schema or {})
        except Exception as e:
            print(f"Error calling LLM API: {e}")
            return {"error": str(e)}
    
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
