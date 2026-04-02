"""
Environment variable validator for Search Intelligence Report API.

Validates all required environment variables at startup with clear error messages.
Checks for presence and valid format of:
- SUPABASE_URL, SUPABASE_KEY
- GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
- DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD
- Optional: OPENAI_API_KEY, ANTHROPIC_API_KEY
"""

import os
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse


class EnvironmentValidationError(Exception):
    """Raised when environment variable validation fails."""
    pass


class EnvValidator:
    """Validates environment variables at application startup."""
    
    REQUIRED_VARS = {
        'SUPABASE_URL': {
            'description': 'Supabase project URL',
            'validator': 'url',
            'example': 'https://xxxxx.supabase.co'
        },
        'SUPABASE_KEY': {
            'description': 'Supabase anon/service role key',
            'validator': 'non_empty',
            'min_length': 20,
            'example': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
        },
        'GOOGLE_CLIENT_ID': {
            'description': 'Google OAuth client ID',
            'validator': 'google_client_id',
            'example': '1234567890-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com'
        },
        'GOOGLE_CLIENT_SECRET': {
            'description': 'Google OAuth client secret',
            'validator': 'non_empty',
            'min_length': 20,
            'example': 'GOCSPX-xxxxxxxxxxxxxxxxxxxx'
        },
        'DATAFORSEO_LOGIN': {
            'description': 'DataForSEO API login email',
            'validator': 'email',
            'example': 'your-email@example.com'
        },
        'DATAFORSEO_PASSWORD': {
            'description': 'DataForSEO API password',
            'validator': 'non_empty',
            'min_length': 8,
            'example': 'your-dataforseo-password'
        }
    }
    
    OPTIONAL_VARS = {
        'OPENAI_API_KEY': {
            'description': 'OpenAI API key for GPT models',
            'validator': 'openai_key',
            'example': 'sk-proj-...'
        },
        'ANTHROPIC_API_KEY': {
            'description': 'Anthropic API key for Claude models',
            'validator': 'anthropic_key',
            'example': 'sk-ant-...'
        },
        'JWT_SECRET_KEY': {
            'description': 'Secret key for JWT token signing',
            'validator': 'non_empty',
            'min_length': 32,
            'example': 'your-secret-key-min-32-chars-long'
        },
        'FRONTEND_URL': {
            'description': 'Frontend application URL for CORS',
            'validator': 'url',
            'example': 'http://localhost:3000'
        },
        'ENVIRONMENT': {
            'description': 'Environment name (development, staging, production)',
            'validator': 'environment',
            'example': 'production'
        },
        'LOG_LEVEL': {
            'description': 'Logging level',
            'validator': 'log_level',
            'example': 'INFO'
        }
    }
    
    @staticmethod
    def validate_url(value: str, var_name: str) -> None:
        """Validate URL format."""
        try:
            result = urlparse(value)
            if not all([result.scheme, result.netloc]):
                raise ValueError(f"Invalid URL format")
            if result.scheme not in ['http', 'https']:
                raise ValueError(f"URL must use http or https scheme")
        except Exception as e:
            raise EnvironmentValidationError(
                f"{var_name} is not a valid URL: {value}\n"
                f"Error: {str(e)}\n"
                f"Expected format: {EnvValidator.REQUIRED_VARS.get(var_name, EnvValidator.OPTIONAL_VARS.get(var_name, {})).get('example', 'https://example.com')}"
            )
    
    @staticmethod
    def validate_email(value: str, var_name: str) -> None:
        """Validate email format."""
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        if not email_pattern.match(value):
            raise EnvironmentValidationError(
                f"{var_name} is not a valid email address: {value}\n"
                f"Expected format: user@example.com"
            )
    
    @staticmethod
    def validate_non_empty(value: str, var_name: str, min_length: Optional[int] = None) -> None:
        """Validate non-empty string with optional minimum length."""
        if not value or not value.strip():
            raise EnvironmentValidationError(
                f"{var_name} cannot be empty\n"
                f"Expected: {EnvValidator.REQUIRED_VARS.get(var_name, EnvValidator.OPTIONAL_VARS.get(var_name, {})).get('example', 'a non-empty value')}"
            )
        
        if min_length and len(value) < min_length:
            raise EnvironmentValidationError(
                f"{var_name} must be at least {min_length} characters long\n"
                f"Current length: {len(value)}\n"
                f"Expected format: {EnvValidator.REQUIRED_VARS.get(var_name, EnvValidator.OPTIONAL_VARS.get(var_name, {})).get('example', 'a longer value')}"
            )
    
    @staticmethod
    def validate_google_client_id(value: str, var_name: str) -> None:
        """Validate Google OAuth client ID format."""
        # Google client IDs typically end with .apps.googleusercontent.com
        if not value.endswith('.apps.googleusercontent.com'):
            raise EnvironmentValidationError(
                f"{var_name} does not appear to be a valid Google OAuth client ID\n"
                f"Expected format: xxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com\n"
                f"Got: {value}"
            )
        
        # Check for basic structure
        parts = value.split('.apps.googleusercontent.com')[0]
        if '-' not in parts:
            raise EnvironmentValidationError(
                f"{var_name} does not match expected Google OAuth client ID format\n"
                f"Expected format: xxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com"
            )
    
    @staticmethod
    def validate_openai_key(value: str, var_name: str) -> None:
        """Validate OpenAI API key format."""
        if not value.startswith('sk-'):
            raise EnvironmentValidationError(
                f"{var_name} does not appear to be a valid OpenAI API key\n"
                f"Expected format: sk-proj-... or sk-...\n"
                f"Got: {value[:10]}..."
            )
    
    @staticmethod
    def validate_anthropic_key(value: str, var_name: str) -> None:
        """Validate Anthropic API key format."""
        if not value.startswith('sk-ant-'):
            raise EnvironmentValidationError(
                f"{var_name} does not appear to be a valid Anthropic API key\n"
                f"Expected format: sk-ant-...\n"
                f"Got: {value[:10]}..."
            )
    
    @staticmethod
    def validate_environment(value: str, var_name: str) -> None:
        """Validate environment name."""
        valid_environments = ['development', 'staging', 'production', 'test']
        if value.lower() not in valid_environments:
            raise EnvironmentValidationError(
                f"{var_name} must be one of: {', '.join(valid_environments)}\n"
                f"Got: {value}"
            )
    
    @staticmethod
    def validate_log_level(value: str, var_name: str) -> None:
        """Validate logging level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if value.upper() not in valid_levels:
            raise EnvironmentValidationError(
                f"{var_name} must be one of: {', '.join(valid_levels)}\n"
                f"Got: {value}"
            )
    
    @classmethod
    def validate_variable(cls, var_name: str, value: str, config: Dict) -> None:
        """Validate a single environment variable based on its configuration."""
        validator_type = config.get('validator')
        
        if validator_type == 'url':
            cls.validate_url(value, var_name)
        elif validator_type == 'email':
            cls.validate_email(value, var_name)
        elif validator_type == 'non_empty':
            min_length = config.get('min_length')
            cls.validate_non_empty(value, var_name, min_length)
        elif validator_type == 'google_client_id':
            cls.validate_google_client_id(value, var_name)
        elif validator_type == 'openai_key':
            cls.validate_openai_key(value, var_name)
        elif validator_type == 'anthropic_key':
            cls.validate_anthropic_key(value, var_name)
        elif validator_type == 'environment':
            cls.validate_environment(value, var_name)
        elif validator_type == 'log_level':
            cls.validate_log_level(value, var_name)
        else:
            # Default: just check non-empty
            cls.validate_non_empty(value, var_name)
    
    @classmethod
    def validate_all(cls, raise_on_optional: bool = False) -> Dict[str, str]:
        """
        Validate all environment variables.
        
        Args:
            raise_on_optional: If True, raise errors for missing optional variables
        
        Returns:
            Dict of all validated environment variables
        
        Raises:
            EnvironmentValidationError: If any required variables are missing or invalid
        """
        errors: List[str] = []
        warnings: List[str] = []
        validated_vars: Dict[str, str] = {}
        
        # Validate required variables
        for var_name, config in cls.REQUIRED_VARS.items():
            value = os.getenv(var_name)
            
            if value is None:
                errors.append(
                    f"❌ {var_name} is required but not set\n"
                    f"   Description: {config['description']}\n"
                    f"   Example: {config['example']}"
                )
            else:
                try:
                    cls.validate_variable(var_name, value, config)
                    validated_vars[var_name] = value
                except EnvironmentValidationError as e:
                    errors.append(f"❌ {str(e)}")
        
        # Validate optional variables
        for var_name, config in cls.OPTIONAL_VARS.items():
            value = os.getenv(var_name)
            
            if value is None:
                message = (
                    f"⚠️  {var_name} is not set (optional)\n"
                    f"   Description: {config['description']}\n"
                    f"   Example: {config['example']}"
                )
                if raise_on_optional:
                    errors.append(message)
                else:
                    warnings.append(message)
            else:
                try:
                    cls.validate_variable(var_name, value, config)
                    validated_vars[var_name] = value
                except EnvironmentValidationError as e:
                    if raise_on_optional:
                        errors.append(f"❌ {str(e)}")
                    else:
                        warnings.append(f"⚠️  {str(e)}")
        
        # Print warnings if any
        if warnings:
            print("\n" + "="*80)
            print("ENVIRONMENT VARIABLE WARNINGS")
            print("="*80)
            for warning in warnings:
                print(f"\n{warning}")
            print("\n" + "="*80 + "\n")
        
        # Raise if there are errors
        if errors:
            error_message = "\n" + "="*80 + "\n"
            error_message += "ENVIRONMENT VARIABLE VALIDATION FAILED\n"
            error_message += "="*80 + "\n\n"
            error_message += "\n\n".join(errors)
            error_message += "\n\n" + "="*80 + "\n"
            error_message += "Please set all required environment variables in your .env file\n"
            error_message += "or environment configuration before starting the application.\n"
            error_message += "="*80 + "\n"
            raise EnvironmentValidationError(error_message)
        
        return validated_vars
    
    @classmethod
    def print_config_template(cls) -> None:
        """Print a template .env file with all variables."""
        print("# Search Intelligence Report - Environment Configuration")
        print("# Copy this template to .env and fill in your values\n")
        
        print("# Required Variables")
        print("# " + "="*70)
        for var_name, config in cls.REQUIRED_VARS.items():
            print(f"\n# {config['description']}")
            print(f"# Example: {config['example']}")
            print(f"{var_name}=")
        
        print("\n\n# Optional Variables")
        print("# " + "="*70)
        for var_name, config in cls.OPTIONAL_VARS.items():
            print(f"\n# {config['description']}")
            print(f"# Example: {config['example']}")
            print(f"# {var_name}=")
    
    @classmethod
    def get_validated_config(cls) -> Dict[str, str]:
        """
        Convenience method to get validated configuration.
        
        Returns:
            Dict of validated environment variables
        """
        return cls.validate_all(raise_on_optional=False)


def validate_environment(raise_on_optional: bool = False) -> Dict[str, str]:
    """
    Validate environment variables at application startup.
    
    Args:
        raise_on_optional: If True, raise errors for missing optional variables
    
    Returns:
        Dict of all validated environment variables
    
    Raises:
        EnvironmentValidationError: If validation fails
    """
    return EnvValidator.validate_all(raise_on_optional=raise_on_optional)


def print_env_template() -> None:
    """Print a template .env file."""
    EnvValidator.print_config_template()


if __name__ == "__main__":
    # When run directly, print the template
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "template":
        print_env_template()
    else:
        # Try to validate current environment
        try:
            config = validate_environment(raise_on_optional=False)
            print("\n✅ Environment validation passed!")
            print(f"✅ {len(config)} variables validated successfully\n")
        except EnvironmentValidationError as e:
            print(str(e))
            sys.exit(1)
