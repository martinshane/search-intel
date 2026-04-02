"""
Environment variable validator for Search Intelligence Report API.

Validates environment variables at startup with clear error messages.

CRITICAL vs OPTIONAL classification:
- CRITICAL (SUPABASE_URL, SUPABASE_KEY): Without these the app cannot store
  data or authenticate users — the API cannot serve ANY request.
- IMPORTANT (GOOGLE_CLIENT_*, JWT_SECRET_KEY): Without these, specific features
  (OAuth, authentication) won't work, but the API can still start and serve
  the health endpoint, which is required for Railway health checks.
- OPTIONAL (DATAFORSEO_*, ANTHROPIC_*, OPENAI_*): Enrichment services that
  the pipeline handles gracefully when missing (modules 3/8/11 skip SERP data,
  module 5 uses fallback narrative, etc.).

Previous versions marked Google OAuth and DataForSEO credentials as REQUIRED,
which caused the API to crash on startup if any were missing — preventing even
the /health endpoint from responding and blocking Railway deployment.
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
    
    # Only truly critical vars — without these the app cannot function at all.
    # Missing any of these triggers a RuntimeError in main.py, preventing startup.
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
    }
    
    # Important vars — features degrade without them, but the API boots and
    # serves health checks.  Missing vars are logged as warnings.
    IMPORTANT_VARS = {
        'GOOGLE_CLIENT_ID': {
            'description': 'Google OAuth client ID (required for GSC/GA4 connection)',
            'validator': 'google_client_id',
            'example': '1234567890-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com'
        },
        'GOOGLE_CLIENT_SECRET': {
            'description': 'Google OAuth client secret (required for GSC/GA4 connection)',
            'validator': 'non_empty',
            'min_length': 20,
            'example': 'GOCSPX-xxxxxxxxxxxxxxxxxxxx'
        },
    }
    
    # Enrichment services — the pipeline handles missing data gracefully.
    OPTIONAL_VARS = {
        'DATAFORSEO_LOGIN': {
            'description': 'DataForSEO API login email (modules 3/8/11 skip SERP data if missing)',
            'validator': 'email',
            'example': 'your-email@example.com'
        },
        'DATAFORSEO_PASSWORD': {
            'description': 'DataForSEO API password (modules 3/8/11 skip SERP data if missing)',
            'validator': 'non_empty',
            'min_length': 8,
            'example': 'your-dataforseo-password'
        },
        'ANTHROPIC_API_KEY': {
            'description': 'Anthropic API key for Claude models (module 5 uses fallback narrative if missing)',
            'validator': 'anthropic_key',
            'example': 'sk-ant-...'
        },
        'OPENAI_API_KEY': {
            'description': 'OpenAI API key for GPT models',
            'validator': 'openai_key',
            'example': 'sk-proj-...'
        },
        'JWT_SECRET_KEY': {
            'description': 'Secret key for JWT token signing (ephemeral key used if missing — sessions lost on restart)',
            'validator': 'non_empty',
            'min_length': 32,
            'example': 'your-secret-key-min-32-chars-long'
        },
        'SUPABASE_SERVICE_ROLE_KEY': {
            'description': 'Supabase service role key for background tasks (bypasses RLS)',
            'validator': 'non_empty',
            'min_length': 20,
            'example': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
        },
        'FRONTEND_URL': {
            'description': 'Frontend application URL for CORS',
            'validator': 'url',
            'example': 'https://search-intel-web-production.up.railway.app'
        },
        'GOOGLE_REDIRECT_URI': {
            'description': 'Google OAuth redirect URI',
            'validator': 'url',
            'example': 'https://search-intel-api-production.up.railway.app/api/auth/callback'
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
                raise ValueError("Invalid URL format")
            if result.scheme not in ['http', 'https']:
                raise ValueError("URL must use http or https scheme")
        except Exception as e:
            raise EnvironmentValidationError(
                f"{var_name} is not a valid URL: {value} — {str(e)}"
            )
    
    @staticmethod
    def validate_email(value: str, var_name: str) -> None:
        """Validate email format."""
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        if not email_pattern.match(value):
            raise EnvironmentValidationError(
                f"{var_name} is not a valid email address: {value}"
            )
    
    @staticmethod
    def validate_non_empty(value: str, var_name: str, min_length: Optional[int] = None) -> None:
        """Validate non-empty string with optional minimum length."""
        if not value or not value.strip():
            raise EnvironmentValidationError(f"{var_name} cannot be empty")
        if min_length and len(value) < min_length:
            raise EnvironmentValidationError(
                f"{var_name} must be at least {min_length} characters long (got {len(value)})"
            )
    
    @staticmethod
    def validate_google_client_id(value: str, var_name: str) -> None:
        """Validate Google OAuth client ID format."""
        if not value.endswith('.apps.googleusercontent.com'):
            raise EnvironmentValidationError(
                f"{var_name} does not appear to be a valid Google OAuth client ID"
            )
        parts = value.split('.apps.googleusercontent.com')[0]
        if '-' not in parts:
            raise EnvironmentValidationError(
                f"{var_name} does not match expected Google OAuth client ID format"
            )
    
    @staticmethod
    def validate_openai_key(value: str, var_name: str) -> None:
        """Validate OpenAI API key format."""
        if not value.startswith('sk-'):
            raise EnvironmentValidationError(
                f"{var_name} does not appear to be a valid OpenAI API key"
            )
    
    @staticmethod
    def validate_anthropic_key(value: str, var_name: str) -> None:
        """Validate Anthropic API key format."""
        if not value.startswith('sk-ant-'):
            raise EnvironmentValidationError(
                f"{var_name} does not appear to be a valid Anthropic API key"
            )
    
    @staticmethod
    def validate_environment(value: str, var_name: str) -> None:
        """Validate environment name."""
        valid_environments = ['development', 'staging', 'production', 'test']
        if value.lower() not in valid_environments:
            raise EnvironmentValidationError(
                f"{var_name} must be one of: {', '.join(valid_environments)} (got: {value})"
            )
    
    @staticmethod
    def validate_log_level(value: str, var_name: str) -> None:
        """Validate logging level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if value.upper() not in valid_levels:
            raise EnvironmentValidationError(
                f"{var_name} must be one of: {', '.join(valid_levels)} (got: {value})"
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
            cls.validate_non_empty(value, var_name)


def validate_environment(raise_on_optional: bool = False) -> Dict:
    """
    Validate environment variables at application startup.
    
    Returns a structured dict that main.py's lifespan handler expects:
        {
            "valid": bool,
            "errors": List[str],
            "critical_errors": List[str],
            "warnings": List[str],
        }
    
    Classification:
    - REQUIRED (Supabase): Missing → critical_error → API won't start
    - IMPORTANT (Google OAuth): Missing → warning (loud) → API starts degraded
    - OPTIONAL (DataForSEO, Anthropic, etc.): Missing → info warning → API starts
    
    This function never raises — it captures all validation issues into
    the returned dict so the caller can decide how to handle them.
    """
    result: Dict = {
        "valid": True,
        "errors": [],
        "critical_errors": [],
        "warnings": [],
    }
    
    # ── Required variables (Supabase only) ─────────────────────────
    # Missing any of these makes the API completely non-functional.
    for var_name, config in EnvValidator.REQUIRED_VARS.items():
        value = os.getenv(var_name)
        if value is None:
            msg = f"{var_name} is required but not set ({config['description']})"
            result["errors"].append(msg)
            result["critical_errors"].append(msg)
            result["valid"] = False
        else:
            try:
                EnvValidator.validate_variable(var_name, value, config)
            except EnvironmentValidationError as e:
                msg = str(e)
                result["errors"].append(msg)
                result["critical_errors"].append(msg)
                result["valid"] = False
    
    # ── Important variables (Google OAuth) ─────────────────────────
    # Without these, users can't connect GSC/GA4 — but the API still
    # boots and responds to health checks, which is essential for
    # Railway deployment and monitoring.
    for var_name, config in EnvValidator.IMPORTANT_VARS.items():
        value = os.getenv(var_name)
        if value is None:
            result["warnings"].append(
                f"\u26a0\ufe0f  {var_name} is not set — {config['description']}. "
                f"OAuth login will not work until this is configured."
            )
        else:
            try:
                EnvValidator.validate_variable(var_name, value, config)
            except EnvironmentValidationError as e:
                result["warnings"].append(f"{var_name} format issue: {str(e)}")
    
    # ── Optional variables ─────────────────────────────────────────
    for var_name, config in EnvValidator.OPTIONAL_VARS.items():
        value = os.getenv(var_name)
        if value is None:
            result["warnings"].append(
                f"{var_name} is not set (optional): {config['description']}"
            )
        else:
            try:
                EnvValidator.validate_variable(var_name, value, config)
            except EnvironmentValidationError as e:
                if raise_on_optional:
                    result["errors"].append(str(e))
                    result["valid"] = False
                else:
                    result["warnings"].append(str(e))
    
    return result


def print_env_template() -> None:
    """Print a template .env file with all variables."""
    print("# Search Intelligence Report - Environment Configuration")
    print("# Copy this template to .env and fill in your values\n")
    
    print("# Required Variables (API will not start without these)")
    print("# " + "=" * 70)
    for var_name, config in EnvValidator.REQUIRED_VARS.items():
        print(f"\n# {config['description']}")
        print(f"# Example: {config['example']}")
        print(f"{var_name}=")
    
    print("\n\n# Important Variables (OAuth won't work without these)")
    print("# " + "=" * 70)
    for var_name, config in EnvValidator.IMPORTANT_VARS.items():
        print(f"\n# {config['description']}")
        print(f"# Example: {config['example']}")
        print(f"{var_name}=")
    
    print("\n\n# Optional Variables")
    print("# " + "=" * 70)
    for var_name, config in EnvValidator.OPTIONAL_VARS.items():
        print(f"\n# {config['description']}")
        print(f"# Example: {config['example']}")
        print(f"# {var_name}=")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "template":
        print_env_template()
    else:
        try:
            result = validate_environment(raise_on_optional=False)
            if result["valid"]:
                print("\nEnvironment validation passed!")
            else:
                print("\nEnvironment validation FAILED:")
                for err in result["errors"]:
                    print(f"  - {err}")
            if result["warnings"]:
                print("\nWarnings:")
                for w in result["warnings"]:
                    print(f"  - {w}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)
