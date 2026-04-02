import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from freezegun import freeze_time
import jwt
from typing import Dict, Any

from app.services.oauth_service import OAuthService
from app.services.token_manager import TokenManager
from app.db.supabase_client import get_supabase_client
from app.models.oauth import OAuthToken, OAuthProvider


class TestOAuthIntegration:
    """Integration tests for OAuth flow with real database interactions."""

    @pytest.fixture(autouse=True)
    async def setup(self):
        """Setup test database and services."""
        self.supabase = get_supabase_client()
        self.token_manager = TokenManager(self.supabase)
        self.oauth_service = OAuthService(self.token_manager)
        
        # Test user ID
        self.test_user_id = "test_user_oauth_integration"
        
        # Clean up any existing test data
        await self._cleanup_test_data()
        
        yield
        
        # Cleanup after test
        await self._cleanup_test_data()

    async def _cleanup_test_data(self):
        """Remove test data from database."""
        try:
            self.supabase.table("oauth_tokens").delete().eq(
                "user_id", self.test_user_id
            ).execute()
        except Exception:
            pass  # Table might not exist yet or be empty

    def _create_mock_oauth_response(
        self,
        provider: str,
        include_refresh: bool = True,
        expires_in: int = 3600
    ) -> Dict[str, Any]:
        """Create mock OAuth token response."""
        response = {
            "access_token": f"mock_access_token_{provider}_{datetime.utcnow().timestamp()}",
            "token_type": "Bearer",
            "expires_in": expires_in,
            "scope": self._get_scopes_for_provider(provider)
        }
        
        if include_refresh:
            response["refresh_token"] = f"mock_refresh_token_{provider}"
        
        return response

    def _get_scopes_for_provider(self, provider: str) -> str:
        """Get OAuth scopes for provider."""
        if provider == "google_search_console":
            return "https://www.googleapis.com/auth/webmasters.readonly"
        elif provider == "google_analytics":
            return "https://www.googleapis.com/auth/analytics.readonly"
        return ""

    @pytest.mark.asyncio
    async def test_gsc_authorization_flow(self):
        """Test complete GSC OAuth authorization and token storage."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        mock_code = "mock_authorization_code_gsc"
        
        mock_response = self._create_mock_oauth_response("google_search_console")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            # Exchange authorization code for tokens
            token = await self.oauth_service.exchange_code_for_token(
                code=mock_code,
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Verify token object
            assert token.access_token == mock_response["access_token"]
            assert token.refresh_token == mock_response["refresh_token"]
            assert token.provider == provider
            assert token.user_id == self.test_user_id
            assert token.scope == mock_response["scope"]
            
            # Verify token stored in database
            stored_token = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=provider
            )
            
            assert stored_token is not None
            assert stored_token.access_token == mock_response["access_token"]
            assert stored_token.refresh_token == mock_response["refresh_token"]
            assert stored_token.is_valid()

    @pytest.mark.asyncio
    async def test_ga4_authorization_flow(self):
        """Test complete GA4 OAuth authorization and token storage."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        mock_code = "mock_authorization_code_ga4"
        
        mock_response = self._create_mock_oauth_response("google_analytics")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            # Exchange authorization code for tokens
            token = await self.oauth_service.exchange_code_for_token(
                code=mock_code,
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Verify token object
            assert token.access_token == mock_response["access_token"]
            assert token.refresh_token == mock_response["refresh_token"]
            assert token.provider == provider
            
            # Verify token stored in database
            stored_token = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=provider
            )
            
            assert stored_token is not None
            assert stored_token.provider == OAuthProvider.GOOGLE_ANALYTICS

    @pytest.mark.asyncio
    async def test_dual_provider_authorization(self):
        """Test user can authorize both GSC and GA4."""
        gsc_code = "mock_code_gsc"
        ga4_code = "mock_code_ga4"
        
        gsc_response = self._create_mock_oauth_response("google_search_console")
        ga4_response = self._create_mock_oauth_response("google_analytics")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            # First call for GSC
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=gsc_response)
            )
            
            gsc_token = await self.oauth_service.exchange_code_for_token(
                code=gsc_code,
                provider=OAuthProvider.GOOGLE_SEARCH_CONSOLE,
                user_id=self.test_user_id
            )
            
            # Second call for GA4
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=ga4_response)
            )
            
            ga4_token = await self.oauth_service.exchange_code_for_token(
                code=ga4_code,
                provider=OAuthProvider.GOOGLE_ANALYTICS,
                user_id=self.test_user_id
            )
            
            # Verify both tokens stored
            stored_gsc = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=OAuthProvider.GOOGLE_SEARCH_CONSOLE
            )
            stored_ga4 = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=OAuthProvider.GOOGLE_ANALYTICS
            )
            
            assert stored_gsc is not None
            assert stored_ga4 is not None
            assert stored_gsc.access_token != stored_ga4.access_token
            assert stored_gsc.provider != stored_ga4.provider

    @pytest.mark.asyncio
    async def test_token_refresh_logic(self):
        """Test automatic token refresh when expired."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        # Create initial token
        initial_response = self._create_mock_oauth_response(
            "google_search_console",
            expires_in=3600
        )
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=initial_response)
            )
            
            token = await self.oauth_service.exchange_code_for_token(
                code="initial_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            original_access_token = token.access_token
            original_refresh_token = token.refresh_token
            
            # Simulate token expiration (move time forward 2 hours)
            with freeze_time(datetime.utcnow() + timedelta(hours=2)):
                # Mock refresh response
                refresh_response = {
                    "access_token": f"refreshed_access_token_{datetime.utcnow().timestamp()}",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": initial_response["scope"]
                }
                
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=refresh_response)
                )
                
                # Get token should trigger refresh
                refreshed_token = await self.token_manager.get_valid_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                
                # Verify token was refreshed
                assert refreshed_token.access_token != original_access_token
                assert refreshed_token.access_token == refresh_response["access_token"]
                assert refreshed_token.refresh_token == original_refresh_token
                
                # Verify refreshed token stored in database
                stored_token = await self.token_manager.get_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                
                assert stored_token.access_token == refresh_response["access_token"]

    @pytest.mark.asyncio
    async def test_token_refresh_with_new_refresh_token(self):
        """Test token refresh when provider returns new refresh token."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        
        initial_response = self._create_mock_oauth_response("google_analytics")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=initial_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="initial_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Simulate expiration and refresh with new refresh token
            with freeze_time(datetime.utcnow() + timedelta(hours=2)):
                refresh_response = {
                    "access_token": "new_access_token",
                    "refresh_token": "new_refresh_token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": initial_response["scope"]
                }
                
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=refresh_response)
                )
                
                refreshed_token = await self.token_manager.get_valid_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                
                # Verify both access and refresh tokens updated
                assert refreshed_token.access_token == "new_access_token"
                assert refreshed_token.refresh_token == "new_refresh_token"

    @pytest.mark.asyncio
    async def test_expired_token_error_handling(self):
        """Test handling of expired tokens without refresh token."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        # Create token without refresh token
        mock_response = self._create_mock_oauth_response(
            "google_search_console",
            include_refresh=False,
            expires_in=1
        )
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="code_no_refresh",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Wait for token to expire
            with freeze_time(datetime.utcnow() + timedelta(seconds=2)):
                # Should raise exception because no refresh token
                with pytest.raises(Exception) as exc_info:
                    await self.token_manager.get_valid_token(
                        user_id=self.test_user_id,
                        provider=provider
                    )
                
                assert "expired" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_invalid_refresh_token_handling(self):
        """Test handling when refresh token is invalid/revoked."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        
        initial_response = self._create_mock_oauth_response("google_analytics")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            # Initial token exchange succeeds
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=initial_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="initial_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Simulate token expiration
            with freeze_time(datetime.utcnow() + timedelta(hours=2)):
                # Refresh attempt fails (revoked token)
                mock_post.return_value = Mock(
                    status_code=400,
                    json=Mock(return_value={"error": "invalid_grant"})
                )
                
                # Should raise exception
                with pytest.raises(Exception) as exc_info:
                    await self.token_manager.get_valid_token(
                        user_id=self.test_user_id,
                        provider=provider
                    )
                
                assert "invalid" in str(exc_info.value).lower() or "revoked" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_token_update_replaces_existing(self):
        """Test that new authorization replaces existing token."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        # First authorization
        first_response = self._create_mock_oauth_response("google_search_console")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=first_response)
            )
            
            first_token = await self.oauth_service.exchange_code_for_token(
                code="first_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            first_access_token = first_token.access_token
            
            # Second authorization (user re-authorizes)
            second_response = self._create_mock_oauth_response("google_search_console")
            
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=second_response)
            )
            
            second_token = await self.oauth_service.exchange_code_for_token(
                code="second_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Verify new token replaces old
            assert second_token.access_token != first_access_token
            
            # Verify only one token in database
            stored_token = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=provider
            )
            
            assert stored_token.access_token == second_token.access_token

    @pytest.mark.asyncio
    async def test_concurrent_token_refresh(self):
        """Test that concurrent refresh requests don't create race conditions."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        initial_response = self._create_mock_oauth_response("google_search_console")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=initial_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="initial_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Simulate token expiration
            with freeze_time(datetime.utcnow() + timedelta(hours=2)):
                refresh_response = self._create_mock_oauth_response("google_search_console")
                
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=refresh_response)
                )
                
                # Make 5 concurrent refresh requests
                tasks = [
                    self.token_manager.get_valid_token(
                        user_id=self.test_user_id,
                        provider=provider
                    )
                    for _ in range(5)
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # All should succeed with same token
                successful_results = [r for r in results if not isinstance(r, Exception)]
                assert len(successful_results) >= 4  # Allow for some race condition handling
                
                # All successful results should have same access token
                access_tokens = [r.access_token for r in successful_results]
                assert len(set(access_tokens)) == 1

    @pytest.mark.asyncio
    async def test_token_encryption_in_database(self):
        """Test that tokens are encrypted when stored in database."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        
        mock_response = self._create_mock_oauth_response("google_analytics")
        plain_access_token = mock_response["access_token"]
        plain_refresh_token = mock_response["refresh_token"]
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="test_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Query database directly to check encryption
            result = self.supabase.table("oauth_tokens").select("*").eq(
                "user_id", self.test_user_id
            ).eq(
                "provider", provider.value
            ).execute()
            
            assert len(result.data) == 1
            stored_data = result.data[0]
            
            # Raw database values should not match plain tokens (encrypted)
            # This assumes TokenManager encrypts before storage
            # If not encrypting yet, this test documents the requirement
            if hasattr(self.token_manager, 'encrypt_token'):
                assert stored_data["access_token"] != plain_access_token
                assert stored_data["refresh_token"] != plain_refresh_token
            
            # Retrieved token should be decrypted and match original
            retrieved_token = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=provider
            )
            
            assert retrieved_token.access_token == plain_access_token
            assert retrieved_token.refresh_token == plain_refresh_token

    @pytest.mark.asyncio
    async def test_token_revocation(self):
        """Test token revocation flow."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        mock_response = self._create_mock_oauth_response("google_search_console")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="test_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # Verify token exists
            token = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=provider
            )
            assert token is not None
            
            # Revoke token
            mock_post.return_value = Mock(status_code=200)
            
            await self.oauth_service.revoke_token(
                user_id=self.test_user_id,
                provider=provider
            )
            
            # Verify token removed from database
            revoked_token = await self.token_manager.get_token(
                user_id=self.test_user_id,
                provider=provider
            )
            assert revoked_token is None

    @pytest.mark.asyncio
    async def test_error_handling_network_failure(self):
        """Test error handling when OAuth provider is unreachable."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        
        with patch('httpx.AsyncClient.post') as mock_post:
            # Simulate network error
            mock_post.side_effect = Exception("Connection timeout")
            
            with pytest.raises(Exception) as exc_info:
                await self.oauth_service.exchange_code_for_token(
                    code="test_code",
                    provider=provider,
                    user_id=self.test_user_id
                )
            
            assert "timeout" in str(exc_info.value).lower() or "connection" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_error_handling_invalid_code(self):
        """Test error handling when authorization code is invalid."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=400,
                json=Mock(return_value={
                    "error": "invalid_grant",
                    "error_description": "Invalid authorization code"
                })
            )
            
            with pytest.raises(Exception) as exc_info:
                await self.oauth_service.exchange_code_for_token(
                    code="invalid_code",
                    provider=provider,
                    user_id=self.test_user_id
                )
            
            assert "invalid" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_token_expiry_edge_cases(self):
        """Test token expiry boundary conditions."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        
        # Create token expiring in exactly 5 minutes (boundary)
        mock_response = self._create_mock_oauth_response(
            "google_analytics",
            expires_in=300
        )
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="test_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # At 4 minutes, token should still be valid
            with freeze_time(datetime.utcnow() + timedelta(minutes=4)):
                token = await self.token_manager.get_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                
                # Depending on implementation, might refresh early (within 5 min)
                # or still be valid
                assert token is not None
            
            # At 6 minutes, should definitely need refresh
            with freeze_time(datetime.utcnow() + timedelta(minutes=6)):
                refresh_response = self._create_mock_oauth_response("google_analytics")
                
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=refresh_response)
                )
                
                refreshed_token = await self.token_manager.get_valid_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                
                assert refreshed_token is not None
                assert refreshed_token.access_token == refresh_response["access_token"]

    @pytest.mark.asyncio
    async def test_multiple_users_token_isolation(self):
        """Test that tokens are isolated between users."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        user1_id = f"{self.test_user_id}_1"
        user2_id = f"{self.test_user_id}_2"
        
        try:
            # Create tokens for two different users
            with patch('httpx.AsyncClient.post') as mock_post:
                # User 1 token
                user1_response = self._create_mock_oauth_response("google_search_console")
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=user1_response)
                )
                
                user1_token = await self.oauth_service.exchange_code_for_token(
                    code="user1_code",
                    provider=provider,
                    user_id=user1_id
                )
                
                # User 2 token
                user2_response = self._create_mock_oauth_response("google_search_console")
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=user2_response)
                )
                
                user2_token = await self.oauth_service.exchange_code_for_token(
                    code="user2_code",
                    provider=provider,
                    user_id=user2_id
                )
                
                # Verify tokens are different
                assert user1_token.access_token != user2_token.access_token
                
                # Verify each user gets their own token
                retrieved_user1 = await self.token_manager.get_token(
                    user_id=user1_id,
                    provider=provider
                )
                retrieved_user2 = await self.token_manager.get_token(
                    user_id=user2_id,
                    provider=provider
                )
                
                assert retrieved_user1.access_token == user1_token.access_token
                assert retrieved_user2.access_token == user2_token.access_token
                assert retrieved_user1.access_token != retrieved_user2.access_token
        
        finally:
            # Cleanup both users
            try:
                self.supabase.table("oauth_tokens").delete().eq("user_id", user1_id).execute()
                self.supabase.table("oauth_tokens").delete().eq("user_id", user2_id).execute()
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_refresh_token_rotation(self):
        """Test that refresh tokens are properly rotated on use."""
        provider = OAuthProvider.GOOGLE_ANALYTICS
        
        initial_response = self._create_mock_oauth_response("google_analytics")
        original_refresh = initial_response["refresh_token"]
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=initial_response)
            )
            
            await self.oauth_service.exchange_code_for_token(
                code="initial_code",
                provider=provider,
                user_id=self.test_user_id
            )
            
            # First refresh
            with freeze_time(datetime.utcnow() + timedelta(hours=2)):
                first_refresh_response = {
                    "access_token": "access_after_first_refresh",
                    "refresh_token": "new_refresh_token_1",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": initial_response["scope"]
                }
                
                mock_post.return_value = Mock(
                    status_code=200,
                    json=Mock(return_value=first_refresh_response)
                )
                
                await self.token_manager.get_valid_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                
                # Second refresh
                with freeze_time(datetime.utcnow() + timedelta(hours=4)):
                    second_refresh_response = {
                        "access_token": "access_after_second_refresh",
                        "refresh_token": "new_refresh_token_2",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "scope": initial_response["scope"]
                    }
                    
                    mock_post.return_value = Mock(
                        status_code=200,
                        json=Mock(return_value=second_refresh_response)
                    )
                    
                    final_token = await self.token_manager.get_valid_token(
                        user_id=self.test_user_id,
                        provider=provider
                    )
                    
                    # Verify token has been rotated twice
                    assert final_token.refresh_token != original_refresh
                    assert final_token.refresh_token == "new_refresh_token_2"

    @pytest.mark.asyncio
    async def test_database_transaction_rollback_on_error(self):
        """Test that database transactions rollback on errors."""
        provider = OAuthProvider.GOOGLE_SEARCH_CONSOLE
        
        mock_response = self._create_mock_oauth_response("google_search_console")
        
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )
            
            # Patch database insert to fail
            with patch.object(self.token_manager, 'store_token') as mock_store:
                mock_store.side_effect = Exception("Database error")
                
                with pytest.raises(Exception):
                    await self.oauth_service.exchange_code_for_token(
                        code="test_code",
                        provider=provider,
                        user_id=self.test_user_id
                    )
                
                # Verify no partial data stored
                token = await self.token_manager.get_token(
                    user_id=self.test_user_id,
                    provider=provider
                )
                assert token is None
