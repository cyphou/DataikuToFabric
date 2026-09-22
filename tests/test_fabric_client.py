"""Tests for the Fabric OAuth token acquisition (`_acquire_token`).

Covers the auth_method branching that selects the Azure AD credential type:
azure_cli, managed_identity, environment, service_principal, and the
DefaultAzureCredential fallback for unknown/legacy values.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.connectors.fabric_client import FABRIC_SCOPE, FabricClient, _acquire_token


def _stub_config(**overrides) -> SimpleNamespace:
    defaults = dict(
        auth_method="azure_cli",
        tenant_id_env="AZURE_TENANT_ID",
        client_id_env="AZURE_CLIENT_ID",
        client_secret_env="AZURE_CLIENT_SECRET",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


@pytest.fixture(autouse=True)
def _clear_token_env():
    os.environ.pop("FABRIC_ACCESS_TOKEN", None)
    yield
    os.environ.pop("FABRIC_ACCESS_TOKEN", None)


class TestEnvTokenOverride:
    def test_env_token_short_circuits_credential_lookup(self):
        os.environ["FABRIC_ACCESS_TOKEN"] = "pre-acquired-token"
        assert _acquire_token(_stub_config()) == "pre-acquired-token"


class TestAuthMethodBranching:
    def test_azure_cli_uses_azure_cli_credential(self):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="cli-token")
        with patch("azure.identity.AzureCliCredential", return_value=mock_cred) as mock_cls:
            token = _acquire_token(_stub_config(auth_method="azure_cli"))
        assert token == "cli-token"
        mock_cls.assert_called_once_with()
        mock_cred.get_token.assert_called_once_with(FABRIC_SCOPE)

    def test_managed_identity_uses_managed_identity_credential(self):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="mi-token")
        with patch("azure.identity.ManagedIdentityCredential", return_value=mock_cred) as mock_cls:
            token = _acquire_token(_stub_config(auth_method="managed_identity"))
        assert token == "mi-token"
        mock_cls.assert_called_once_with()

    def test_environment_uses_environment_credential(self):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="env-token")
        with patch("azure.identity.EnvironmentCredential", return_value=mock_cred) as mock_cls:
            token = _acquire_token(_stub_config(auth_method="environment"))
        assert token == "env-token"
        mock_cls.assert_called_once_with()

    def test_unknown_method_falls_back_to_default_credential(self):
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="default-token")
        with patch("azure.identity.DefaultAzureCredential", return_value=mock_cred) as mock_cls:
            token = _acquire_token(_stub_config(auth_method="something_unrecognized"))
        assert token == "default-token"
        mock_cls.assert_called_once_with()


class TestServicePrincipal:
    def test_uses_client_secret_credential_with_resolved_env_values(self):
        os.environ["SP_TENANT"] = "tenant-123"
        os.environ["SP_CLIENT"] = "client-456"
        os.environ["SP_SECRET"] = "shh-secret"
        try:
            mock_cred = MagicMock()
            mock_cred.get_token.return_value = MagicMock(token="sp-token")
            with patch("azure.identity.ClientSecretCredential", return_value=mock_cred) as mock_cls:
                token = _acquire_token(
                    _stub_config(
                        auth_method="service_principal",
                        tenant_id_env="SP_TENANT",
                        client_id_env="SP_CLIENT",
                        client_secret_env="SP_SECRET",
                    )
                )
            assert token == "sp-token"
            mock_cls.assert_called_once_with("tenant-123", "client-456", "shh-secret")
        finally:
            for k in ("SP_TENANT", "SP_CLIENT", "SP_SECRET"):
                os.environ.pop(k, None)

    def test_raises_when_required_env_vars_are_missing(self):
        os.environ.pop("SP_MISSING_TENANT", None)
        with pytest.raises(RuntimeError, match="service_principal"):
            _acquire_token(
                _stub_config(
                    auth_method="service_principal",
                    tenant_id_env="SP_MISSING_TENANT",
                    client_id_env="SP_MISSING_CLIENT",
                    client_secret_env="SP_MISSING_SECRET",
                )
            )


class TestFabricClientHeaders:
    def test_bearer_header_set_from_access_token(self):
        client = FabricClient(workspace_id="ws-1", access_token="tok-abc")
        assert client._headers["Authorization"] == "Bearer tok-abc"
