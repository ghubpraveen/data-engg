from __future__ import annotations

import json
import logging
import os

import google.auth
import google.auth.credentials

log = logging.getLogger(__name__)

class GoogleBaseHook(BaseHook):

    def get_credentials_and_project_id(self) -> tuple[google.auth.credentials.Credentials, str | None]:
        """Returns the Credentials object for Google API and the associated project_id."""
        if self._cached_credentials is not None:
            return self._cached_credentials, self._cached_project_id

        key_path: str | None = self._get_field("key_path", None)
        try:
            keyfile_dict: str | dict[str, str] | None = self._get_field("keyfile_dict", None)
            keyfile_dict_json: dict[str, str] | None = None
            if keyfile_dict:
                if isinstance(keyfile_dict, dict):
                    keyfile_dict_json = keyfile_dict
                else:
                    keyfile_dict_json = json.loads(keyfile_dict)
        except json.decoder.JSONDecodeError:
            raise AirflowException("Invalid key JSON.")

        key_secret_name: str | None = self._get_field("key_secret_name", None)
        key_secret_project_id: str | None = self._get_field("key_secret_project_id", None)

        credential_config_file: str | None = self._get_field("credential_config_file", None)

        target_principal, delegates = _get_target_principal_and_delegates(self.impersonation_chain)

        credentials, project_id = get_credentials_and_project_id(
            key_path=key_path,
            keyfile_dict=keyfile_dict_json,
            credential_config_file=credential_config_file,
            key_secret_name=key_secret_name,
            key_secret_project_id=key_secret_project_id,
            scopes=self.scopes,
            delegate_to=self.delegate_to,
            target_principal=target_principal,
            delegates=delegates,
        )

        overridden_project_id = self._get_field("project")
        if overridden_project_id:
            project_id = overridden_project_id

        self._cached_credentials = credentials
        self._cached_project_id = project_id

        return credentials, project_id

    def get_credentials(self) -> google.auth.credentials.Credentials:
        """Returns the Credentials object for Google API."""
        credentials, _ = self.get_credentials_and_project_id()
        return credentials

