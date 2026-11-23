from __future__ import annotations

from collections.abc import Callable

from deltalake._internal import RawUnityCatalog


class UnityCatalog:
    """A Unity Catalog server, to open the tables that it has by name.

    Open a table with [DeltaTable.from_catalog][deltalake.DeltaTable.from_catalog].
    """

    def __init__(
        self,
        workspace_url: str,
        token: str | Callable[[], str] | None = None,
    ) -> None:
        """
        Args:
            workspace_url: the URL of the Unity Catalog server, or of the Databricks workspace
            token: the access token, or a callable without arguments that returns the access
                token. The catalog calls the callable before each request that it sends, so a
                callable can give a new token when the last one expires. Without a token, the
                credentials come from the `UNITY_` or `DATABRICKS_` environment variables.

        Example:
            ```python
            from deltalake import DeltaTable, UnityCatalog

            catalog = UnityCatalog(
                "https://adb-1234567890.XX.azuredatabricks.net",
                token="<access-token>",
            )
            dt = DeltaTable.from_catalog(catalog, "my_catalog.my_schema.my_table")
            ```
        """
        self._catalog = RawUnityCatalog(workspace_url, token)
        self._workspace_url = workspace_url

    def __repr__(self) -> str:
        return f"UnityCatalog(workspace_url={self._workspace_url!r})"
