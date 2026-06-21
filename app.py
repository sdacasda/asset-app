"""Compatibility entrypoint for Docker/1Panel.

The project has moved into the ``asset_app`` package in v12.  Keeping this
small wrapper lets the existing command keep working:

    uvicorn app:app --host 0.0.0.0 --port 8000

Future refactors can move code from ``asset_app.legacy_app`` into smaller
route/service modules without changing deployment commands.
"""

from asset_app.legacy_app import app

__all__ = ["app"]
