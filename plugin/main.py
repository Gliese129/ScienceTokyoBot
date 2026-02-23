"""Compatibility entrypoint for local imports.

AstrBot loads plugin from repository root `main.py`.
This module re-exports the same class for internal package-style imports.
"""

from main import ScienceTokyoNerdBotPlugin

__all__ = ["ScienceTokyoNerdBotPlugin"]
