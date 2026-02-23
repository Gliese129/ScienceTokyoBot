from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

try:
    from astrbot.api.star import Context, Star, register
except ModuleNotFoundError:
    class Context:  # type: ignore[no-redef]
        pass

    class Star:  # type: ignore[no-redef]
        def __init__(self, context: Context) -> None:
            self.context = context

    def register(*_args: Any, **_kwargs: Any):  # type: ignore[no-redef]
        def _inner(cls: type) -> type:
            return cls

        return _inner

    AstrMessageEvent = Any  # type: ignore

    class _DummyFilter:
        @staticmethod
        def command(_name: str):
            def _inner(func):
                return func

            return _inner

        @staticmethod
        def llm_tool(_name: str | None = None, **_kwargs: Any):
            def _inner(func):
                return func

            return _inner

    filter = _DummyFilter()  # type: ignore[assignment]

    class Plain:  # type: ignore[no-redef]
        def __init__(self, text: str) -> None:
            self.text = text
else:
    try:
        from astrbot.api.event import AstrMessageEvent, filter
    except Exception:
        AstrMessageEvent = Any  # type: ignore

        class _DummyFilter:
            @staticmethod
            def command(_name: str):
                def _inner(func):
                    return func

                return _inner

            @staticmethod
            def llm_tool(_name: str | None = None, **_kwargs: Any):
                def _inner(func):
                    return func

                return _inner

        filter = _DummyFilter()  # type: ignore[assignment]

    try:
        from astrbot.api.message_components import Plain
    except Exception:
        class Plain:  # type: ignore[no-redef]
            def __init__(self, text: str) -> None:
                self.text = text

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path
except Exception:
    def get_astrbot_data_path() -> Path:  # type: ignore[no-redef]
        return Path('data')

try:
    from astrbot.core.utils.session_waiter import SessionController, session_waiter
    SESSION_WAITER_AVAILABLE = True
except Exception:
    SESSION_WAITER_AVAILABLE = False
    SessionController = Any  # type: ignore[assignment]

    def session_waiter(*_args: Any, **_kwargs: Any):  # type: ignore[no-redef]
        def _inner(func: Any) -> Any:
            return func

        return _inner


try:
    from astrbot.api import logger as astr_logger
except Exception:
    astr_logger = logging.getLogger('astrbot_plugin_isct_bot')

__all__ = [
    'Context',
    'Star',
    'register',
    'AstrMessageEvent',
    'filter',
    'Plain',
    'get_astrbot_data_path',
    'SessionController',
    'session_waiter',
    'SESSION_WAITER_AVAILABLE',
    'astr_logger',
]
