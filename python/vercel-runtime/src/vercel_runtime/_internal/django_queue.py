from __future__ import annotations

import importlib
import json
import os
import urllib.request
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

_QUEUE_TOPIC = "django-tasks"
_MAX_DELAY_SECONDS = 86400  # Vercel Queue max retention / max delay (24h)


def _send_to_queue(
    region: str,
    payload: dict[str, Any],
    delay_seconds: int,
) -> None:
    oidc_token = os.environ.get("VERCEL_OIDC_TOKEN", "")
    url = f"https://{region}.vercel-queue.com/api/v3/topic/{_QUEUE_TOPIC}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {oidc_token}",
            "Content-Type": "application/json",
            "Vqs-Delay-Seconds": str(delay_seconds),
        },
    )
    with urllib.request.urlopen(req) as resp:  # pyright: ignore[reportUnknownVariableType]
        resp.read()  # pyright: ignore[reportUnknownMemberType]


def patch_enqueue() -> None:
    """Monkey-patch django.tasks.Task.enqueue to push directly to Vercel Queue.

    Falls back to the original implementation for unsupported features:
    - tasks with non-default priority
    - tasks with run_after more than 24h in the future

    No-op if django.tasks is not installed or QUEUE_REGION is not set.
    """
    try:
        import django.tasks as _dt  # pyright: ignore[reportMissingModuleSource]
    except ImportError:
        return

    region = os.environ.get("QUEUE_REGION")
    if not region:
        return

    import logging

    _logger = logging.getLogger(__name__)
    _Task: type = _dt.Task  # pyright: ignore[reportAttributeAccessIssue]
    _default_priority: int = getattr(_dt, "DEFAULT_TASK_PRIORITY", 0)
    _original_enqueue = _Task.enqueue

    def _patched_enqueue(task_self: Any, /, *args: Any, **kwargs: Any) -> None:
        from datetime import datetime, timezone

        # Fall back for priority — no equivalent in Vercel Queue.
        if getattr(task_self, "priority", _default_priority) != _default_priority:
            _logger.warning(
                "Vercel Queue integration does not support task priority; "
                "falling back to database backend for task %s",
                task_self.module_path,
            )
            _original_enqueue(task_self, *args, **kwargs)
            return

        # Compute delay from run_after; fall back if > 24h.
        run_after = getattr(task_self, "run_after", None)
        delay_seconds = 0
        if run_after is not None:
            delta = (run_after - datetime.now(tz=timezone.utc)).total_seconds()
            if delta > _MAX_DELAY_SECONDS:
                _logger.warning(
                    "Vercel Queue integration does not support run_after > 24h; "
                    "falling back to database backend for task %s",
                    task_self.module_path,
                )
                _original_enqueue(task_self, *args, **kwargs)
                return
            delay_seconds = max(0, int(delta))

        payload: dict[str, Any] = {
            "task_path": task_self.module_path,
            "queue_name": getattr(task_self, "queue_name", "default"),
            "args": list(args),
            "kwargs": kwargs,
        }
        _send_to_queue(region, payload, delay_seconds)

    _Task.enqueue = _patched_enqueue  # pyright: ignore[reportAttributeAccessIssue]


def _consumer_app(
    environ: dict[str, Any],
    start_response: Callable[..., Any],
) -> Iterable[bytes]:
    """WSGI app that receives Vercel Queue push messages and executes tasks."""
    if environ.get("REQUEST_METHOD") != "POST":
        start_response(
            "405 Method Not Allowed", [("Content-Type", "application/json")]
        )
        return [b'{"error":"method not allowed"}']

    try:
        content_length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(content_length)
        payload: dict[str, Any] = json.loads(body)

        task_path: str = payload["task_path"]
        args: list[Any] = payload.get("args", [])
        kwargs: dict[str, Any] = payload.get("kwargs", {})

        module_path, attr_name = task_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        task_obj = getattr(module, attr_name)
        task_obj.func(*args, **kwargs)

        start_response("200 OK", [("Content-Type", "application/json")])
        return [b'{"ok":true}']

    except Exception as exc:
        import logging
        import traceback

        logging.getLogger(__name__).error(
            "Task execution failed: %s\n%s", exc, traceback.format_exc()
        )
        start_response(
            "500 Internal Server Error", [("Content-Type", "application/json")]
        )
        return [json.dumps({"error": str(exc)}).encode()]


# Exposed as `app` so vc_init.py recognises this as a WSGI entrypoint
# when django-background-task.py is the function entrypoint.
app = _consumer_app
