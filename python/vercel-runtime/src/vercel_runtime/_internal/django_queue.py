from __future__ import annotations

import contextlib
import email.parser
import email.policy
import importlib
import json
import logging
import os
import traceback
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

_logger = logging.getLogger(__name__)

_MAX_DELAY_SECONDS = 86400  # Vercel Queue max retention / max delay (24h)


def _queue_base_url() -> str:
    return os.environ.get(
        "VERCEL_QUEUE_BASE_URL", "https://vercel-queue.com"
    ).rstrip("/")


def _queue_base_path() -> str:
    base = os.environ.get("VERCEL_QUEUE_BASE_PATH", "/api/v2/messages")
    return base if base.startswith("/") else "/" + base


def _queue_token() -> str:
    token = os.environ.get("VERCEL_QUEUE_TOKEN") or os.environ.get(
        "VERCEL_OIDC_TOKEN"
    )
    if not token:
        raise RuntimeError(
            "No queue authentication token available. "
            "Set VERCEL_QUEUE_TOKEN or VERCEL_OIDC_TOKEN."
        )
    return token


def _send_to_queue(
    queue_name: str,
    payload: dict[str, Any],
    delay_seconds: int,
) -> None:
    url = f"{_queue_base_url()}{_queue_base_path()}"
    body = json.dumps(payload).encode("utf-8")
    headers: dict[str, str] = {
        "Authorization": f"Bearer {_queue_token()}",
        "Vqs-Queue-Name": queue_name,
        "Content-Type": "application/json",
    }
    if delay_seconds > 0:
        headers["Vqs-Delay-Seconds"] = str(delay_seconds)
    deployment_id = os.environ.get("VERCEL_DEPLOYMENT_ID")
    if deployment_id:
        headers["Vqs-Deployment-Id"] = deployment_id
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    with urllib.request.urlopen(req) as resp:  # pyright: ignore[reportUnknownVariableType]
        resp.read()  # pyright: ignore[reportUnknownMemberType]


def _parse_cloudevent(body: bytes) -> tuple[str, str, str]:
    """Return (queue_name, consumer_group, message_id)."""
    try:
        data: Any = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise ValueError(
            "Failed to parse CloudEvent from request body"
        ) from exc
    if not isinstance(data, dict):
        raise ValueError("Invalid CloudEvent: body must be a JSON object")
    if data.get("type") != "com.vercel.queue.v1beta":
        raise ValueError(
            "Invalid CloudEvent type: expected 'com.vercel.queue.v1beta', "
            f"got {data.get('type')!r}"
        )
    ce_data = data.get("data")
    if not isinstance(ce_data, dict):
        raise ValueError("Invalid CloudEvent: 'data' must be an object")
    missing = [
        k
        for k in ("queueName", "consumerGroup", "messageId")
        if k not in ce_data
    ]
    if missing:
        raise ValueError(
            f"Missing required CloudEvent data fields: {', '.join(missing)}"
        )
    return (
        str(ce_data["queueName"]),
        str(ce_data["consumerGroup"]),
        str(ce_data["messageId"]),
    )


def _receive_message(
    queue_name: str,
    consumer_group: str,
    message_id: str,
) -> tuple[Any, str]:
    """Fetch a message by ID; return (payload, ticket)."""
    safe_id = urllib.parse.quote(message_id, safe="")
    url = f"{_queue_base_url()}{_queue_base_path()}/{safe_id}"
    headers: dict[str, str] = {
        "Authorization": f"Bearer {_queue_token()}",
        "Vqs-Queue-Name": queue_name,
        "Vqs-Consumer-Group": consumer_group,
        "Accept": "multipart/mixed",
    }
    req = urllib.request.Request(url, method="GET", headers=headers)
    with urllib.request.urlopen(req) as resp:  # pyright: ignore[reportUnknownVariableType]
        content_type: str = (
            resp.getheader("Content-Type") or ""  # pyright: ignore[reportUnknownMemberType]
        )
        raw_body: bytes = resp.read()  # pyright: ignore[reportUnknownMemberType]

    prefix = (
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode(
            "latin1"
        )
    )
    msg = email.parser.BytesParser(  # pyright: ignore[reportArgumentType]
        policy=email.policy.default
    ).parsebytes(prefix + raw_body)
    for part in msg.walk():
        if part.is_multipart():
            continue
        part_headers = dict(part.items())
        ticket = part_headers.get("Vqs-Ticket")
        if not ticket:
            continue
        payload_bytes: bytes = (
            part.get_payload(decode=True) or b""  # pyright: ignore[reportAssignmentType]
        )
        ct = part_headers.get("Content-Type", "")
        payload: Any
        if "application/json" in ct.lower():
            payload = json.loads(payload_bytes.decode("utf-8"))
        else:
            payload = payload_bytes
        return payload, str(ticket)
    raise RuntimeError("Multipart response contained no valid message parts")


def _delete_message(
    queue_name: str,
    consumer_group: str,
    message_id: str,
    ticket: str,
) -> None:
    safe_id = urllib.parse.quote(message_id, safe="")
    url = f"{_queue_base_url()}{_queue_base_path()}/{safe_id}"
    headers: dict[str, str] = {
        "Authorization": f"Bearer {_queue_token()}",
        "Vqs-Queue-Name": queue_name,
        "Vqs-Consumer-Group": consumer_group,
        "Vqs-Ticket": ticket,
    }
    req = urllib.request.Request(url, method="DELETE", headers=headers)
    with urllib.request.urlopen(req) as resp:  # pyright: ignore[reportUnknownVariableType]
        resp.read()  # pyright: ignore[reportUnknownMemberType]


def _setup_django() -> None:
    """Initialize Django if not already configured."""
    try:
        import django  # pyright: ignore[reportMissingModuleSource]

        with contextlib.suppress(RuntimeError):
            django.setup()
    except ImportError:
        pass


def _build_envelope(
    task_self: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    run_after_iso: str | None,
    default_priority: int,
) -> dict[str, Any]:
    return {
        "vercel": {"kind": "django-tasks", "version": 1},
        "task": {
            "module_path": task_self.module_path,
            "takes_context": bool(getattr(task_self, "takes_context", False)),
            "backend": getattr(task_self, "backend", "default"),
            "queue_name": getattr(task_self, "queue_name", "default"),
            "priority": int(getattr(task_self, "priority", default_priority)),
            "run_after": run_after_iso,
        },
        "args": list(args),
        "kwargs": dict(kwargs),
    }


def patch_enqueue() -> None:
    """Monkey-patch django.tasks.Task.enqueue to push to Vercel Queue.

    Falls back to the original implementation for unsupported features:
    - tasks with non-default priority
    - tasks with run_after more than 24h in the future

    No-op if django.tasks is not installed or a queue token is not set.
    """
    try:
        import django.tasks as _dt  # pyright: ignore[reportMissingModuleSource]
    except ImportError:
        return

    if not (
        os.environ.get("VERCEL_QUEUE_TOKEN")
        or os.environ.get("VERCEL_OIDC_TOKEN")
    ):
        return

    task_class: type = _dt.Task  # pyright: ignore[reportAttributeAccessIssue]
    default_priority: int = getattr(_dt, "DEFAULT_TASK_PRIORITY", 0)
    original_enqueue = task_class.enqueue

    def _patched_enqueue(task_self: Any, /, *args: Any, **kwargs: Any) -> None:
        prio = getattr(task_self, "priority", default_priority)
        if prio != default_priority:
            _logger.warning(
                "Vercel Queue integration does not support task priority; "
                "falling back to database backend for task %s",
                task_self.module_path,
            )
            original_enqueue(task_self, *args, **kwargs)
            return

        run_after = getattr(task_self, "run_after", None)
        run_after_iso: str | None = None
        delay_seconds = 0
        if run_after is not None:
            delta = (run_after - datetime.now(tz=UTC)).total_seconds()
            if delta > _MAX_DELAY_SECONDS:
                _logger.warning(
                    "Vercel Queue integration does not support run_after "
                    "> 24h; falling back to database backend for task %s",
                    task_self.module_path,
                )
                original_enqueue(task_self, *args, **kwargs)
                return
            delay_seconds = max(0, int(delta))
            run_after_iso = run_after.isoformat()

        queue_name: str = getattr(task_self, "queue_name", "default")
        envelope = _build_envelope(
            task_self, args, kwargs, run_after_iso, default_priority
        )
        _send_to_queue(queue_name, envelope, delay_seconds)

    task_class.enqueue = _patched_enqueue  # pyright: ignore[reportAttributeAccessIssue]

    # Also patch the async variant if present.
    original_aenqueue = getattr(task_class, "aenqueue", None)
    if original_aenqueue is not None:

        async def _patched_aenqueue(
            task_self: Any, /, *args: Any, **kwargs: Any
        ) -> None:
            prio = getattr(task_self, "priority", default_priority)
            if prio != default_priority:
                _logger.warning(
                    "Vercel Queue integration does not support task "
                    "priority; falling back to database backend for "
                    "task %s",
                    task_self.module_path,
                )
                await original_aenqueue(task_self, *args, **kwargs)
                return

            run_after = getattr(task_self, "run_after", None)
            run_after_iso: str | None = None
            delay_seconds = 0
            if run_after is not None:
                delta = (run_after - datetime.now(tz=UTC)).total_seconds()
                if delta > _MAX_DELAY_SECONDS:
                    _logger.warning(
                        "Vercel Queue integration does not support "
                        "run_after > 24h; falling back to database "
                        "backend for task %s",
                        task_self.module_path,
                    )
                    await original_aenqueue(task_self, *args, **kwargs)
                    return
                delay_seconds = max(0, int(delta))
                run_after_iso = run_after.isoformat()

            queue_name: str = getattr(task_self, "queue_name", "default")
            envelope = _build_envelope(
                task_self, args, kwargs, run_after_iso, default_priority
            )
            _send_to_queue(queue_name, envelope, delay_seconds)

        task_class.aenqueue = _patched_aenqueue  # pyright: ignore[reportAttributeAccessIssue]


def _consumer_app(
    environ: dict[str, Any],
    start_response: Callable[..., Any],
) -> Iterable[bytes]:
    """WSGI app that receives Vercel Queue push messages and runs tasks."""
    if environ.get("REQUEST_METHOD") != "POST":
        start_response(
            "405 Method Not Allowed",
            [("Content-Type", "application/json")],
        )
        return [b'{"error":"method not allowed"}']

    ticket: str | None = None
    queue_name = consumer_group = message_id = ""
    try:
        content_length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(content_length)

        queue_name, consumer_group, message_id = _parse_cloudevent(body)
        payload, ticket = _receive_message(
            queue_name, consumer_group, message_id
        )

        if not isinstance(payload, dict):
            raise ValueError("Invalid task payload: expected object")
        vercel_info = payload.get("vercel")
        if (
            not isinstance(vercel_info, dict)
            or vercel_info.get("kind") != "django-tasks"
        ):
            raise ValueError(
                "Invalid task payload: not a django-tasks envelope"
            )

        task_info: dict[str, Any] = payload.get("task") or {}
        task_path: str = str(task_info.get("module_path") or "")
        takes_context: bool = bool(task_info.get("takes_context"))
        args: list[Any] = payload.get("args") or []
        kwargs: dict[str, Any] = payload.get("kwargs") or {}

        if not task_path:
            raise ValueError("Missing 'task.module_path' in envelope")

        _setup_django()

        module_path_str, attr_name = task_path.rsplit(".", 1)
        module = importlib.import_module(module_path_str)
        task_obj = getattr(module, attr_name)
        func = getattr(task_obj, "func", task_obj)

        if takes_context:
            # Inject a minimal TaskContext for context-aware tasks.
            try:
                from django.tasks.base import (  # pyright: ignore[reportMissingModuleSource]
                    TaskContext,
                )

                ctx = TaskContext(  # pyright: ignore[reportArgumentType]
                    task_result=None
                )
                func(ctx, *args, **kwargs)
            except Exception:
                func(*args, **kwargs)
        else:
            func(*args, **kwargs)

        _delete_message(queue_name, consumer_group, message_id, ticket)
        ticket = None  # prevent double-delete in finally

        start_response("200 OK", [("Content-Type", "application/json")])
        return [b'{"ok":true}']

    except ValueError as exc:
        _logger.error("Bad request: %s", exc)
        start_response(
            "400 Bad Request", [("Content-Type", "application/json")]
        )
        return [json.dumps({"error": str(exc)}).encode()]
    except Exception as exc:
        _logger.error(
            "Task execution failed: %s\n%s", exc, traceback.format_exc()
        )
        start_response(
            "500 Internal Server Error",
            [("Content-Type", "application/json")],
        )
        return [json.dumps({"error": str(exc)}).encode()]


# Exposed as `app` so vc_init.py recognises this as a WSGI entrypoint
# when api/django_background_task.py is the function entrypoint.
app = _consumer_app
