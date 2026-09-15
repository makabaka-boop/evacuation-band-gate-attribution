"""请求关联标识（X-Request-ID）：入口中间件、请求上下文与日志过滤器。

现场联调和演练复盘时，运维需要把一次 HTTP 调用在入口、业务处理与数据库
会话的日志串到同一个标识上。本模块提供最小闭环 —— 不引入新的业务记录
（无新表/新字段），也不引入独立进程：

* :class:`RequestIdMiddleware` —— 纯 ASGI 入口中间件。校验调用方经
  ``X-Request-ID`` 传入的标识（1..64 位字母/数字/点/下划线/短横线）；
  未传时生成缺省标识。标识放入请求上下文（ContextVar 与 request.state），
  所有响应（含 400/404/409/422/500）都回写 ``X-Request-ID`` 响应头；
  格式非法时在进入路由与创建数据库会话之前即以 400 拒绝，响应头与错误
  正文携带新生成的可追踪标识。
* :data:`request_id_var` —— 承载标识的 :class:`~contextvars.ContextVar`。
  并发请求各自在独立的任务上下文中读写，互不串号；中间件在请求结束
  （正常返回或异常退出）时一律复位。
* :class:`RequestIdFilter` —— 日志过滤器。自动给每条日志记录附加
  ``request_id`` 字段（取值与当次响应头一致），各日志点无需手工传参。
"""
from __future__ import annotations

import logging
import re
import uuid
from contextvars import ContextVar

from starlette.datastructures import MutableHeaders
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

#: 关联标识的请求/响应头名。
REQUEST_ID_HEADER = "X-Request-ID"

#: 合法标识：1..64 位字母、数字、点、下划线或短横线。
_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,64}")

#: 当前请求上下文中的关联标识；不在请求上下文中时为 None。
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

logger = logging.getLogger(__name__)


def current_request_id() -> str | None:
    """返回当前请求上下文中的关联标识（不在请求上下文中时为 None）。"""
    return request_id_var.get()


def generate_request_id() -> str:
    """生成缺省关联标识（32 位小写十六进制，天然满足合法格式）。"""
    return uuid.uuid4().hex


def is_valid_request_id(value: str) -> bool:
    """校验标识格式：1..64 位字母、数字、点、下划线或短横线。"""
    return _REQUEST_ID_PATTERN.fullmatch(value) is not None


class RequestIdFilter(logging.Filter):
    """把当前请求上下文中的 request_id 附加到每条日志记录。

    不在请求上下文中的记录（启动、后台任务等）记为 ``-``，保证引用
    ``%(request_id)s`` 的格式串永不缺字段。
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = current_request_id() or "-"
        return True


class _RequestIdStreamHandler(logging.StreamHandler):
    """自带 request_id 格式与过滤器的控制台处理器（供幂等安装识别）。"""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s "
                "[request_id=%(request_id)s] %(message)s"
            )
        )
        self.addFilter(RequestIdFilter())


def install_request_id_logging() -> None:
    """给应用日志（``app`` 命名空间）安装 request_id 过滤器与控制台处理器。

    幂等：重复调用不会重复添加处理器。处理器级过滤器对所有传播而来的
    ``app.*`` 记录生效；记录格式自带 ``request_id``，运维可直接按它
    grep 串起一次调用的入口、业务与数据库会话日志。
    """
    app_logger = logging.getLogger("app")
    if any(isinstance(h, _RequestIdStreamHandler) for h in app_logger.handlers):
        return
    app_logger.addHandler(_RequestIdStreamHandler())
    app_logger.setLevel(logging.INFO)


def _provided_request_id(scope: Scope) -> str | None:
    """从 ASGI 原始头中取出调用方提供的标识（未传返回 None）。"""
    for name, value in scope.get("headers", []):
        if name.lower() == b"x-request-id":
            return value.decode("latin-1")
    return None


class RequestIdMiddleware:
    """入口中间件：解析/生成关联标识，贯穿请求上下文并回写响应头。

    采用纯 ASGI 实现（而非 BaseHTTPMiddleware），使 ContextVar 对下游
    路由处理与数据库会话依赖（同一任务上下文）可见，且并发请求互不串号。
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        provided = _provided_request_id(scope)
        if provided is not None and not is_valid_request_id(provided):
            # 非法标识：在进入路由与创建数据库会话之前直接 400，
            # 响应头与错误正文携带新生成的可追踪标识。
            await self._reject_invalid(scope, receive, send)
            return

        request_id = provided if provided is not None else generate_request_id()
        # 放入请求上下文：ContextVar 供日志过滤器/数据库会话使用，
        # request.state 供路由与依赖按需取用。
        token = request_id_var.set(request_id)
        scope.setdefault("state", {})["request_id"] = request_id

        method = scope.get("method", "?")
        path = scope.get("path", "?")
        response_started = False
        status_code: int | None = None

        async def send_with_request_id(message: Message) -> None:
            nonlocal response_started, status_code
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers[REQUEST_ID_HEADER] = request_id
            await send(message)

        logger.info("request started: %s %s", method, path)
        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            # 未捕获异常：与框架默认一致返回 500 纯文本，仅补响应头。
            logger.exception("request failed: %s %s", method, path)
            if response_started:
                raise
            response = PlainTextResponse(
                "Internal Server Error",
                status_code=500,
                headers={REQUEST_ID_HEADER: request_id},
            )
            await response(scope, receive, send)
        else:
            logger.info(
                "request finished: %s %s -> %s", method, path, status_code
            )
        finally:
            # 正常返回与异常退出都复位上下文，杜绝同一连接/任务上的串号。
            request_id_var.reset(token)

    async def _reject_invalid(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        request_id = generate_request_id()
        token = request_id_var.set(request_id)
        try:
            logger.warning(
                "rejecting invalid %s header: %s %s",
                REQUEST_ID_HEADER,
                scope.get("method", "?"),
                scope.get("path", "?"),
            )
            response = JSONResponse(
                status_code=400,
                content={
                    "detail": (
                        f"invalid {REQUEST_ID_HEADER} header: expected 1-64 "
                        "characters of letters, digits, '.', '_' or '-'"
                    ),
                    "request_id": request_id,
                },
                headers={REQUEST_ID_HEADER: request_id},
            )
            await response(scope, receive, send)
        finally:
            request_id_var.reset(token)
