from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.metrics import ready


async def healthz(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def readyz(_: web.Request) -> web.Response:
    is_ready = ready._value.get() == 1
    return web.json_response({"status": "ready" if is_ready else "starting"}, status=200 if is_ready else 503)


async def metrics(_: web.Request) -> web.Response:
    return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST)


async def start_health_server(port: int) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/readyz", readyz)
    app.router.add_get("/metrics", metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    return runner
