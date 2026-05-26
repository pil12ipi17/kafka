from aiohttp import web


async def healthz(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def start_health_server(port: int) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/readyz", healthz)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    return runner
