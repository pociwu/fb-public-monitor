from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

from playwright.async_api import async_playwright


async def run() -> None:
    data_dir = Path(os.getenv("FACEBOOK_BROWSER_DATA_DIR", "/browser-data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            str(data_dir),
            headless=False,
            locale="zh-TW",
            timezone_id="Asia/Taipei",
            viewport={"width": 1365, "height": 900},
            args=["--disable-dev-shm-usage"],
        )
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
        print("互動式 Facebook 登入已啟動；登入完成後請保留此頁，再以 Ctrl+C 停止服務。", flush=True)
        last_state: bool | None = None
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        try:
            while not stop.is_set():
                cookies = await context.cookies("https://www.facebook.com")
                logged_in = any(cookie.get("name") == "c_user" and cookie.get("value") for cookie in cookies)
                if logged_in != last_state:
                    print("登入狀態：" + ("已登入，可停止 browser-login" if logged_in else "尚未登入"), flush=True)
                    last_state = logged_in
                try:
                    await asyncio.wait_for(stop.wait(), timeout=3)
                except TimeoutError:
                    pass
        finally:
            await context.close()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
