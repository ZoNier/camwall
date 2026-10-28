from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.getenv("CAMWALL_CONFIG", str(BASE_DIR / "playlist.json")))
WEB_DIR = BASE_DIR / "static"
FFMPEG = os.getenv("CAMWALL_FFMPEG", "ffmpeg")
logging.basicConfig(level=os.getenv("CAMWALL_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("camwall")


class Camera(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    url: str = Field(min_length=8, max_length=2048)
    favorite: bool = False
    country_name: str | None = None
    enabled: bool = True


class Settings(BaseModel):
    grid_columns: int = Field(default=4, ge=1, le=6)
    theme: str = "dark"
    autoplay: bool = True
    low_latency: bool = True
    reconnect_seconds: int = Field(default=3, ge=1, le=30)
    stream_audio: bool = False


class ConfigModel(BaseModel):
    settings: Settings = Settings()
    cameras: list[Camera] = Field(default_factory=list)


def load_config() -> ConfigModel:
    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            raw = {"settings": {}, "cameras": raw}
        config = ConfigModel.model_validate(raw)
        for index, camera in enumerate(config.cameras, 1):
            if not camera.url.startswith(("rtsp://", "rtsps://")):
                raise ValueError(f"Camera #{index}: URL must start with rtsp:// or rtsps://")
        return config
    except (OSError, json.JSONDecodeError, ValidationError, ValueError) as exc:
        raise RuntimeError(f"Cannot load {CONFIG_PATH}: {exc}") from exc


def save_config(config: ConfigModel) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(config.model_dump(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(CONFIG_PATH)


config = ConfigModel()
config_lock = asyncio.Lock()


class StreamHub:
    """Starts one FFmpeg process per camera and broadcasts MPEG-TS to viewers."""
    def __init__(self):
        self.clients: dict[int, set[asyncio.Queue[bytes]]] = {}
        self.tasks: dict[int, asyncio.Task] = {}
        self.processes: dict[int, asyncio.subprocess.Process] = {}
        self.lock = asyncio.Lock()

    async def subscribe(self, camera_id: int) -> asyncio.Queue[bytes]:
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=8)
        async with self.lock:
            self.clients.setdefault(camera_id, set()).add(queue)
            task = self.tasks.get(camera_id)
            if task is None or task.done():
                self.tasks[camera_id] = asyncio.create_task(self.runner(camera_id))
        return queue

    async def unsubscribe(self, camera_id: int, queue: asyncio.Queue[bytes]) -> None:
        async with self.lock:
            viewers = self.clients.get(camera_id)
            if viewers:
                viewers.discard(queue)
                if not viewers:
                    self.clients.pop(camera_id, None)

    async def broadcast(self, camera_id: int, data: bytes) -> None:
        for queue in list(self.clients.get(camera_id, set())):
            if queue.full():
                with suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with suppress(asyncio.QueueFull):
                queue.put_nowait(data)

    async def runner(self, camera_id: int) -> None:
        while True:
            async with self.lock:
                if not self.clients.get(camera_id) or not 0 <= camera_id < len(config.cameras):
                    return
                camera = config.cameras[camera_id]
                settings = config.settings
                if not camera.enabled:
                    return
                url = camera.url

            args = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-nostdin",
                    "-rtsp_transport", "tcp", "-fflags", "nobuffer", "-flags", "low_delay",
                    "-analyzeduration", "1000000", "-probesize", "1000000", "-i", url,
                    "-map", "0:v:0", "-c:v", "copy"]
            args += ["-map", "0:a:0?"] if settings.stream_audio else ["-an"]
            args += ["-muxdelay", "0", "-muxpreload", "0", "-mpegts_flags", "+resend_headers",
                     "-flush_packets", "1", "-f", "mpegts", "pipe:1"]
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
                self.processes[camera_id] = proc
                log.info("FFmpeg started camera=%d pid=%d", camera_id, proc.pid)
                while True:
                    async with self.lock:
                        if not self.clients.get(camera_id):
                            break
                    chunk = await proc.stdout.read(64 * 1024)
                    if not chunk:
                        break
                    await self.broadcast(camera_id, chunk)
            except FileNotFoundError:
                log.exception("FFmpeg executable not found: %s", FFMPEG)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Streaming failed for camera %d", camera_id)
            finally:
                if proc is not None:
                    if proc.returncode is None:
                        with suppress(ProcessLookupError):
                            proc.send_signal(signal.SIGTERM)
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=3)
                        except asyncio.TimeoutError:
                            proc.kill()
                            await proc.wait()
                    self.processes.pop(camera_id, None)
            async with self.lock:
                if not self.clients.get(camera_id):
                    return
            await asyncio.sleep(settings.reconnect_seconds)

    async def stop_all(self) -> None:
        for task in list(self.tasks.values()):
            task.cancel()
        for task in list(self.tasks.values()):
            with suppress(asyncio.CancelledError):
                await task
        for proc in list(self.processes.values()):
            if proc.returncode is None:
                proc.terminate()
                with suppress(Exception):
                    await asyncio.wait_for(proc.wait(), timeout=3)


hub = StreamHub()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global config
    config = load_config()
    yield
    await hub.stop_all()


app = FastAPI(title="CamWall", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/health")
async def health():
    return {"status": "ok", "cameras": len(config.cameras), "ffmpeg": FFMPEG}


@app.get("/api/config")
async def get_config():
    # This endpoint includes RTSP credentials for the settings editor. Do not expose
    # this app to untrusted networks; authentication is not implemented yet.
    async with config_lock:
        return config.model_dump()


@app.put("/api/config")
async def put_config(payload: dict[str, Any]):
    global config
    try:
        candidate = ConfigModel.model_validate(payload)
        for index, camera in enumerate(candidate.cameras, 1):
            if not camera.url.startswith(("rtsp://", "rtsps://")):
                raise ValueError(f"Camera #{index}: URL must start with rtsp:// or rtsps://")
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    async with config_lock:
        save_config(candidate)
        config = candidate
    return {"ok": True, "cameras": len(config.cameras)}


@app.websocket("/ws/stream/{camera_id}")
async def stream(websocket: WebSocket, camera_id: int):
    await websocket.accept()
    async with config_lock:
        if camera_id < 0 or camera_id >= len(config.cameras) or not config.cameras[camera_id].enabled:
            await websocket.close(code=1008, reason="Unknown or disabled camera")
            return
    queue = await hub.subscribe(camera_id)
    try:
        while True:
            await websocket.send_bytes(await queue.get())
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        await hub.unsubscribe(camera_id, queue)
