from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from starlette.middleware.sessions import SessionMiddleware
from pwdlib import PasswordHash
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.getenv("CAMWALL_CONFIG", str(BASE_DIR / "playlist.json")))
SETTINGS_PATH = Path(os.getenv("CAMWALL_SETTINGS", str(BASE_DIR / "settings.json")))
WEB_DIR = BASE_DIR / "static"
FFMPEG = os.getenv("CAMWALL_FFMPEG", "ffmpeg")
FFPROBE = os.getenv("CAMWALL_FFPROBE", "ffprobe")
AUTH_PATH = Path(os.getenv("CAMWALL_AUTH_FILE", str(BASE_DIR / "auth.json")))
SESSION_SECRET = os.getenv("CAMWALL_SESSION_SECRET", "")
PASSWORD_HASHER = PasswordHash.recommended()
LOGIN_ATTEMPTS: dict[str, tuple[int, float]] = {}
logging.basicConfig(level=os.getenv("CAMWALL_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("camwall")


class Camera(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    url: str = Field(min_length=8, max_length=2048)
    favorite: bool = False
    country_name: str | None = None
    enabled: bool = True
    autoplay: bool = False
    audio: bool = False


class Settings(BaseModel):
    grid_columns: int = Field(default=4, ge=1, le=6)
    theme: str = "dark"
    low_latency: bool = True
    reconnect_seconds: int = Field(default=3, ge=1, le=30)


class ConfigModel(BaseModel):
    settings: Settings = Settings()
    cameras: list[Camera] = Field(default_factory=list)


def _atomic_write_json(path: Path, data: Any, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, mode)
    tmp.replace(path)


def load_config() -> ConfigModel:
    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        legacy_settings: dict[str, Any] = {}
        if isinstance(raw, list):
            cameras_raw = raw
        elif isinstance(raw, dict):
            cameras_raw = raw.get("cameras", [])
            legacy_settings = raw.get("settings", {}) or {}
        else:
            raise ValueError("playlist.json must contain a camera list or an object with cameras")
        if SETTINGS_PATH.exists():
            settings_raw = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        else:
            settings_raw = legacy_settings
        legacy_autoplay = bool(settings_raw.get("autoplay", False))
        legacy_audio = bool(settings_raw.get("stream_audio", False))
        migrated_cameras = []
        for item in cameras_raw:
            item = dict(item)
            item.setdefault("autoplay", legacy_autoplay)
            item.setdefault("audio", legacy_audio)
            migrated_cameras.append(item)
        settings = Settings.model_validate(settings_raw)
        cameras = [Camera.model_validate(item) for item in migrated_cameras]
        for index, camera in enumerate(cameras, 1):
            if not camera.url.startswith(("rtsp://", "rtsps://")):
                raise ValueError(f"Camera #{index}: URL must start with rtsp:// or rtsps://")
        _atomic_write_json(CONFIG_PATH, {"cameras": [c.model_dump() for c in cameras]}, 0o600)
        _atomic_write_json(SETTINGS_PATH, settings.model_dump(), 0o600)
        return ConfigModel(settings=settings, cameras=cameras)
    except (OSError, json.JSONDecodeError, ValidationError, ValueError) as exc:
        raise RuntimeError(f"Cannot load CamWall configuration: {exc}") from exc


def save_config(config: ConfigModel) -> None:
    _atomic_write_json(SETTINGS_PATH, config.settings.model_dump(), 0o600)
    _atomic_write_json(CONFIG_PATH, {"cameras": [camera.model_dump() for camera in config.cameras]}, 0o600)


config = ConfigModel()
config_lock = asyncio.Lock()


class CameraStream:
    """One persistent RTSP ingest process per enabled camera; no video transcoding."""
    def __init__(self, camera_id: int):
        self.camera_id = camera_id
        self.task: asyncio.Task | None = None
        self.process: asyncio.subprocess.Process | None = None
        self.clients: set[asyncio.Queue[bytes | None]] = set()
        self.buffer: deque[bytes] = deque(maxlen=128)
        self.preview: bytes | None = None
        self.preview_task: asyncio.Task | None = None
        self.state = "starting"
        self.codec = "unknown"
        self.width: int | None = None
        self.height: int | None = None
        self.last_error: str | None = None
        self.last_data_at: float | None = None
        self.lock = asyncio.Lock()

    async def probe(self, url: str) -> None:
        args = [FFPROBE, "-v", "error", "-rtsp_transport", "tcp", "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height", "-of", "json", url]
        try:
            proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
            data = json.loads(stdout or b"{}")
            streams = data.get("streams") or []
            if streams:
                stream = streams[0]
                self.codec = str(stream.get("codec_name") or "unknown").lower()
                self.width = stream.get("width")
                self.height = stream.get("height")
        except Exception as exc:
            self.last_error = f"ffprobe: {type(exc).__name__}"
            log.warning("Codec probe failed camera=%d: %s", self.camera_id, exc)

    async def subscribe(self) -> asyncio.Queue[bytes | None]:
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=48)
        async with self.lock:
            self.clients.add(queue)
            # Send recent TS data immediately; resend_headers makes MPEG-TS
            # PAT/PMT available again at keyframes for late joiners.
            for chunk in self.buffer:
                if not queue.full():
                    queue.put_nowait(chunk)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[bytes | None]) -> None:
        async with self.lock:
            self.clients.discard(queue)

    async def broadcast(self, chunk: bytes) -> None:
        self.buffer.append(chunk)
        for queue in tuple(self.clients):
            if queue.full():
                # Never drop arbitrary MPEG-TS chunks: that breaks continuity.
                # Disconnect the slow viewer instead of feeding it corrupted TS.
                self.clients.discard(queue)
                while not queue.empty():
                    with suppress(asyncio.QueueEmpty):
                        queue.get_nowait()
                queue.put_nowait(None)
                continue
            queue.put_nowait(chunk)

    async def preview_loop(self) -> None:
        """Generate one shared, low-frequency JPEG snapshot from the existing TS buffer."""
        await asyncio.sleep(5 + (self.camera_id % 7) * 2)
        while True:
            await asyncio.sleep(25)
            if not self.buffer or self.state not in ("live", "reconnecting"):
                continue
            # The latest few chunks are enough for a keyframe search; avoid joining
            # the entire ring buffer and keep preview decoding deliberately infrequent.
            payload = b"".join(list(self.buffer)[-24:])
            if len(payload) < 188 * 10:
                continue
            sync = next((offset for offset in range(min(188, len(payload) - 376))
                         if payload[offset] == 0x47 and payload[offset + 188] == 0x47
                         and payload[offset + 376] == 0x47), None)
            if sync is None:
                continue
            payload = payload[sync:]
            args = [FFMPEG, "-hide_banner", "-loglevel", "error",
                    "-fflags", "+discardcorrupt+genpts", "-analyzeduration", "5M",
                    "-probesize", "5M", "-skip_frame", "nokey",
                    "-f", "mpegts", "-i", "pipe:0", "-an", "-frames:v", "1",
                    "-vf", "scale=480:-2", "-q:v", "7",
                    "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL)
                stdout, _ = await asyncio.wait_for(proc.communicate(payload), timeout=6)
                if stdout.startswith(bytes((255, 216))):
                    self.preview = stdout
            except asyncio.CancelledError:
                if proc and proc.returncode is None:
                    proc.kill()
                    with suppress(Exception):
                        await proc.wait()
                raise
            except Exception as exc:
                log.debug("Preview generation skipped camera=%d: %s", self.camera_id, exc)
                if proc and proc.returncode is None:
                    proc.kill()
                    with suppress(Exception):
                        await proc.wait()

    async def run(self) -> None:
        while True:
            camera = config.cameras[self.camera_id] if self.camera_id < len(config.cameras) else None
            if camera is None or not camera.enabled:
                self.state = "disabled"
                return
            self.state = "connecting"
            self.last_data_at = None
            self.codec = "unknown"
            self.width = None
            self.height = None
            self.buffer.clear()
            await self.probe(camera.url)
            args = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-nostdin",
                    "-rtsp_transport", "tcp",
                    "-fflags", "nobuffer", "-flags", "low_delay",
                    "-i", camera.url, "-map", "0:v:0", "-c:v", "copy"]
            if camera.audio:
                args += ["-map", "0:a:0?", "-c:a", "copy"]
            else:
                args += ["-an"]
            args += ["-muxdelay", "0", "-muxpreload", "0", "-mpegts_flags", "+resend_headers",
                     "-flush_packets", "1", "-f", "mpegts", "pipe:1"]
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
                self.process = proc
                self.state = "connecting"
                log.info("RTSP relay process started camera=%d codec=%s pid=%d", self.camera_id, self.codec, proc.pid)
                assert proc.stdout is not None
                while True:
                    try:
                        chunk = await asyncio.wait_for(proc.stdout.read(188 * 348), timeout=30)
                    except asyncio.TimeoutError:
                        self.last_error = "No stream data received for 30 seconds"
                        log.warning("RTSP stream stalled camera=%d pid=%d; restarting FFmpeg", self.camera_id, proc.pid)
                        break
                    if not chunk:
                        return_code = await proc.wait()
                        self.last_error = f"FFmpeg exited with code {return_code}"
                        log.warning("FFmpeg exited camera=%d pid=%d code=%s",
                                    self.camera_id, proc.pid, return_code)
                        break
                    if self.state != "live":
                        self.state = "live"
                        self.last_error = None
                        log.info("RTSP stream is receiving data camera=%d", self.camera_id)
                    self.last_data_at = time.monotonic()
                    await self.broadcast(chunk)
            except FileNotFoundError:
                self.state = "error"
                self.last_error = "FFmpeg executable not found"
                log.exception("FFmpeg executable not found: %s", FFMPEG)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                log.exception("Stream relay failed camera=%d", self.camera_id)
            finally:
                self.process = None
                if proc is not None and proc.returncode is None:
                    with suppress(ProcessLookupError):
                        proc.send_signal(signal.SIGTERM)
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
            self.state = "reconnecting"
            await asyncio.sleep(config.settings.reconnect_seconds)

    def status(self) -> dict[str, Any]:
        idle_seconds = (round(time.monotonic() - self.last_data_at, 1)
                        if self.last_data_at is not None else None)
        return {"id": self.camera_id, "state": self.state, "codec": self.codec,
                "width": self.width, "height": self.height, "viewers": len(self.clients),
                "last_data_seconds_ago": idle_seconds, "error": self.last_error}


class StreamManager:
    def __init__(self):
        self.streams: dict[int, CameraStream] = {}

    async def start(self) -> None:
        for camera_id, camera in enumerate(config.cameras):
            if camera.enabled:
                stream = CameraStream(camera_id)
                self.streams[camera_id] = stream
                stream.task = asyncio.create_task(stream.run(), name=f"camwall-camera-{camera_id}")
                stream.preview_task = asyncio.create_task(stream.preview_loop(), name=f"camwall-preview-{camera_id}")

    async def restart(self) -> None:
        await self.stop()
        await self.start()

    async def apply_config(self, previous: ConfigModel, candidate: ConfigModel) -> None:
        count = max(len(previous.cameras), len(candidate.cameras))
        for camera_id in range(count):
            old = previous.cameras[camera_id] if camera_id < len(previous.cameras) else None
            new = candidate.cameras[camera_id] if camera_id < len(candidate.cameras) else None
            old_signature = (old.url, old.enabled, old.audio) if old else None
            new_signature = (new.url, new.enabled, new.audio) if new else None
            if old_signature == new_signature:
                continue
            stream = self.streams.pop(camera_id, None)
            if stream:
                if stream.task:
                    stream.task.cancel()
                    with suppress(asyncio.CancelledError):
                        await stream.task
                if stream.preview_task:
                    stream.preview_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await stream.preview_task
            if new and new.enabled:
                stream = CameraStream(camera_id)
                self.streams[camera_id] = stream
                stream.task = asyncio.create_task(stream.run(), name=f"camwall-camera-{camera_id}")
                stream.preview_task = asyncio.create_task(stream.preview_loop(), name=f"camwall-preview-{camera_id}")

    async def stop(self) -> None:
        tasks = [task for s in self.streams.values() for task in (s.task, s.preview_task) if task]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        self.streams.clear()

    async def subscribe(self, camera_id: int) -> asyncio.Queue[bytes | None]:
        stream = self.streams.get(camera_id)
        if stream is None:
            raise KeyError(camera_id)
        return await stream.subscribe()

    async def unsubscribe(self, camera_id: int, queue: asyncio.Queue[bytes | None]) -> None:
        stream = self.streams.get(camera_id)
        if stream:
            await stream.unsubscribe(queue)

    def statuses(self) -> list[dict[str, Any]]:
        return [self.streams[i].status() if i in self.streams else
                {"id": i, "state": "disabled", "codec": None, "viewers": 0, "error": None}
                for i in range(len(config.cameras))]


manager = StreamManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global config
    config = load_config()
    await manager.start()
    yield
    await manager.stop()


app = FastAPI(title="CamWall", version="2.1.0", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
if not SESSION_SECRET:
    raise RuntimeError("CAMWALL_SESSION_SECRET is not configured")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, session_cookie="camwall_session",
                   max_age=30 * 24 * 60 * 60, same_site="strict",
                   https_only=os.getenv("CAMWALL_COOKIE_SECURE", "0") == "1")
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


def auth_record() -> dict[str, str]:
    try:
        data = json.loads(AUTH_PATH.read_text(encoding="utf-8"))
        if not isinstance(data.get("username"), str) or not isinstance(data.get("password_hash"), str):
            raise ValueError("invalid auth file")
        return data
    except Exception as exc:
        log.exception("Cannot read auth configuration")
        raise HTTPException(status_code=503, detail="Authentication is not configured") from exc


def require_same_origin(request: Request) -> None:
    from urllib.parse import urlsplit
    origin = request.headers.get("origin", "")
    parsed = urlsplit(origin)
    host = request.headers.get("host", "")
    if not origin or parsed.netloc != host or parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=403, detail="Invalid request origin")


def require_login(request: Request) -> str:
    username = request.session.get("username")
    issued_at = request.session.get("issued_at")
    remember = bool(request.session.get("remember", False))
    max_age = 30 * 24 * 60 * 60 if remember else 12 * 60 * 60
    if not username or not isinstance(issued_at, (int, float)) or time.time() - issued_at > max_age:
        request.session.clear()
        raise HTTPException(status_code=401, detail="Authentication required")
    return str(username)


class LoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)
    remember: bool = False


class FavoritePayload(BaseModel):
    favorite: bool


class PasswordPayload(BaseModel):
    current_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=8, max_length=256)


@app.post("/api/auth/login")
async def login(payload: LoginPayload, request: Request):
    require_same_origin(request)
    now = asyncio.get_running_loop().time()
    ip = request.client.host if request.client else "unknown"
    attempts, blocked_until = LOGIN_ATTEMPTS.get(ip, (0, 0.0))
    if now < blocked_until:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    record = auth_record()
    valid = payload.username == record["username"] and PASSWORD_HASHER.verify(payload.password, record["password_hash"])
    if not valid:
        attempts += 1
        LOGIN_ATTEMPTS[ip] = (attempts, now + 300 if attempts >= 5 else 0.0)
        raise HTTPException(status_code=401, detail="Невірний логін або пароль")
    LOGIN_ATTEMPTS.pop(ip, None)
    request.session.clear()
    request.session["username"] = record["username"]
    request.session["issued_at"] = time.time()
    request.session["remember"] = payload.remember
    return {"ok": True, "username": record["username"]}


@app.get("/api/auth/me")
async def auth_me(request: Request):
    try:
        username = require_login(request)
        return {"authenticated": True, "username": username}
    except HTTPException:
        return {"authenticated": False, "username": None}


@app.post("/api/auth/logout")
async def logout(request: Request):
    require_same_origin(request)
    require_login(request)
    request.session.clear()
    return {"ok": True}


@app.post("/api/auth/password")
async def change_password(payload: PasswordPayload, request: Request):
    require_same_origin(request)
    username = require_login(request)
    record = auth_record()
    if username != record["username"] or not PASSWORD_HASHER.verify(payload.current_password, record["password_hash"]):
        raise HTTPException(status_code=403, detail="Поточний пароль неправильний")
    record["password_hash"] = PASSWORD_HASHER.hash(payload.new_password)
    _atomic_write_json(AUTH_PATH, record, 0o600)
    request.session.clear()
    return {"ok": True}


@app.get("/")
async def index():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/health")
async def health():
    ingest = manager.statuses()
    enabled_ids = [i for i, camera in enumerate(config.cameras) if camera.enabled]
    healthy = all(i < len(ingest) and ingest[i]["state"] == "live" for i in enabled_ids)
    return {"status": "ok" if healthy else "degraded", "cameras": len(config.cameras),
            "ffmpeg": FFMPEG, "ingest": ingest}


@app.get("/api/config")
async def get_config(request: Request):
    require_login(request)
    # Includes RTSP credentials for the settings editor. Restrict this app to trusted networks.
    async with config_lock:
        return config.model_dump()


@app.get("/api/streams")
async def get_streams(request: Request):
    require_login(request)
    return manager.statuses()


@app.get("/api/preview/{camera_id}")
async def get_preview(camera_id: int, request: Request):
    require_login(request)
    stream = manager.streams.get(camera_id)
    if stream is None or stream.preview is None:
        raise HTTPException(status_code=404, detail="Preview is not ready")
    return Response(content=stream.preview, media_type="image/jpeg",
                    headers={"Cache-Control": "no-store, max-age=0"})


@app.patch("/api/cameras/{camera_id}/favorite")
async def set_favorite(camera_id: int, payload: FavoritePayload, request: Request):
    require_same_origin(request)
    require_login(request)
    async with config_lock:
        if camera_id < 0 or camera_id >= len(config.cameras):
            raise HTTPException(status_code=404, detail="Camera not found")
        config.cameras[camera_id].favorite = payload.favorite
        save_config(config)
    return {"ok": True, "favorite": payload.favorite}


@app.put("/api/config")
async def put_config(payload: dict[str, Any], request: Request):
    global config
    require_same_origin(request)
    require_login(request)
    try:
        candidate = ConfigModel.model_validate(payload)
        for index, camera in enumerate(candidate.cameras, 1):
            if not camera.url.startswith(("rtsp://", "rtsps://")):
                raise ValueError(f"Camera #{index}: URL must start with rtsp:// or rtsps://")
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    async with config_lock:
        previous = config
        save_config(candidate)
        config = candidate
        await manager.apply_config(previous, candidate)
    return {"ok": True, "cameras": len(config.cameras)}


@app.websocket("/ws/stream/{camera_id}")
async def stream(websocket: WebSocket, camera_id: int):
    origin = websocket.headers.get("origin", "")
    host = websocket.headers.get("host", "")
    from urllib.parse import urlsplit
    parsed_origin = urlsplit(origin)
    if not origin or parsed_origin.netloc != host or parsed_origin.scheme not in ("http", "https"):
        await websocket.close(code=1008, reason="Invalid origin")
        return
    session = websocket.scope.get("session", {})
    issued_at = session.get("issued_at")
    max_age = 30 * 24 * 60 * 60 if session.get("remember") else 12 * 60 * 60
    if (not session.get("username") or not isinstance(issued_at, (int, float))
            or time.time() - issued_at > max_age):
        await websocket.close(code=1008, reason="Authentication required")
        return
    await websocket.accept()
    if camera_id < 0 or camera_id >= len(config.cameras) or not config.cameras[camera_id].enabled:
        await websocket.close(code=1008, reason="Unknown or disabled camera")
        return
    try:
        queue = await manager.subscribe(camera_id)
    except KeyError:
        await websocket.close(code=1011, reason="Camera stream is not available")
        return
    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                await websocket.close(code=1013, reason="Viewer cannot keep up with live stream")
                break
            await websocket.send_bytes(chunk)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        await manager.unsubscribe(camera_id, queue)
