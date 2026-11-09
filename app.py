from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
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
HLS_DIR = Path(os.getenv("CAMWALL_HLS_DIR", str(BASE_DIR / "hls")))
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
    """One persistent RTSP-to-HLS FFmpeg process per enabled camera; no transcoding."""
    def __init__(self, camera_id: int):
        self.camera_id = camera_id
        self.task: asyncio.Task | None = None
        self.preview_task: asyncio.Task | None = None
        self.probe_task: asyncio.Task | None = None
        self.process: asyncio.subprocess.Process | None = None
        self.hls_dir = HLS_DIR / str(camera_id)
        self.preview: bytes | None = None
        self.preview_etag: str | None = None
        self.ffmpeg_stderr: deque[str] = deque(maxlen=20)
        self.state = "starting"
        self.codec = "unknown"
        self.width: int | None = None
        self.height: int | None = None
        self.last_error: str | None = None
        self.last_data_at: float | None = None
        self.probe_attempted = False

    async def probe(self, url: str) -> None:
        args = [FFPROBE, "-v", "error", "-rtsp_transport", "tcp", "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height", "-of", "json", url]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=8)
            if proc.returncode != 0:
                detail = (stderr or b"").decode("utf-8", "replace").strip()
                detail = re.sub(r"(?i)(rtsps?://)[^/@\s]+@", r"\1[credentials]@", detail)
                self.last_error = "ffprobe failed" + (f": {detail[-500:]}" if detail else "")
                log.warning("Codec probe failed camera=%d exit=%s: %s",
                            self.camera_id, proc.returncode, detail[-500:] or "(no details)")
                return
            data = json.loads(stdout or b"{}")
            streams = data.get("streams") or []
            if streams:
                stream = streams[0]
                self.codec = str(stream.get("codec_name") or "unknown").lower()
                self.width = stream.get("width")
                self.height = stream.get("height")
            else:
                self.last_error = "ffprobe: video stream not found"
        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                proc.kill()
                with suppress(Exception):
                    await proc.wait()
            self.last_error = "ffprobe timed out after 8 seconds"
            log.warning("Codec probe timed out camera=%d", self.camera_id)
        except asyncio.CancelledError:
            if proc and proc.returncode is None:
                proc.kill()
                with suppress(Exception):
                    await proc.wait()
            raise
        except Exception as exc:
            self.last_error = f"ffprobe: {type(exc).__name__}"
            log.warning("Codec probe failed camera=%d: %s", self.camera_id, type(exc).__name__)

    async def capture_stderr(self, reader: asyncio.StreamReader | None) -> None:
        if reader is None:
            return
        while line := await reader.readline():
            message = line.decode("utf-8", "replace").strip()
            if not message:
                continue
            message = re.sub(r"(?i)(rtsps?://)[^/@\s]+@", r"\1[credentials]@", message)
            self.ffmpeg_stderr.append(message[-1000:])

    async def preview_loop(self) -> None:
        """Create an occasional JPEG preview from the latest already-generated HLS segment."""
        await asyncio.sleep(5 + (self.camera_id % 7) * 2)
        while True:
            await asyncio.sleep(25)
            try:
                segments = sorted(self.hls_dir.glob("segment_*.m4s"), key=lambda p: p.stat().st_mtime_ns)
                if not segments:
                    continue
                segment = segments[-1]
                if segment.stat().st_size < 188 * 10:
                    continue
                init_segment = self.hls_dir / "init.mp4"
                if not init_segment.is_file():
                    continue
                preview_input = self.hls_dir / ".preview-input.mp4"
                preview_input.write_bytes(init_segment.read_bytes() + segment.read_bytes())
                proc = None
                try:
                    args = [FFMPEG, "-hide_banner", "-loglevel", "error",
                            "-skip_frame", "nokey", "-i", str(preview_input), "-an", "-frames:v", "1",
                            "-vf", "scale=480:-2", "-q:v", "7",
                            "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"]
                    proc = await asyncio.create_subprocess_exec(
                        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
                    if stdout.startswith(bytes((255, 216))):
                        self.preview = stdout
                        self.preview_etag = hashlib.sha256(stdout).hexdigest()
                except asyncio.CancelledError:
                    if proc and proc.returncode is None:
                        proc.kill()
                        with suppress(Exception):
                            await proc.wait()
                    raise
                finally:
                    preview_input.unlink(missing_ok=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug("Preview generation skipped camera=%d: %s", self.camera_id, exc)

    async def run(self) -> None:
        while True:
            camera = config.cameras[self.camera_id] if self.camera_id < len(config.cameras) else None
            if camera is None or not camera.enabled:
                self.state = "disabled"
                return
            self.state = "connecting"
            self.last_data_at = None
            self.ffmpeg_stderr.clear()
            self.codec = "unknown"
            self.width = None
            self.height = None
            self.hls_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.rmtree(self.hls_dir, ignore_errors=True)
            self.hls_dir.mkdir(parents=True, exist_ok=True)
            # ffprobe is diagnostic only. Never block or gate the RTSP-to-HLS pipeline.
            self.probe_attempted = False
            segment_seconds = "1" if config.settings.low_latency else "2"
            args = [FFMPEG, "-hide_banner", "-loglevel", "warning", "-nostdin",
                    "-rtsp_transport", "tcp", "-fflags", "nobuffer", "-flags", "low_delay",
                    "-i", camera.url, "-map", "0:v:0", "-c:v", "copy"]
            if camera.audio:
                args += ["-map", "0:a:0?", "-c:a", "copy"]
            else:
                args += ["-an"]
            args += ["-f", "hls", "-hls_time", segment_seconds, "-hls_list_size", "6",
                     "-hls_delete_threshold", "2",
                     "-hls_flags", "delete_segments+omit_endlist+independent_segments+temp_file",
                     "-hls_segment_type", "fmp4", "-hls_fmp4_init_filename", "init.mp4",
                     "-hls_segment_filename", str(self.hls_dir / "segment_%09d.m4s"),
                     str(self.hls_dir / "playlist.m3u8")]
            proc = None
            stderr_task = None
            last_segment_mtime = 0
            start_time = time.monotonic()
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE, start_new_session=True)
                self.process = proc
                stderr_task = asyncio.create_task(self.capture_stderr(proc.stderr),
                                                  name=f"camwall-stderr-{self.camera_id}")
                log.info("RTSP-to-HLS FFmpeg started camera=%d codec=%s pid=%d segment=%ss",
                         self.camera_id, self.codec, proc.pid, segment_seconds)
                while proc.returncode is None:
                    await asyncio.sleep(1)
                    segments = list(self.hls_dir.glob("segment_*.m4s"))
                    if segments:
                        newest = max(segments, key=lambda p: p.stat().st_mtime_ns)
                        mtime = newest.stat().st_mtime_ns
                        if newest.stat().st_size > 0 and mtime != last_segment_mtime:
                            last_segment_mtime = mtime
                            self.last_data_at = time.monotonic()
                            if self.state != "live":
                                self.state = "live"
                                self.last_error = None
                                log.info("HLS segment produced camera=%d", self.camera_id)
                                if not self.probe_attempted and self.probe_task is None:
                                    self.probe_attempted = True
                                    self.probe_task = asyncio.create_task(
                                        self.probe(camera.url), name=f"camwall-probe-{self.camera_id}")
                    idle = (time.monotonic() - self.last_data_at
                            if self.last_data_at is not None else time.monotonic() - start_time)
                    if idle > 30:
                        self.last_error = "No HLS segment produced for 30 seconds"
                        log.warning("HLS output stalled camera=%d pid=%d; restarting FFmpeg",
                                    self.camera_id, proc.pid)
                        break
                if proc.returncode is not None:
                    if stderr_task:
                        await stderr_task
                    details = " | ".join(self.ffmpeg_stderr)
                    self.last_error = f"FFmpeg exited with code {proc.returncode}" + (f": {details}" if details else "")
                    log.warning("FFmpeg exited camera=%d code=%s stderr=%s",
                                self.camera_id, proc.returncode, details or "(empty)")
            except FileNotFoundError:
                self.state = "error"
                self.last_error = "FFmpeg executable not found"
                log.exception("FFmpeg executable not found: %s", FFMPEG)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                log.exception("HLS stream failed camera=%d", self.camera_id)
            finally:
                self.process = None
                if self.probe_task and not self.probe_task.done():
                    self.probe_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await self.probe_task
                self.probe_task = None
                if proc is not None and proc.returncode is None:
                    with suppress(ProcessLookupError):
                        proc.send_signal(signal.SIGTERM)
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
                if stderr_task and not stderr_task.done():
                    with suppress(asyncio.CancelledError):
                        await stderr_task
                if self.last_error and self.last_error.startswith("No HLS segment produced"):
                    details = " | ".join(self.ffmpeg_stderr)
                    if details:
                        self.last_error += f": {details}"
                        log.warning("HLS stalled camera=%d stderr=%s", self.camera_id, details)
            self.state = "reconnecting"
            await asyncio.sleep(config.settings.reconnect_seconds)

    def status(self) -> dict[str, Any]:
        idle_seconds = (round(time.monotonic() - self.last_data_at, 1)
                        if self.last_data_at is not None else None)
        effective_state = "stalled" if self.state == "live" and idle_seconds is not None and idle_seconds > 15 else self.state
        return {"id": self.camera_id, "state": effective_state, "codec": self.codec,
                "width": self.width, "height": self.height,
                "last_data_seconds_ago": idle_seconds, "error": self.last_error}


class StreamManager:
    def __init__(self):
        self.streams: dict[int, CameraStream] = {}

    async def _start_camera(self, camera_id: int) -> None:
        stream = CameraStream(camera_id)
        self.streams[camera_id] = stream
        stream.task = asyncio.create_task(stream.run(), name=f"camwall-camera-{camera_id}")
        stream.preview_task = asyncio.create_task(stream.preview_loop(), name=f"camwall-preview-{camera_id}")

    async def start(self) -> None:
        HLS_DIR.mkdir(parents=True, exist_ok=True)
        for camera_id, camera in enumerate(config.cameras):
            if camera.enabled:
                await self._start_camera(camera_id)
                # Avoid a burst of simultaneous RTSP handshakes on camera startup.
                await asyncio.sleep(0.35)

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
                if stream.probe_task:
                    stream.probe_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await stream.probe_task
            if new and new.enabled:
                await self._start_camera(camera_id)

    async def stop(self) -> None:
        tasks = [task for s in self.streams.values() for task in (s.task, s.preview_task, s.probe_task) if task]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        self.streams.clear()

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


app = FastAPI(title="CamWall", version="2.2.0", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
if not SESSION_SECRET:
    raise RuntimeError("CAMWALL_SESSION_SECRET is not configured")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, session_cookie="camwall_session",
                   max_age=30 * 24 * 60 * 60, same_site="strict",
                   https_only=os.getenv("CAMWALL_COOKIE_SECURE", "0") == "1")
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

@app.middleware("http")
async def response_cache_policy(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/api/preview/") and response.status_code >= 400:
        response.headers["Cache-Control"] = "no-store"
    elif (path.startswith("/api/") and not path.startswith("/api/preview/")) or path == "/health" or path == "/":
        response.headers["Cache-Control"] = "no-store"
    elif path.startswith("/static/vendor/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    elif path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=3600, must-revalidate"
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    return response


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
    etag = f'"{stream.preview_etag}"' if stream.preview_etag else None
    if etag and request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers={
            "ETag": etag, "Cache-Control": "private, max-age=20"
        })
    headers = {"Cache-Control": "private, max-age=20, stale-while-revalidate=10"}
    if etag:
        headers["ETag"] = etag
    return Response(content=stream.preview, media_type="image/jpeg", headers=headers)


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


@app.get("/hls/{camera_id}/{filename}")
async def get_hls_file(camera_id: int, filename: str, request: Request):
    require_login(request)
    if filename not in ("playlist.m3u8", "init.mp4") and not re.fullmatch(r"segment_\d{9}\.m4s", filename):
        raise HTTPException(status_code=404, detail="HLS resource not found")
    if camera_id < 0 or camera_id >= len(config.cameras) or not config.cameras[camera_id].enabled:
        raise HTTPException(status_code=404, detail="Camera not found")
    path = HLS_DIR / str(camera_id) / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="HLS resource is not ready")
    media_type = ("application/vnd.apple.mpegurl" if filename == "playlist.m3u8"
                  else "video/mp4")
    cache_control = "no-store, max-age=0" if filename == "playlist.m3u8" else "private, no-cache"
    return FileResponse(path, media_type=media_type,
                        headers={"Cache-Control": cache_control, "X-Content-Type-Options": "nosniff"})


