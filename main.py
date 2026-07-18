import os, uuid, json, base64, asyncio, secrets, logging, shutil, tempfile, time
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Request, UploadFile, File, Form, Query, Cookie, status
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
import aiosqlite, uvicorn

# ---------- Настройка логирования ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- Переменные окружения ----------
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
API_KEY = os.getenv("API_KEY", "default-api-key-change-me")
SECRET_KEY = os.getenv("SECRET_KEY", "secret-key-for-sessions")
DEVICE_PASSWORD = os.getenv("DEVICE_PASSWORD", "standoff-soft_671692")
SESSION_TIMEOUT = timedelta(hours=24)
MAX_INLINE_FILE_SIZE = 2 * 1024 * 1024
MAX_BIG_FILE_SIZE = 500 * 1024 * 1024
TEMP_DIR = os.path.join(tempfile.gettempdir(), "hssu_uploads")
os.makedirs(TEMP_DIR, exist_ok=True)
DATABASE_URL = "hssucontrol.db"
UPLOAD_EXPIRE_SECONDS = 3600

# ---------- Lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE commands_queue SET status='pending' WHERE status='in_progress'")
        await db.commit()
    yield
    for ws in list(stream_connections.values()):
        try: await ws.close(code=1000)
        except: pass
    stream_connections.clear()
    for views in view_connections.values():
        for ws in views:
            try: await ws.close(code=1000)
            except: pass
    view_connections.clear()
    for f in pending_commands.values():
        if not f.done(): f.cancel()
    pending_commands.clear()
    for fname in os.listdir(TEMP_DIR):
        try: os.remove(os.path.join(TEMP_DIR, fname))
        except: pass

app = FastAPI(lifespan=lifespan, title="HSSUPRavle Control", version="2.3", docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ---------- Глобальные состояния ----------
stream_connections: Dict[str, WebSocket] = {}
view_connections: Dict[str, Set[WebSocket]] = {}
pending_commands: Dict[str, asyncio.Future] = {}
uploads: Dict[str, dict] = {}

# ---------- БД ----------
async def init_db():
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS devices(id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT UNIQUE NOT NULL,name TEXT NOT NULL,status TEXT DEFAULT 'inactive',last_seen TIMESTAMP,registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,user TEXT,action TEXT,device_id TEXT,details TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS commands_queue(id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT NOT NULL,command TEXT NOT NULL,status TEXT DEFAULT 'pending',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,result TEXT,file_path TEXT)""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_queue_device_status ON commands_queue(device_id,status)")
        await db.commit()

def generate_session_token(): return secrets.token_urlsafe(32)
def generate_command_id(): return str(uuid.uuid4())
def generate_upload_id(): return secrets.token_urlsafe(16)
def safe_path(path: str) -> str:
    if not path: return "/"
    n = os.path.normpath(path).replace('\\', '/')
    if n.startswith('..') or '/../' in n:
        raise HTTPException(400, "Invalid path")
    return '/' + n.lstrip('/')

async def get_device_or_404(device_id: str, db: aiosqlite.Connection) -> dict:
    cur = await db.execute("SELECT * FROM devices WHERE device_id=?", (device_id,))
    row = await cur.fetchone()
    if not row: raise HTTPException(404, "Device not found")
    return dict(row)

async def log_action(user: str, action: str, device_id: str = None, details: str = ""):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("INSERT INTO logs(user,action,device_id,details) VALUES(?,?,?,?)", (user, action, device_id, details))
        await db.commit()

async def get_current_user(session_token: Optional[str] = Cookie(None)):
    if not session_token: raise HTTPException(401, "Not authenticated")
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur = await db.execute("SELECT * FROM sessions WHERE token=?", (session_token,))
        if not await cur.fetchone(): raise HTTPException(401, "Invalid session")
    return session_token

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
async def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY: raise HTTPException(403, "Invalid API Key")
    return api_key

# ---------- Очередь команд ----------
async def enqueue_command(device_id: str, command: dict, file_data: Optional[bytes] = None) -> int:
    command_json = json.dumps(command)
    file_path = None
    if file_data:
        filename = f"{device_id}_{command.get('command_id', generate_command_id())}_{int(datetime.now().timestamp())}"
        file_path = os.path.join(TEMP_DIR, filename)
        with open(file_path, "wb") as f: f.write(file_data)
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur = await db.execute("INSERT INTO commands_queue(device_id,command,status,file_path) VALUES(?,?,'pending',?)", (device_id, command_json, file_path))
        await db.commit()
        return cur.lastrowid

async def get_pending_command(device_id: str) -> Optional[dict]:
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur = await db.execute("SELECT id,command,file_path FROM commands_queue WHERE device_id=? AND status='pending' ORDER BY created_at LIMIT 1", (device_id,))
        row = await cur.fetchone()
        if not row: return None
        cmd_id, command_json, file_path = row[0], row[1], row[2]
        await db.execute("UPDATE commands_queue SET status='in_progress' WHERE id=?", (cmd_id,))
        await db.commit()
        command = json.loads(command_json)
        if file_path and os.path.exists(file_path):
            upload_id = generate_upload_id()
            uploads[upload_id] = {"path": file_path, "device_id": device_id, "expires": time.time() + UPLOAD_EXPIRE_SECONDS}
            command["file_url"] = f"/api/download/{upload_id}"
        command["_db_id"] = cmd_id
        return command

async def complete_command(db_id: int, result: dict, status: str = "success"):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE commands_queue SET status=?,result=? WHERE id=?", (status, json.dumps(result), db_id))
        await db.commit()

async def send_command_to_device(device_id: str, command: dict, timeout: float = 30.0, file_data: Optional[bytes] = None) -> dict:
    cmd_id = command.get("command_id", generate_command_id())
    command["command_id"] = cmd_id
    if device_id in stream_connections:
        ws = stream_connections[device_id]
        if file_data:
            if len(file_data) > MAX_INLINE_FILE_SIZE:
                filename = f"{device_id}_{cmd_id}_{int(time.time())}"
                file_path = os.path.join(TEMP_DIR, filename)
                with open(file_path, "wb") as f: f.write(file_data)
                upload_id = generate_upload_id()
                uploads[upload_id] = {"path": file_path, "device_id": device_id, "expires": time.time() + UPLOAD_EXPIRE_SECONDS}
                command["file_url"] = f"/api/download/{upload_id}"
            else:
                command["_file_data"] = base64.b64encode(file_data).decode('utf-8')
        future = asyncio.get_event_loop().create_future()
        pending_commands[cmd_id] = future
        try:
            await ws.send_text(json.dumps(command))
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            raise HTTPException(504, "Device response timeout")
        finally:
            pending_commands.pop(cmd_id, None)
    else:
        db_id = await enqueue_command(device_id, command, file_data)
        raise HTTPException(202, "Device offline, command queued", headers={"X-Command-ID": cmd_id, "X-Queue-ID": str(db_id)})

# ---------- Скачивание временных файлов ----------
@app.get("/api/download/{upload_id}")
async def download_file(upload_id: str):
    upload = uploads.get(upload_id)
    if not upload: raise HTTPException(404, "Upload not found")
    if time.time() > upload["expires"]:
        del uploads[upload_id]; raise HTTPException(410, "Expired")
    file_path = upload["path"]
    if not os.path.exists(file_path):
        del uploads[upload_id]; raise HTTPException(404, "File not found")
    async def file_generator():
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192): yield chunk
        finally:
            try: os.remove(file_path)
            except: pass
            uploads.pop(upload_id, None)
    return StreamingResponse(file_generator(), media_type="application/octet-stream", headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"})

# ---------- WebSocket ----------
@app.websocket("/ws/stream/{device_id}")
async def ws_stream(websocket: WebSocket, device_id: str):
    await websocket.accept()
    async with aiosqlite.connect(DATABASE_URL) as db:
        try: await get_device_or_404(device_id, db)
        except: await websocket.close(1008, "Not registered"); return
    if device_id in stream_connections:
        try: await stream_connections[device_id].close(1000)
        except: pass
    stream_connections[device_id] = websocket
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE devices SET status='active',last_seen=CURRENT_TIMESTAMP WHERE device_id=?", (device_id,))
        await db.commit()
    logger.info(f"Device {device_id} connected via WebSocket")
    try:
        while True:
            msg = await websocket.receive()
            if "text" in msg:
                try:
                    data = json.loads(msg["text"])
                    cid = data.get("command_id")
                    if cid and cid in pending_commands:
                        f = pending_commands[cid]
                        if not f.done(): f.set_result(data)
                except: pass
            elif "bytes" in msg:
                if device_id in view_connections:
                    for v in view_connections[device_id]:
                        try: await v.send_bytes(msg["bytes"])
                        except: pass
    except WebSocketDisconnect: pass
    finally:
        stream_connections.pop(device_id, None)
        async with aiosqlite.connect(DATABASE_URL) as db:
            await db.execute("UPDATE devices SET status='inactive' WHERE device_id=?", (device_id,))
            await db.commit()
        logger.info(f"Device {device_id} disconnected")
        if device_id in view_connections:
            for v in view_connections[device_id]:
                try: await v.close(1000)
                except: pass
            del view_connections[device_id]

@app.websocket("/ws/view/{device_id}")
async def ws_view(websocket: WebSocket, device_id: str, session_token: Optional[str] = Cookie(None)):
    if not session_token: await websocket.close(1008, "Unauthorized"); return
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur = await db.execute("SELECT * FROM sessions WHERE token=?", (session_token,))
        if not await cur.fetchone(): await websocket.close(1008, "Invalid session"); return
    await websocket.accept()
    async with aiosqlite.connect(DATABASE_URL) as db:
        try: await get_device_or_404(device_id, db)
        except: await websocket.close(1008, "Device not found"); return
    view_connections.setdefault(device_id, set()).add(websocket)
    try:
        while True:
            msg = await websocket.receive_text()
            try: command = json.loads(msg)
            except: await websocket.send_text(json.dumps({"error": "Invalid JSON"})); continue
            if device_id not in stream_connections:
                await websocket.send_text(json.dumps({"error": "Device offline"})); continue
            if "command_id" not in command: command["command_id"] = generate_command_id()
            try:
                result = await send_command_to_device(device_id, command, 30.0)
                await websocket.send_text(json.dumps(result))
            except HTTPException as e:
                if e.status_code == 202:
                    await websocket.send_text(json.dumps({"status": "queued", "command_id": command["command_id"], "detail": e.detail}))
                else: raise
    except WebSocketDisconnect: pass
    finally:
        if device_id in view_connections:
            view_connections[device_id].discard(websocket)
            if not view_connections[device_id]: del view_connections[device_id]

# ---------- API (поддержка JSON и Form) ----------
@app.post("/api/command/poll")
async def poll_commands(request: Request):
    # Принимаем и JSON, и Form
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
        device_id = data.get("device_id")
    else:
        form = await request.form()
        device_id = form.get("device_id")
    if not device_id:
        raise HTTPException(400, "Missing device_id")
    logger.info(f"Poll from device {device_id}")
    cmd = await get_pending_command(device_id)
    if not cmd: return {"status": "no_commands"}
    db_id = cmd.pop("_db_id")
    return {"status": "command", "command": cmd, "_db_id": db_id}

@app.post("/api/command/result")
async def command_result(request: Request):
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
        device_id = data.get("device_id")
        db_id = data.get("db_id")
        result = data.get("result", "{}")
        status = data.get("status", "success")
    else:
        form = await request.form()
        device_id = form.get("device_id")
        db_id = form.get("db_id")
        result = form.get("result", "{}")
        status = form.get("status", "success")
    if not device_id or not db_id:
        raise HTTPException(400, "Missing device_id or db_id")
    try:
        db_id = int(db_id)
    except:
        raise HTTPException(400, "db_id must be integer")
    try: res = json.loads(result)
    except: res = {"raw": result}
    await complete_command(db_id, res, status)
    logger.info(f"Result from device {device_id} for command {db_id}")
    return {"status": "ok"}

@app.post("/api/register")
async def register(request: Request):
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
        device_id = data.get("device_id")
        device_name = data.get("device_name")
        password = data.get("password")
    else:
        form = await request.form()
        device_id = form.get("device_id")
        device_name = form.get("device_name")
        password = form.get("password")
    if not device_id or not device_name or not password:
        raise HTTPException(400, "Missing fields")
    if password != DEVICE_PASSWORD:
        raise HTTPException(403, "Invalid device password")
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur = await db.execute("SELECT * FROM devices WHERE device_id=?", (device_id,))
        if await cur.fetchone():
            await db.execute("UPDATE devices SET name=?,status='active',last_seen=CURRENT_TIMESTAMP WHERE device_id=?", (device_name, device_id))
            await db.commit()
            logger.info(f"Device {device_id} ({device_name}) updated")
            return {"status": "updated"}
        else:
            await db.execute("INSERT INTO devices(device_id,name,status,last_seen) VALUES(?,?,'active',CURRENT_TIMESTAMP)", (device_id, device_name))
            await db.commit()
            logger.info(f"Device {device_id} ({device_name}) registered")
            return {"status": "registered"}

@app.post("/api/heartbeat")
async def heartbeat(request: Request):
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
        device_id = data.get("device_id")
        status = data.get("status", "active")
    else:
        form = await request.form()
        device_id = form.get("device_id")
        status = form.get("status", "active")
    if not device_id:
        raise HTTPException(400, "Missing device_id")
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE devices SET status=?,last_seen=CURRENT_TIMESTAMP WHERE device_id=?", (status, device_id))
        await db.commit()
    logger.debug(f"Heartbeat from {device_id}")
    return {"status": "ok"}

# ---------- Остальные эндпоинты (без изменений) ----------
# ... (здесь все /api/devices, /api/v1/devices, /api/device/... и т.д. – они уже были в коде, я не буду дублировать, но они должны остаться)

# Для полноты, если вы копируете этот код, вставьте сюда все остальные эндпоинты (get_devices, list_files, download_file_rest, upload_file_small, upload_big, delete_file, rename_file, install_apk, touch, swipe, shell, broadcast, delete_device_api, delete_device_web, /api/docs, /openapi.json, /health) – они идентичны предыдущей версии. Я приведу их в финальном сниппете.

# ---------- Веб-интерфейс ----------
# (HTML-страницы без изменений, можете оставить как было)

# ---------- Запуск ----------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)