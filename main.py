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

# ========== НАСТРОЙКА ЛОГИРОВАНИЯ ==========
LOG_FILE = "app.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== ПЕРЕМЕННЫЕ ==========
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

# ========== LIFESPAN ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=== SERVER STARTING ===")
    await init_db()
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE commands_queue SET status='pending' WHERE status='in_progress'")
        await db.commit()
    yield
    logger.info("=== SERVER SHUTTING DOWN ===")
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

# ========== ГЛОБАЛЬНЫЕ СОСТОЯНИЯ ==========
stream_connections: Dict[str, WebSocket] = {}
view_connections: Dict[str, Set[WebSocket]] = {}
pending_commands: Dict[str, asyncio.Future] = {}
uploads: Dict[str, dict] = {}

# ========== БД ==========
async def init_db():
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS devices(id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT UNIQUE NOT NULL,name TEXT NOT NULL,status TEXT DEFAULT 'inactive',last_seen TIMESTAMP,registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,user TEXT,action TEXT,device_id TEXT,details TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS commands_queue(id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT NOT NULL,command TEXT NOT NULL,status TEXT DEFAULT 'pending',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,result TEXT,file_path TEXT)""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_queue_device_status ON commands_queue(device_id,status)")
        await db.commit()
    logger.info("Database initialized")

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

# ========== ОЧЕРЕДЬ КОМАНД ==========
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

# ========== СКАЧИВАНИЕ ВРЕМЕННЫХ ФАЙЛОВ ==========
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

# ========== WEBSOCKET ==========
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

# ========== API ==========
@app.post("/api/command/poll")
async def poll_commands(request: Request):
    logger.info("=== POLL REQUEST ===")
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
        device_id = data.get("device_id")
        logger.info(f"JSON poll: {data}")
    else:
        form = await request.form()
        device_id = form.get("device_id")
        logger.info(f"Form poll: {dict(form)}")
    if not device_id:
        raise HTTPException(400, "Missing device_id")
    cmd = await get_pending_command(device_id)
    if not cmd: return {"status": "no_commands"}
    db_id = cmd.pop("_db_id")
    return {"status": "command", "command": cmd, "_db_id": db_id}

@app.post("/api/command/result")
async def command_result(request: Request):
    logger.info("=== COMMAND RESULT ===")
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
    logger.info(f"Result from {device_id} for command {db_id}")
    return {"status": "ok"}

@app.post("/api/register")
async def register(request: Request):
    logger.info("=== REGISTER REQUEST RECEIVED ===")
    try:
        if request.headers.get("content-type") == "application/json":
            data = await request.json()
            device_id = data.get("device_id")
            device_name = data.get("device_name")
            password = data.get("password")
            logger.info(f"JSON registration: device_id={device_id}, device_name={device_name}, password={password[:4]}***")
        else:
            form = await request.form()
            device_id = form.get("device_id")
            device_name = form.get("device_name")
            password = form.get("password")
            logger.info(f"Form registration: device_id={device_id}, device_name={device_name}, password={password[:4] if password else 'None'}***")
    except Exception as e:
        logger.error(f"Error reading registration request: {e}")
        raise HTTPException(400, "Invalid request format")

    if not device_id or not device_name or not password:
        logger.error("Missing fields in registration")
        raise HTTPException(400, "Missing fields")

    if password != DEVICE_PASSWORD:
        logger.error(f"Invalid password for device {device_id}")
        raise HTTPException(403, "Invalid device password")

    async with aiosqlite.connect(DATABASE_URL) as db:
        cur = await db.execute("SELECT * FROM devices WHERE device_id=?", (device_id,))
        if await cur.fetchone():
            await db.execute("UPDATE devices SET name=?,status='active',last_seen=CURRENT_TIMESTAMP WHERE device_id=?", (device_name, device_id))
            await db.commit()
            logger.info(f"Device {device_id} ({device_name}) UPDATED")
            return {"status": "updated"}
        else:
            await db.execute("INSERT INTO devices(device_id,name,status,last_seen) VALUES(?,?,'active',CURRENT_TIMESTAMP)", (device_id, device_name))
            await db.commit()
            logger.info(f"Device {device_id} ({device_name}) REGISTERED")
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
    return {"status": "ok"}

# ========== ДОПОЛНИТЕЛЬНЫЕ ЭНДПОИНТЫ ==========
@app.get("/api/devices")
async def get_devices(session: str = Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM devices ORDER BY registered_at DESC")
        rows = await cur.fetchall()
        devices = [dict(r) for r in rows]
        for d in devices:
            d["online"] = d["device_id"] in stream_connections
            cur2 = await db.execute("SELECT COUNT(*) FROM commands_queue WHERE device_id=? AND status='pending'", (d["device_id"],))
            cnt = await cur2.fetchone()
            d["queue_count"] = cnt[0] if cnt else 0
        return devices

@app.get("/api/v1/devices")
async def get_devices_api(api_key: str = Depends(verify_api_key)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM devices ORDER BY registered_at DESC")
        rows = await cur.fetchall()
        devices = [dict(r) for r in rows]
        for d in devices:
            d["online"] = d["device_id"] in stream_connections
            cur2 = await db.execute("SELECT COUNT(*) FROM commands_queue WHERE device_id=? AND status='pending'", (d["device_id"],))
            cnt = await cur2.fetchone()
            d["queue_count"] = cnt[0] if cnt else 0
        return devices

@app.get("/api/device/{device_id}/files")
async def list_files(device_id: str, path: str = Query("/"), session: str = Depends(get_current_user)):
    path = safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "list_files", "path": path})
        if res.get("status") == "success": return res.get("result", [])
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.get("/api/device/{device_id}/file")
async def download_file_rest(device_id: str, path: str = Query(...), session: str = Depends(get_current_user)):
    path = safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "download_file", "path": path}, 60.0)
        if res.get("status") != "success": raise HTTPException(500, res.get("error", "Unknown"))
        data_b64 = res.get("result", {}).get("data")
        if not data_b64: raise HTTPException(500, "No data")
        content = base64.b64decode(data_b64)
        return StreamingResponse(iter([content]), media_type="application/octet-stream", headers={"Content-Disposition": f"attachment; filename={os.path.basename(path)}"})
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/device/{device_id}/file")
async def upload_file_small(device_id: str, file: UploadFile = File(...), path: str = Form(...), overwrite: bool = Form(True), session: str = Depends(get_current_user)):
    path = safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    content = await file.read()
    if len(content) > MAX_INLINE_FILE_SIZE:
        raise HTTPException(413, f"File too large (max {MAX_INLINE_FILE_SIZE//1024//1024} MB) - use /upload_big")
    try:
        res = await send_command_to_device(device_id, {"command": "upload_file", "path": path, "overwrite": overwrite}, 60.0, file_data=content)
        if res.get("status") == "success":
            await log_action(session, "upload_file", device_id, f"Uploaded {file.filename} to {path}")
            return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            await log_action(session, "upload_file_queued", device_id, f"Queued {path}")
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/device/{device_id}/upload_big")
async def upload_file_big(device_id: str, file: UploadFile = File(...), path: str = Form(...), overwrite: bool = Form(True), session: str = Depends(get_current_user)):
    path = safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    content = await file.read()
    if len(content) > MAX_BIG_FILE_SIZE: raise HTTPException(413, f"File too large (max {MAX_BIG_FILE_SIZE//1024//1024} MB)")
    try:
        res = await send_command_to_device(device_id, {"command": "upload_file", "path": path, "overwrite": overwrite}, 60.0, file_data=content)
        if res.get("status") == "success":
            await log_action(session, "upload_big", device_id, f"Uploaded {file.filename} to {path}")
            return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            await log_action(session, "upload_big_queued", device_id, f"Queued {path}")
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.delete("/api/device/{device_id}/file")
async def delete_file(device_id: str, path: str = Query(...), recursive: bool = False, session: str = Depends(get_current_user)):
    path = safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "delete_file", "path": path, "recursive": recursive})
        if res.get("status") == "success":
            await log_action(session, "delete_file", device_id, f"Deleted {path}")
            return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.put("/api/device/{device_id}/file")
async def rename_file(device_id: str, old_path: str = Query(...), new_path: str = Query(...), session: str = Depends(get_current_user)):
    old_path = safe_path(old_path); new_path = safe_path(new_path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "rename_file", "old_path": old_path, "new_path": new_path})
        if res.get("status") == "success":
            await log_action(session, "rename_file", device_id, f"Renamed {old_path} to {new_path}")
            return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/device/{device_id}/install_apk")
async def install_apk(device_id: str, apk_path: str = Form(...), session: str = Depends(get_current_user)):
    apk_path = safe_path(apk_path)
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "install_apk", "apk_path": apk_path}, 120.0)
        if res.get("status") == "success":
            await log_action(session, "install_apk", device_id, f"Installed {apk_path}")
            return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/device/{device_id}/touch")
async def touch(device_id: str, x: float = Form(...), y: float = Form(...), action: str = Form("click"), session: str = Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "touch", "x": x, "y": y, "action": action})
        if res.get("status") == "success": return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/device/{device_id}/swipe")
async def swipe(device_id: str, x1: float = Form(...), y1: float = Form(...), x2: float = Form(...), y2: float = Form(...), duration: int = Form(300), session: str = Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "swipe", "x1": x1, "y1": y1, "x2": x2, "y2": y2, "duration": duration})
        if res.get("status") == "success": return {"status": "ok"}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/device/{device_id}/shell")
async def shell(device_id: str, command: str = Form(...), session: str = Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db: await get_device_or_404(device_id, db)
    try:
        res = await send_command_to_device(device_id, {"command": "shell", "cmd": command}, 60.0)
        if res.get("status") == "success":
            await log_action(session, "shell", device_id, f"Executed: {command}")
            return {"status": "ok", "output": res.get("result", "")}
        raise HTTPException(500, res.get("error", "Unknown"))
    except HTTPException as e:
        if e.status_code == 202:
            return JSONResponse(202, {"status": "queued", "command_id": e.headers.get("X-Command-ID"), "detail": e.detail})
        raise

@app.post("/api/devices/command")
async def broadcast(command: dict, session: str = Depends(get_current_user)):
    if "command" not in command: raise HTTPException(400, "Missing 'command' field")
    results = {}
    for device_id, ws in stream_connections.items():
        try:
            cmd = command.copy()
            cmd["command_id"] = generate_command_id()
            future = asyncio.get_event_loop().create_future()
            pending_commands[cmd["command_id"]] = future
            await ws.send_text(json.dumps(cmd))
            results[device_id] = {"status": "sent"}
        except Exception as e: results[device_id] = {"error": str(e)}
    await log_action(session, "broadcast", details=f"Broadcasted: {command}")
    return {"status": "ok", "results": results}

@app.delete("/api/device/{device_id}")
async def delete_device_api(device_id: str, api_key: str = Depends(verify_api_key)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await get_device_or_404(device_id, db)
        await db.execute("DELETE FROM devices WHERE device_id=?", (device_id,))
        await db.execute("DELETE FROM commands_queue WHERE device_id=?", (device_id,))
        await db.commit()
    stream_connections.pop(device_id, None)
    if device_id in view_connections:
        for ws in view_connections[device_id]:
            try: await ws.close(1000)
            except: pass
        del view_connections[device_id]
    for fname in os.listdir(TEMP_DIR):
        if fname.startswith(device_id):
            try: os.remove(os.path.join(TEMP_DIR, fname))
            except: pass
    await log_action("API", "delete_device", device_id)
    return {"status": "ok"}

@app.delete("/api/web/device/{device_id}")
async def delete_device_web(device_id: str, session: str = Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await get_device_or_404(device_id, db)
        await db.execute("DELETE FROM devices WHERE device_id=?", (device_id,))
        await db.execute("DELETE FROM commands_queue WHERE device_id=?", (device_id,))
        await db.commit()
    stream_connections.pop(device_id, None)
    if device_id in view_connections:
        for ws in view_connections[device_id]:
            try: await ws.close(1000)
            except: pass
        del view_connections[device_id]
    for fname in os.listdir(TEMP_DIR):
        if fname.startswith(device_id):
            try: os.remove(os.path.join(TEMP_DIR, fname))
            except: pass
    await log_action(session, "delete_device", device_id)
    return {"status": "ok"}

@app.get("/api/docs", include_in_schema=False)
async def swagger_docs(session: str = Depends(get_current_user