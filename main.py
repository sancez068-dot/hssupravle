import os,uuid,json,base64,asyncio,secrets,logging,shutil,tempfile,time
from datetime import datetime,timedelta
from typing import Dict,Set,Optional,List
from contextlib import asynccontextmanager
from fastapi import FastAPI,WebSocket,WebSocketDisconnect,HTTPException,Depends,Request,UploadFile,File,Form,Query,Cookie,status
from fastapi.responses import HTMLResponse,StreamingResponse,JSONResponse,FileResponse
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
import aiosqlite,uvicorn

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD","admin123")
API_KEY=os.getenv("API_KEY","default-api-key-change-me")
SECRET_KEY=os.getenv("SECRET_KEY","secret-key-for-sessions")
DEVICE_PASSWORD=os.getenv("DEVICE_PASSWORD","standoff-soft_671692")
SESSION_TIMEOUT=timedelta(hours=24)
MAX_INLINE_FILE_SIZE=2*1024*1024
MAX_BIG_FILE_SIZE=500*1024*1024
TEMP_DIR=os.path.join(tempfile.gettempdir(),"hssu_uploads")
os.makedirs(TEMP_DIR,exist_ok=True)
DATABASE_URL="hssucontrol.db"
UPLOAD_EXPIRE_SECONDS=3600

@asynccontextmanager
async def lifespan(app:FastAPI):
    await init_db()
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE commands_queue SET status='pending' WHERE status='in_progress'")
        await db.commit()
    yield
    for ws in list(stream_connections.values()):
        try:await ws.close(code=1000)
        except:pass
    stream_connections.clear()
    for views in view_connections.values():
        for ws in views:
            try:await ws.close(code=1000)
            except:pass
    view_connections.clear()
    for f in pending_commands.values():
        if not f.done():f.cancel()
    pending_commands.clear()
    for fname in os.listdir(TEMP_DIR):
        try:os.remove(os.path.join(TEMP_DIR,fname))
        except:pass

app=FastAPI(lifespan=lifespan,title="HSSUPRavle Control",version="2.2",docs_url=None,redoc_url=None)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])
if not os.getenv("DEBUG"):app.add_middleware(HTTPSRedirectMiddleware)

stream_connections:Dict[str,WebSocket]={}
view_connections:Dict[str,Set[WebSocket]]={}
pending_commands:Dict[str,asyncio.Future]={}
uploads:Dict[str,dict]={}

async def init_db():
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS devices(id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT UNIQUE NOT NULL,name TEXT NOT NULL,status TEXT DEFAULT 'inactive',last_seen TIMESTAMP,registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,user TEXT,action TEXT,device_id TEXT,details TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS commands_queue(id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT NOT NULL,command TEXT NOT NULL,status TEXT DEFAULT 'pending',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,result TEXT,file_path TEXT)""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_queue_device_status ON commands_queue(device_id,status)")
        await db.commit()

def generate_session_token():return secrets.token_urlsafe(32)
def generate_command_id():return str(uuid.uuid4())
def generate_upload_id():return secrets.token_urlsafe(16)
def safe_path(path:str)->str:
    if not path:return "/"
    n=os.path.normpath(path).replace('\\','/')
    if n.startswith('..') or '/../' in n:
        raise HTTPException(400,"Invalid path")
    return '/'+n.lstrip('/')

async def get_device_or_404(device_id:str,db:aiosqlite.Connection)->dict:
    cur=await db.execute("SELECT * FROM devices WHERE device_id=?",(device_id,))
    row=await cur.fetchone()
    if not row:raise HTTPException(404,"Device not found")
    return dict(row)

async def log_action(user:str,action:str,device_id:str=None,details:str=""):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("INSERT INTO logs(user,action,device_id,details) VALUES(?,?,?,?)",(user,action,device_id,details))
        await db.commit()

async def get_current_user(session_token:Optional[str]=Cookie(None)):
    if not session_token:raise HTTPException(401,"Not authenticated")
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur=await db.execute("SELECT * FROM sessions WHERE token=?",(session_token,))
        if not await cur.fetchone():raise HTTPException(401,"Invalid session")
    return session_token

api_key_header=APIKeyHeader(name="X-API-Key",auto_error=False)
async def verify_api_key(api_key:str=Depends(api_key_header)):
    if api_key!=API_KEY:raise HTTPException(403,"Invalid API Key")
    return api_key

async def enqueue_command(device_id:str,command:dict,file_data:Optional[bytes]=None)->int:
    command_json=json.dumps(command)
    file_path=None
    if file_data:
        filename=f"{device_id}_{command.get('command_id',generate_command_id())}_{int(datetime.now().timestamp())}"
        file_path=os.path.join(TEMP_DIR,filename)
        with open(file_path,"wb") as f:f.write(file_data)
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur=await db.execute("INSERT INTO commands_queue(device_id,command,status,file_path) VALUES(?,?,'pending',?)",(device_id,command_json,file_path))
        await db.commit()
        return cur.lastrowid

async def get_pending_command(device_id:str)->Optional[dict]:
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur=await db.execute("SELECT id,command,file_path FROM commands_queue WHERE device_id=? AND status='pending' ORDER BY created_at LIMIT 1",(device_id,))
        row=await cur.fetchone()
        if not row:return None
        cmd_id,command_json,file_path=row[0],row[1],row[2]
        await db.execute("UPDATE commands_queue SET status='in_progress' WHERE id=?",(cmd_id,))
        await db.commit()
        command=json.loads(command_json)
        if file_path and os.path.exists(file_path):
            upload_id=generate_upload_id()
            uploads[upload_id]={"path":file_path,"device_id":device_id,"expires":time.time()+UPLOAD_EXPIRE_SECONDS}
            command["file_url"]=f"/api/download/{upload_id}"
        command["_db_id"]=cmd_id
        return command

async def complete_command(db_id:int,result:dict,status:str="success"):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE commands_queue SET status=?,result=? WHERE id=?",(status,json.dumps(result),db_id))
        await db.commit()

async def send_command_to_device(device_id:str,command:dict,timeout:float=30.0,file_data:Optional[bytes]=None)->dict:
    cmd_id=command.get("command_id",generate_command_id())
    command["command_id"]=cmd_id
    if device_id in stream_connections:
        ws=stream_connections[device_id]
        if file_data:
            if len(file_data)>MAX_INLINE_FILE_SIZE:
                filename=f"{device_id}_{cmd_id}_{int(time.time())}"
                file_path=os.path.join(TEMP_DIR,filename)
                with open(file_path,"wb") as f:f.write(file_data)
                upload_id=generate_upload_id()
                uploads[upload_id]={"path":file_path,"device_id":device_id,"expires":time.time()+UPLOAD_EXPIRE_SECONDS}
                command["file_url"]=f"/api/download/{upload_id}"
            else:
                command["_file_data"]=base64.b64encode(file_data).decode('utf-8')
        future=asyncio.get_event_loop().create_future()
        pending_commands[cmd_id]=future
        try:
            await ws.send_text(json.dumps(command))
            result=await asyncio.wait_for(future,timeout=timeout)
            return result
        except asyncio.TimeoutError:
            raise HTTPException(504,"Device response timeout")
        finally:
            pending_commands.pop(cmd_id,None)
    else:
        db_id=await enqueue_command(device_id,command,file_data)
        raise HTTPException(202,"Device offline, command queued",headers={"X-Command-ID":cmd_id,"X-Queue-ID":str(db_id)})

@app.get("/api/download/{upload_id}")
async def download_file(upload_id:str):
    upload=uploads.get(upload_id)
    if not upload:raise HTTPException(404,"Upload not found")
    if time.time()>upload["expires"]:
        del uploads[upload_id];raise HTTPException(410,"Expired")
    file_path=upload["path"]
    if not os.path.exists(file_path):
        del uploads[upload_id];raise HTTPException(404,"File not found")
    async def file_generator():
        try:
            with open(file_path,"rb") as f:
                while chunk:=f.read(8192):yield chunk
        finally:
            try:os.remove(file_path)
            except:pass
            uploads.pop(upload_id,None)
    return StreamingResponse(file_generator(),media_type="application/octet-stream",headers={"Content-Disposition":f"attachment; filename={os.path.basename(file_path)}"})

@app.websocket("/ws/stream/{device_id}")
async def ws_stream(websocket:WebSocket,device_id:str):
    await websocket.accept()
    async with aiosqlite.connect(DATABASE_URL) as db:
        try:await get_device_or_404(device_id,db)
        except:await websocket.close(1008,"Not registered");return
    if device_id in stream_connections:
        try:await stream_connections[device_id].close(1000)
        except:pass
    stream_connections[device_id]=websocket
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE devices SET status='active',last_seen=CURRENT_TIMESTAMP WHERE device_id=?",(device_id,))
        await db.commit()
    try:
        while True:
            msg=await websocket.receive()
            if "text" in msg:
                try:
                    data=json.loads(msg["text"])
                    cid=data.get("command_id")
                    if cid and cid in pending_commands:
                        f=pending_commands[cid]
                        if not f.done():f.set_result(data)
                except:pass
            elif "bytes" in msg:
                if device_id in view_connections:
                    for v in view_connections[device_id]:
                        try:await v.send_bytes(msg["bytes"])
                        except:pass
    except WebSocketDisconnect:pass
    finally:
        stream_connections.pop(device_id,None)
        async with aiosqlite.connect(DATABASE_URL) as db:
            await db.execute("UPDATE devices SET status='inactive' WHERE device_id=?",(device_id,))
            await db.commit()
        if device_id in view_connections:
            for v in view_connections[device_id]:
                try:await v.close(1000)
                except:pass
            del view_connections[device_id]

@app.websocket("/ws/view/{device_id}")
async def ws_view(websocket:WebSocket,device_id:str,session_token:Optional[str]=Cookie(None)):
    if not session_token:await websocket.close(1008,"Unauthorized");return
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur=await db.execute("SELECT * FROM sessions WHERE token=?",(session_token,))
        if not await cur.fetchone():await websocket.close(1008,"Invalid session");return
    await websocket.accept()
    async with aiosqlite.connect(DATABASE_URL) as db:
        try:await get_device_or_404(device_id,db)
        except:await websocket.close(1008,"Device not found");return
    view_connections.setdefault(device_id,set()).add(websocket)
    try:
        while True:
            msg=await websocket.receive_text()
            try:command=json.loads(msg)
            except:await websocket.send_text(json.dumps({"error":"Invalid JSON"}));continue
            if device_id not in stream_connections:
                await websocket.send_text(json.dumps({"error":"Device offline"}));continue
            if "command_id" not in command:command["command_id"]=generate_command_id()
            try:
                result=await send_command_to_device(device_id,command,30.0)
                await websocket.send_text(json.dumps(result))
            except HTTPException as e:
                if e.status_code==202:
                    await websocket.send_text(json.dumps({"status":"queued","command_id":command["command_id"],"detail":e.detail}))
                else:raise
    except WebSocketDisconnect:pass
    finally:
        if device_id in view_connections:
            view_connections[device_id].discard(websocket)
            if not view_connections[device_id]:del view_connections[device_id]

@app.post("/api/command/poll")
async def poll_commands(device_id:str=Form(...)):
    cmd=await get_pending_command(device_id)
    if not cmd:return {"status":"no_commands"}
    db_id=cmd.pop("_db_id")
    return {"status":"command","command":cmd,"_db_id":db_id}

@app.post("/api/command/result")
async def command_result(device_id:str=Form(...),db_id:int=Form(...),result:str=Form(...),status:str=Form("success")):
    try:res=json.loads(result)
    except:res={"raw":result}
    await complete_command(db_id,res,status)
    return {"status":"ok"}

@app.post("/api/register")
async def register(device_id:str=Form(...),device_name:str=Form(...),password:str=Form(...)):
    if password!=DEVICE_PASSWORD:raise HTTPException(403,"Invalid device password")
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur=await db.execute("SELECT * FROM devices WHERE device_id=?",(device_id,))
        if await cur.fetchone():
            await db.execute("UPDATE devices SET name=?,status='active',last_seen=CURRENT_TIMESTAMP WHERE device_id=?",(device_name,device_id))
            await db.commit()
            return {"status":"updated"}
        else:
            await db.execute("INSERT INTO devices(device_id,name,status,last_seen) VALUES(?,?,'active',CURRENT_TIMESTAMP)",(device_id,device_name))
            await db.commit()
            return {"status":"registered"}

@app.post("/api/heartbeat")
async def heartbeat(device_id:str=Form(...),status:str=Form("active")):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("UPDATE devices SET status=?,last_seen=CURRENT_TIMESTAMP WHERE device_id=?",(status,device_id))
        await db.commit()
    return {"status":"ok"}

@app.get("/api/devices")
async def get_devices(session:str=Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        db.row_factory=aiosqlite.Row
        cur=await db.execute("SELECT * FROM devices ORDER BY registered_at DESC")
        rows=await cur.fetchall()
        devices=[dict(r) for r in rows]
        for d in devices:
            d["online"]=d["device_id"] in stream_connections
            cur2=await db.execute("SELECT COUNT(*) FROM commands_queue WHERE device_id=? AND status='pending'",(d["device_id"],))
            cnt=await cur2.fetchone()
            d["queue_count"]=cnt[0] if cnt else 0
        return devices

@app.get("/api/v1/devices")
async def get_devices_api(api_key:str=Depends(verify_api_key)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        db.row_factory=aiosqlite.Row
        cur=await db.execute("SELECT * FROM devices ORDER BY registered_at DESC")
        rows=await cur.fetchall()
        devices=[dict(r) for r in rows]
        for d in devices:
            d["online"]=d["device_id"] in stream_connections
            cur2=await db.execute("SELECT COUNT(*) FROM commands_queue WHERE device_id=? AND status='pending'",(d["device_id"],))
            cnt=await cur2.fetchone()
            d["queue_count"]=cnt[0] if cnt else 0
        return devices

@app.get("/api/device/{device_id}/files")
async def list_files(device_id:str,path:str=Query("/"),session:str=Depends(get_current_user)):
    path=safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"list_files","path":path})
        if res.get("status")=="success":return res.get("result",[])
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.get("/api/device/{device_id}/file")
async def download_file_rest(device_id:str,path:str=Query(...),session:str=Depends(get_current_user)):
    path=safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"download_file","path":path},60.0)
        if res.get("status")!="success":raise HTTPException(500,res.get("error","Unknown"))
        data_b64=res.get("result",{}).get("data")
        if not data_b64:raise HTTPException(500,"No data")
        content=base64.b64decode(data_b64)
        return StreamingResponse(iter([content]),media_type="application/octet-stream",headers={"Content-Disposition":f"attachment; filename={os.path.basename(path)}"})
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/device/{device_id}/file")
async def upload_file_small(device_id:str,file:UploadFile=File(...),path:str=Form(...),overwrite:bool=Form(True),session:str=Depends(get_current_user)):
    path=safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    content=await file.read()
    if len(content)>MAX_INLINE_FILE_SIZE:
        raise HTTPException(413,f"File too large (max {MAX_INLINE_FILE_SIZE//1024//1024} MB) - use /upload_big")
    try:
        res=await send_command_to_device(device_id,{"command":"upload_file","path":path,"overwrite":overwrite},60.0,file_data=content)
        if res.get("status")=="success":
            await log_action(session,"upload_file",device_id,f"Uploaded {file.filename} to {path}")
            return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            await log_action(session,"upload_file_queued",device_id,f"Queued {path}")
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/device/{device_id}/upload_big")
async def upload_file_big(device_id:str,file:UploadFile=File(...),path:str=Form(...),overwrite:bool=Form(True),session:str=Depends(get_current_user)):
    path=safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    content=await file.read()
    if len(content)>MAX_BIG_FILE_SIZE:raise HTTPException(413,f"File too large (max {MAX_BIG_FILE_SIZE//1024//1024} MB)")
    try:
        res=await send_command_to_device(device_id,{"command":"upload_file","path":path,"overwrite":overwrite},60.0,file_data=content)
        if res.get("status")=="success":
            await log_action(session,"upload_big",device_id,f"Uploaded {file.filename} to {path}")
            return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            await log_action(session,"upload_big_queued",device_id,f"Queued {path}")
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.delete("/api/device/{device_id}/file")
async def delete_file(device_id:str,path:str=Query(...),recursive:bool=False,session:str=Depends(get_current_user)):
    path=safe_path(path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"delete_file","path":path,"recursive":recursive})
        if res.get("status")=="success":
            await log_action(session,"delete_file",device_id,f"Deleted {path}")
            return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.put("/api/device/{device_id}/file")
async def rename_file(device_id:str,old_path:str=Query(...),new_path:str=Query(...),session:str=Depends(get_current_user)):
    old_path=safe_path(old_path);new_path=safe_path(new_path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"rename_file","old_path":old_path,"new_path":new_path})
        if res.get("status")=="success":
            await log_action(session,"rename_file",device_id,f"Renamed {old_path} to {new_path}")
            return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/device/{device_id}/install_apk")
async def install_apk(device_id:str,apk_path:str=Form(...),session:str=Depends(get_current_user)):
    apk_path=safe_path(apk_path)
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"install_apk","apk_path":apk_path},120.0)
        if res.get("status")=="success":
            await log_action(session,"install_apk",device_id,f"Installed {apk_path}")
            return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/device/{device_id}/touch")
async def touch(device_id:str,x:float=Form(...),y:float=Form(...),action:str=Form("click"),session:str=Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"touch","x":x,"y":y,"action":action})
        if res.get("status")=="success":return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/device/{device_id}/swipe")
async def swipe(device_id:str,x1:float=Form(...),y1:float=Form(...),x2:float=Form(...),y2:float=Form(...),duration:int=Form(300),session:str=Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"swipe","x1":x1,"y1":y1,"x2":x2,"y2":y2,"duration":duration})
        if res.get("status")=="success":return {"status":"ok"}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/device/{device_id}/shell")
async def shell(device_id:str,command:str=Form(...),session:str=Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:await get_device_or_404(device_id,db)
    try:
        res=await send_command_to_device(device_id,{"command":"shell","cmd":command},60.0)
        if res.get("status")=="success":
            await log_action(session,"shell",device_id,f"Executed: {command}")
            return {"status":"ok","output":res.get("result","")}
        raise HTTPException(500,res.get("error","Unknown"))
    except HTTPException as e:
        if e.status_code==202:
            return JSONResponse(202,{"status":"queued","command_id":e.headers.get("X-Command-ID"),"detail":e.detail})
        raise

@app.post("/api/devices/command")
async def broadcast(command:dict,session:str=Depends(get_current_user)):
    if "command" not in command:raise HTTPException(400,"Missing 'command' field")
    results={}
    for device_id,ws in stream_connections.items():
        try:
            cmd=command.copy()
            cmd["command_id"]=generate_command_id()
            future=asyncio.get_event_loop().create_future()
            pending_commands[cmd["command_id"]]=future
            await ws.send_text(json.dumps(cmd))
            results[device_id]={"status":"sent"}
        except Exception as e:results[device_id]={"error":str(e)}
    await log_action(session,"broadcast",details=f"Broadcasted: {command}")
    return {"status":"ok","results":results}

@app.delete("/api/device/{device_id}")
async def delete_device_api(device_id:str,api_key:str=Depends(verify_api_key)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await get_device_or_404(device_id,db)
        await db.execute("DELETE FROM devices WHERE device_id=?",(device_id,))
        await db.execute("DELETE FROM commands_queue WHERE device_id=?",(device_id,))
        await db.commit()
    stream_connections.pop(device_id,None)
    if device_id in view_connections:
        for ws in view_connections[device_id]:
            try:await ws.close(1000)
            except:pass
        del view_connections[device_id]
    for fname in os.listdir(TEMP_DIR):
        if fname.startswith(device_id):
            try:os.remove(os.path.join(TEMP_DIR,fname))
            except:pass
    await log_action("API","delete_device",device_id)
    return {"status":"ok"}

@app.delete("/api/web/device/{device_id}")
async def delete_device_web(device_id:str,session:str=Depends(get_current_user)):
    async with aiosqlite.connect(DATABASE_URL) as db:
        await get_device_or_404(device_id,db)
        await db.execute("DELETE FROM devices WHERE device_id=?",(device_id,))
        await db.execute("DELETE FROM commands_queue WHERE device_id=?",(device_id,))
        await db.commit()
    stream_connections.pop(device_id,None)
    if device_id in view_connections:
        for ws in view_connections[device_id]:
            try:await ws.close(1000)
            except:pass
        del view_connections[device_id]
    for fname in os.listdir(TEMP_DIR):
        if fname.startswith(device_id):
            try:os.remove(os.path.join(TEMP_DIR,fname))
            except:pass
    await log_action(session,"delete_device",device_id)
    return {"status":"ok"}

@app.get("/api/docs",include_in_schema=False)
async def swagger_docs(session:str=Depends(get_current_user)):
    return get_swagger_ui_html(openapi_url="/openapi.json",title="HSSUPRavle API")

@app.get("/openapi.json",include_in_schema=False)
async def openapi(session:str=Depends(get_current_user)):
    return app.openapi()

LOGIN_PAGE='''<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Login</title></head>
<body style="background:#222;color:#eee;font-family:monospace;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;">
<div style="background:#333;padding:30px;border-radius:8px;width:300px;text-align:center;">
<h1 style="color:#e94560;">HSSUPRavle</h1>
<form action="/login" method="post">
<input type="password" name="password" placeholder="Admin Password" style="width:100%;padding:10px;margin:10px 0;background:#444;border:none;color:#eee;border-radius:4px;">
<button type="submit" style="width:100%;padding:10px;background:#e94560;border:none;color:#fff;font-weight:bold;cursor:pointer;border-radius:4px;">Enter</button>
</form>
</div></body></html>'''

DASHBOARD_PAGE='''<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Dashboard</title></head>
<body style="background:#1a1a2e;color:#eee;font-family:monospace;padding:20px;">
<div style="display:flex;justify-content:space-between;border-bottom:1px solid #333;padding-bottom:10px;">
<h1 style="color:#e94560;margin:0;">HSSUPRavle</h1>
<a href="/logout" style="color:#e94560;">Logout</a></div>
<button onclick="refresh()" style="margin:20px 0;background:#333;color:#eee;border:none;padding:6px 12px;cursor:pointer;">Refresh</button>
<table style="width:100%;border-collapse:collapse;"><thead><tr style="background:#222;"><th>Name</th><th>ID</th><th>Status</th><th>Queue</th><th>Last Seen</th><th>Actions</th></tr></thead><tbody id="devices"></tbody></table>
<script>
function refresh(){fetch('/api/devices',{credentials:'include'}).then(r=>r.json()).then(data=>{let h='';data.forEach(d=>{const online=d.online?'<span style="color:#0f0;">Online</span>':'<span style="color:#f00;">Offline</span>';const queue=d.queue_count?`<span style="color:#ff0;">${d.queue_count}</span>`:'0';h+=`<tr><td>${d.name}</td><td>${d.device_id.slice(0,8)}..</td><td>${online}</td><td>${queue}</td><td>${d.last_seen}</td><td><a href="/device/${d.device_id}" style="color:#0af;">Manage</a> <button onclick="del('${d.device_id}')" style="background:#e94560;border:none;color:#fff;cursor:pointer;">Delete</button></td></tr>`;});document.getElementById('devices').innerHTML=h;});}
function del(id){if(confirm('Delete device?')){fetch('/api/web/device/'+id,{method:'DELETE',credentials:'include'}).then(()=>refresh());}}
refresh();setInterval(refresh,10000);
</script></body></html>'''

DEVICE_PAGE_TEMPLATE='''<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Device {device_id}</title></head>
<body style="background:#1a1a2e;color:#eee;font-family:monospace;padding:20px;">
<div style="display:flex;justify-content:space-between;border-bottom:1px solid #333;padding-bottom:10px;">
<h1 style="color:#e94560;margin:0;">Device: {device_name} <span id="status" style="font-size:14px;color:#aaa;"></span></h1>
<div><a href="/" style="color:#e94560;">Dashboard</a> | <a href="/logout" style="color:#e94560;">Logout</a></div></div>
<div style="margin:10px 0;">
<button onclick="showTab('files')" style="background:#333;border:none;color:#eee;padding:6px 16px;cursor:pointer;">Files</button>
<button onclick="showTab('screen')" style="background:#333;border:none;color:#eee;padding:6px 16px;cursor:pointer;">Screen</button>
<span id="queue-info" style="margin-left:20px;color:#aaa;"></span>
</div>
<div id="tab-files" style="display:block;">
<div><input id="path" value="/storage/emulated/0" style="width:50%;background:#222;color:#eee;border:1px solid #444;padding:4px;">
<button onclick="listFiles()" style="background:#333;border:none;color:#eee;padding:4px 12px;cursor:pointer;">Go</button>
<button onclick="listFiles('/')" style="background:#333;border:none;color:#eee;padding:4px 12px;cursor:pointer;">Root</button>
<button onclick="uploadFile()" style="background:#28a745;border:none;color:#fff;padding:4px 12px;cursor:pointer;">Upload</button>
<button onclick="uploadBig()" style="background:#f0ad4e;border:none;color:#000;padding:4px 12px;cursor:pointer;">Upload Big</button>
</div>
<table style="width:100%;border-collapse:collapse;margin-top:10px;">
<thead><tr style="background:#222;"><th>Name</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
<tbody id="filelist"></tbody></table>
</div>
<div id="tab-screen" style="display:none;">
<img id="screen-img" style="width:100%;max-width:600px;border:1px solid #444;cursor:crosshair;" />
<div style="margin-top:10px;">
<button onclick="sendTouch(0.5,0.5,'click')" style="background:#28a745;border:none;color:#fff;padding:4px 12px;cursor:pointer;">Click</button>
<button onclick="sendSwipe(0.2,0.5,0.8,0.5)" style="background:#f0ad4e;border:none;color:#000;padding:4px 12px;cursor:pointer;">Swipe H</button>
<button onclick="sendSwipe(0.5,0.2,0.5,0.8)" style="background:#f0ad4e;border:none;color:#000;padding:4px 12px;cursor:pointer;">Swipe V</button>
</div>
</div>
<div id="upload-modal" style="display:none;position:fixed;left:0;top:0;width:100%;height:100%;background:rgba(0,0,0,0.7);">
<div style="background:#222;padding:20px;margin:10% auto;width:300px;border-radius:8px;">
<h3>Upload File</h3>
<input id="upload-path" placeholder="Target path" style="width:100%;background:#333;color:#eee;border:none;padding:6px;margin:6px 0;">
<input id="upload-file" type="file" style="width:100%;margin:6px 0;">
<button onclick="doUpload()" style="background:#28a745;border:none;color:#fff;padding:6px 12px;cursor:pointer;">Upload</button>
<button onclick="document.getElementById('upload-modal').style.display='none'" style="background:#e94560;border:none;color:#fff;padding:6px 12px;cursor:pointer;">Cancel</button>
</div>
</div>
<script>
const deviceId='{device_id}';let currentPath='/storage/emulated/0';let bigMode=false;
const ws=new WebSocket('wss://'+location.host+'/ws/view/'+deviceId);
ws.onopen=()=>document.getElementById('status').textContent='🟢 Online';
ws.onclose=()=>document.getElementById('status').textContent='🔴 Offline';
ws.onmessage=e=>{if(e.data instanceof Blob){document.getElementById('screen-img').src=URL.createObjectURL(e.data);}else{try{const d=JSON.parse(e.data);console.log('Response:',d);}catch(e){}}};
function showTab(t){document.querySelectorAll('[id^="tab-"]').forEach(el=>el.style.display='none');document.getElementById('tab-'+t).style.display='block';}
function updateQueueInfo(){fetch('/api/devices',{credentials:'include'}).then(r=>r.json()).then(data=>{const d=data.find(x=>x.device_id===deviceId);if(d)document.getElementById('queue-info').textContent='Queue: '+(d.queue_count||0);});}
function listFiles(path){if(path!==undefined)currentPath=path;else currentPath=document.getElementById('path').value;document.getElementById('path').value=currentPath;fetch('/api/device/'+deviceId+'/files?path='+encodeURIComponent(currentPath),{credentials:'include'}).then(r=>r.json()).then(data=>{if(data.status==='queued'){alert('Command queued, check later');updateQueueInfo();return;}let html='';if(currentPath!=='/')html+='<tr><td><a href="#" onclick="listFiles(\''+currentPath+'/..\')">..</a></td><td></td><td></td><td></td></tr>';data.forEach(item=>{const isDir=item.is_dir;const size=isDir?'-':(item.size/1024).toFixed(1)+' KB';const actions=isDir?'<button onclick="listFiles(\''+currentPath+'/'+item.name+'\')">Open</button>':'<button onclick="downloadFile(\''+currentPath+'/'+item.name+'\')">Download</button> <button onclick="renameFile(\''+currentPath+'/'+item.name+'\')">Rename</button> <button onclick="deleteFile(\''+currentPath+'/'+item.name+'\')">Delete</button>'+(item.name.endsWith('.apk')?' <button onclick="installApk(\''+currentPath+'/'+item.name+'\')">Install</button>':'');html+='<tr><td>'+(isDir?'📁':'📄')+' '+item.name+'</td><td>'+size+'</td><td>'+(item.modified||'')+'</td><td>'+actions+'</td></tr>';});document.getElementById('filelist').innerHTML=html;});}
function downloadFile(path){fetch('/api/device/'+deviceId+'/file?path='+encodeURIComponent(path),{credentials:'include'}).then(res=>{if(res.status===202)return res.json().then(data=>{alert('Queued: '+data.detail);throw new Error('queued');});return res.blob();}).then(blob=>{const url=URL.createObjectURL(blob);const a=document.createElement('a');a.href=url;a.download=path.split('/').pop();a.click();}).catch(err=>{if(err.message!=='queued')console.error(err);});}
function deleteFile(path){if(!confirm('Delete '+path+'?'))return;fetch('/api/device/'+deviceId+'/file?path='+encodeURIComponent(path),{method:'DELETE',credentials:'include'}).then(r=>r.json()).then(data=>{if(data.status==='queued'){alert('Queued');updateQueueInfo();}else listFiles();}).catch(err=>alert(err));}
function renameFile(path){const newPath=prompt('New name:',path);if(!newPath||newPath===path)return;fetch('/api/device/'+deviceId+'/file?old_path='+encodeURIComponent(path)+'&new_path='+encodeURIComponent(newPath),{method:'PUT',credentials:'include'}).then(r=>r.json()).then(data=>{if(data.status==='queued'){alert('Queued');updateQueueInfo();}else listFiles();}).catch(err=>alert(err));}
function installApk(path){if(!confirm('Install APK '+path+'?'))return;fetch('/api/device/'+deviceId+'/install_apk',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:'apk_path='+encodeURIComponent(path),credentials:'include'}).then(r=>r.json()).then(data=>{if(data.status==='queued'){alert('Queued');updateQueueInfo();}else alert(data.status==='ok'?'Installed':'Error');}).catch(err=>alert(err));}
function uploadFile(){bigMode=false;document.getElementById('upload-modal').style.display='block';}
function uploadBig(){bigMode=true;document.getElementById('upload-modal').style.display='block';}
function doUpload(){const file=document.getElementById('upload-file').files[0];const path=document.getElementById('upload-path').value;if(!file||!path){alert('Select file and path');return;}const fd=new FormData();fd.append('file',file);fd.append('path',path);const endpoint=bigMode?'/upload_big':'';fetch('/api/device/'+deviceId+'/file'+endpoint,{method:'POST',body:fd,credentials:'include'}).then(r=>r.json()).then(data=>{if(data.status==='queued'){alert('Upload queued');updateQueueInfo();}else if(data.status==='ok'){alert('Uploaded');document.getElementById('upload-modal').style.display='none';listFiles();}else alert('Error');}).catch(err=>alert(err));}
function sendTouch(x,y,action){ws.send(JSON.stringify({command:'touch',x,y,action}));}
function sendSwipe(x1,y1,x2,y2,duration=300){ws.send(JSON.stringify({command:'swipe',x1,y1,x2,y2,duration}));}
document.getElementById('screen-img').addEventListener('click',function(e){const rect=this.getBoundingClientRect();const x=(e.clientX-rect.left)/rect.width;const y=(e.clientY-rect.top)/rect.height;sendTouch(x,y,'click');});
listFiles();setInterval(updateQueueInfo,5000);
</script></body></html>'''

async def web_auth(request:Request):
    token=request.cookies.get("session_token")
    if not token:return None
    async with aiosqlite.connect(DATABASE_URL) as db:
        cur=await db.execute("SELECT * FROM sessions WHERE token=?",(token,))
        if await cur.fetchone():return token
    return None

@app.get("/",response_class=HTMLResponse)
async def index(request:Request):
    session=await web_auth(request)
    return DASHBOARD_PAGE if session else LOGIN_PAGE

@app.get("/device/{device_id}",response_class=HTMLResponse)
async def device_page(request:Request,device_id:str):
    session=await web_auth(request)
    if not session:return LOGIN_PAGE
    async with aiosqlite.connect(DATABASE_URL) as db:
        try:device=await get_device_or_404(device_id,db)
        except:return HTMLResponse("Device not found", status_code=404)
    return HTMLResponse(DEVICE_PAGE_TEMPLATE.format(device_id=device_id,device_name=device['name']))

@app.post("/login")
async def login(request:Request,password:str=Form(...)):
    if password!=ADMIN_PASSWORD:
        return HTMLResponse(LOGIN_PAGE.replace('</form>','</form><div style="color:#e94560;margin-top:10px;">Invalid password</div>'), status_code=401)
    token=generate_session_token()
    async with aiosqlite.connect(DATABASE_URL) as db:
        await db.execute("INSERT INTO sessions(token) VALUES(?)",(token,))
        await db.commit()
    await log_action(token,"login")
    resp=HTMLResponse(content="<script>window.location.href='/';</script>", status_code=302)
    resp.set_cookie("session_token",token,max_age=int(SESSION_TIMEOUT.total_seconds()),httponly=True,secure=True,samesite='lax')
    return resp

@app.get("/logout")
async def logout(request:Request):
    token=request.cookies.get("session_token")
    if token:
        async with aiosqlite.connect(DATABASE_URL) as db:
            await db.execute("DELETE FROM sessions WHERE token=?",(token,))
            await db.commit()
        await log_action(token,"logout")
    resp=HTMLResponse(content="<script>window.location.href='/';</script>", status_code=302)
    resp.delete_cookie("session_token")
    return resp

if __name__=="__main__":
    port=int(os.getenv("PORT",8000))
    uvicorn.run(app,host="0.0.0.0",port=port)