"""Minimal FastAPI server for distributed video downloading."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Optional

from datetime import datetime, timezone

import aiofiles
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.requests import ClientDisconnect

from . import db

logger = logging.getLogger("server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

CLIENT_VERSION = "0.2.0"
DB_PATH = os.environ.get("DB_PATH", "server.db")
VIDEO_IDS_DIR = os.environ.get("VIDEO_IDS_DIR", "./news-downloader/data/video_ids/")
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "./uploads")


def get_conn():
    return db.connect(DB_PATH)


async def expire_loop():
    """Background task to expire stale batch assignments every 5 minutes."""
    while True:
        await asyncio.sleep(300)
        try:
            conn = get_conn()
            expired = db.expire_stale_batches(conn, timeout_minutes=30)
            if expired:
                logger.info(f"Expired {expired} stale assignments")
            conn.close()
        except Exception as e:
            logger.error(f"Expire loop error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: init DB and load video IDs
    conn = get_conn()
    db.init_db(conn)

    ids_dir = Path(VIDEO_IDS_DIR)
    if ids_dir.exists():
        logger.info(f"Loading video IDs from {VIDEO_IDS_DIR}")
        counts = db.load_video_ids(conn, VIDEO_IDS_DIR)
        for ch, n in counts.items():
            logger.info(f"  {ch}: {n:,} IDs")
    else:
        logger.warning(f"Video IDs directory not found: {VIDEO_IDS_DIR}")

    conn.close()

    # Start background expiry task
    task = asyncio.create_task(expire_loop())
    yield
    task.cancel()


app = FastAPI(title="ytminer-server", lifespan=lifespan)


# ─── Request/Response Models ────────────────────────────────────


class ReportRequest(BaseModel):
    batch_id: str
    worker: str
    results: dict  # {video_id: "ok"|"failed"|"skipped"}
    errors: Optional[dict] = None  # {video_id: error_category}
    channel: Optional[str] = None  # channel filter for next batch


# ─── Endpoints ───────────────────────────────────────────────────


@app.get("/batch")
def get_batch(
    request: Request,
    worker: str = Query(...),
    channel: Optional[str] = Query(None),
    batch_size: int = Query(50, ge=1, le=200),
):
    conn = get_conn()
    # Store worker IP
    ip = request.client.host if request.client else ""
    conn.execute(
        "INSERT INTO workers (name, last_seen, ip_address) VALUES (?, datetime('now'), ?) "
        "ON CONFLICT(name) DO UPDATE SET last_seen=datetime('now'), ip_address=?",
        (worker, ip, ip),
    )
    conn.commit()

    batch = db.assign_batch(conn, worker, batch_size=batch_size, channel=channel)
    conn.close()

    if batch is None:
        return {"batch_id": None, "channel": None, "video_ids": [], "message": "No more work available"}
    return batch


@app.post("/report")
def post_report(body: ReportRequest):
    conn = get_conn()
    counts = db.report_batch(conn, body.batch_id, body.worker, body.results)

    # Log error categories if provided
    if body.errors:
        for vid, cat in body.errors.items():
            conn.execute(
                "UPDATE videos SET error_category=? WHERE video_id=?",
                (cat, vid),
            )
        conn.commit()

    # Immediately assign next batch (respect channel filter)
    next_batch = db.assign_batch(conn, body.worker, channel=body.channel)
    conn.close()

    return {"counts": counts, "next_batch": next_batch}


@app.post("/upload/{channel}/{video_id}")
async def upload_video(
    channel: str,
    video_id: str,
    request: Request,
    worker: str = Query(...),
    filename: str = Query(...),
):
    """Receive a streamed file upload (video or info.json)."""
    dest_dir = Path(UPLOAD_DIR) / channel
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    total_bytes = 0
    try:
        async with aiofiles.open(dest_path, "wb") as f:
            async for chunk in request.stream():
                await f.write(chunk)
                total_bytes += len(chunk)
    except ClientDisconnect:
        # Client dropped mid-upload — clean up partial file
        if dest_path.exists():
            dest_path.unlink()
        logger.warning(
            "Client disconnected during upload: %s/%s (%s, %s bytes received)",
            channel, video_id, filename, total_bytes,
        )
        return JSONResponse(
            {"error": "client disconnected", "bytes_received": total_bytes},
            status_code=499,
        )

    # Run DB update in thread pool to avoid blocking the event loop
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None, partial(_mark_uploaded, video_id, filename, total_bytes, worker),
    )
    return {"status": "ok", "bytes": total_bytes}


def _mark_uploaded(video_id: str, filename: str, total_bytes: int, worker: str):
    """Synchronous DB update, run in a thread pool."""
    now = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    if filename.endswith(".mp4"):
        conn.execute(
            "UPDATE videos SET uploaded=1, uploaded_at=? WHERE video_id=?",
            (now, video_id),
        )
        conn.execute(
            "UPDATE workers SET total_uploaded=total_uploaded+1, "
            "total_upload_bytes=total_upload_bytes+? WHERE name=?",
            (total_bytes, worker),
        )
        conn.commit()
    conn.close()


@app.get("/status")
def get_status():
    conn = get_conn()
    status = db.get_status(conn)
    conn.close()
    return status


@app.get("/version")
def get_version():
    return {"client_version": CLIENT_VERSION}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    template_path = Path(__file__).parent / "templates" / "dashboard.html"
    html = template_path.read_text()
    return HTMLResponse(html)


@app.get("/download/client")
def download_client():
    """Serve the client package tarball for pip install."""
    dist_path = Path(__file__).parent / "dist" / "ytminer-client.tar.gz"
    if not dist_path.exists():
        return {"error": "Client package not found. Run build_client.sh first."}
    return FileResponse(
        dist_path,
        media_type="application/gzip",
        filename="ytminer-client.tar.gz",
    )
