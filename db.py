"""SQLite-backed work queue for distributed video downloading."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def connect(db_path: str = "server.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            batch_id TEXT,
            assigned_to TEXT,
            assigned_at TEXT,
            completed_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            error_category TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
        CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel);
        CREATE INDEX IF NOT EXISTS idx_videos_batch ON videos(batch_id);

        CREATE TABLE IF NOT EXISTS workers (
            name TEXT PRIMARY KEY,
            last_seen TEXT NOT NULL,
            total_done INTEGER NOT NULL DEFAULT 0,
            total_failed INTEGER NOT NULL DEFAULT 0,
            total_skipped INTEGER NOT NULL DEFAULT 0,
            ip_address TEXT
        );
    """)
    conn.commit()

    # Add upload columns (safe to run on existing DBs)
    for stmt in [
        "ALTER TABLE videos ADD COLUMN uploaded INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE videos ADD COLUMN uploaded_at TEXT",
        "ALTER TABLE workers ADD COLUMN total_uploaded INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE workers ADD COLUMN total_upload_bytes INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()


def load_video_ids(conn: sqlite3.Connection, video_ids_dir: str) -> dict:
    """Load video IDs from .txt files into the database. Returns {channel: count_added}."""
    ids_path = Path(video_ids_dir)
    results = {}
    for txt_file in sorted(ids_path.glob("*.txt")):
        channel = txt_file.stem
        with open(txt_file) as f:
            video_ids = [line.strip() for line in f if line.strip()]

        # Insert only new IDs
        added = 0
        for batch_start in range(0, len(video_ids), 500):
            batch = video_ids[batch_start:batch_start + 500]
            for vid in batch:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO videos (video_id, channel) VALUES (?, ?)",
                        (vid, channel),
                    )
                    added += conn.total_changes  # approximate
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        results[channel] = len(video_ids)
    return results


def assign_batch(
    conn: sqlite3.Connection,
    worker: str,
    batch_size: int = 50,
    channel: Optional[str] = None,
    max_attempts: int = 3,
) -> Optional[dict]:
    """Assign a batch of pending videos to a worker. Returns batch info or None."""
    now = datetime.now(timezone.utc).isoformat()
    batch_id = uuid.uuid4().hex[:12]

    # Build query — prioritize @geonews, then other channels
    where = "status = 'pending' AND attempts < ?"
    params: list = [max_attempts]
    if channel:
        where += " AND channel = ?"
        params.append(channel)
    params.append(batch_size)

    rows = conn.execute(
        f"SELECT video_id, channel FROM videos WHERE {where} "
        f"ORDER BY CASE WHEN channel = '@geonews' THEN 0 ELSE 1 END "
        f"LIMIT ?",
        params,
    ).fetchall()

    if not rows:
        return None

    video_ids = [r["video_id"] for r in rows]
    batch_channel = rows[0]["channel"]

    # Mark as assigned
    placeholders = ",".join("?" for _ in video_ids)
    conn.execute(
        f"UPDATE videos SET status='assigned', batch_id=?, assigned_to=?, assigned_at=? "
        f"WHERE video_id IN ({placeholders})",
        [batch_id, worker, now] + video_ids,
    )

    # Upsert worker
    conn.execute(
        "INSERT INTO workers (name, last_seen, ip_address) VALUES (?, ?, '') "
        "ON CONFLICT(name) DO UPDATE SET last_seen=?",
        (worker, now, now),
    )
    conn.commit()

    return {
        "batch_id": batch_id,
        "channel": batch_channel,
        "video_ids": video_ids,
    }


def report_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    worker: str,
    results: dict,
) -> dict:
    """Report results for a batch. results: {video_id: 'ok'|'failed'|'skipped'}"""
    now = datetime.now(timezone.utc).isoformat()
    counts = {"done": 0, "failed": 0, "skipped": 0}

    for video_id, status in results.items():
        if status == "ok":
            conn.execute(
                "UPDATE videos SET status='done', completed_at=? WHERE video_id=? AND batch_id=?",
                (now, video_id, batch_id),
            )
            counts["done"] += 1
        elif status == "skipped":
            conn.execute(
                "UPDATE videos SET status='skipped', completed_at=? WHERE video_id=? AND batch_id=?",
                (now, video_id, batch_id),
            )
            counts["skipped"] += 1
        else:  # failed
            conn.execute(
                "UPDATE videos SET status='pending', batch_id=NULL, assigned_to=NULL, "
                "assigned_at=NULL, attempts=attempts+1 WHERE video_id=? AND batch_id=?",
                (video_id, batch_id),
            )
            counts["failed"] += 1

    # Update worker stats
    conn.execute(
        "UPDATE workers SET last_seen=?, total_done=total_done+?, "
        "total_failed=total_failed+?, total_skipped=total_skipped+? WHERE name=?",
        (now, counts["done"], counts["failed"], counts["skipped"], worker),
    )
    conn.commit()
    return counts


def expire_stale_batches(conn: sqlite3.Connection, timeout_minutes: int = 30) -> int:
    """Reset assigned batches that have timed out."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)).isoformat()
    cursor = conn.execute(
        "UPDATE videos SET status='pending', batch_id=NULL, assigned_to=NULL, "
        "assigned_at=NULL WHERE status='assigned' AND assigned_at < ?",
        (cutoff,),
    )
    conn.commit()
    return cursor.rowcount


def get_status(conn: sqlite3.Connection) -> dict:
    """Get overall status for the dashboard."""
    # Per-channel counts
    channels = {}
    rows = conn.execute(
        "SELECT channel, status, COUNT(*) as cnt FROM videos GROUP BY channel, status"
    ).fetchall()
    for r in rows:
        ch = r["channel"]
        if ch not in channels:
            channels[ch] = {"pending": 0, "assigned": 0, "done": 0, "failed": 0, "skipped": 0, "total": 0}
        channels[ch][r["status"]] = r["cnt"]
        channels[ch]["total"] += r["cnt"]

    # Workers
    workers = []
    for r in conn.execute(
        "SELECT name, last_seen, total_done, total_failed, total_skipped, "
        "total_uploaded, total_upload_bytes, ip_address FROM workers ORDER BY last_seen DESC"
    ).fetchall():
        workers.append(dict(r))

    # Totals
    total_row = conn.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done, "
        "SUM(CASE WHEN status='assigned' THEN 1 ELSE 0 END) as assigned, "
        "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending, "
        "SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) as skipped, "
        "SUM(CASE WHEN uploaded=1 THEN 1 ELSE 0 END) as uploaded "
        "FROM videos"
    ).fetchone()

    # Rate: completions in last hour
    rate_row = conn.execute(
        "SELECT COUNT(*) as cnt FROM videos "
        "WHERE status IN ('done','skipped') AND completed_at > datetime('now', '-1 hour')"
    ).fetchone()
    rate_per_hour = rate_row["cnt"] if rate_row else 0

    # First and last completion timestamps
    time_row = conn.execute(
        "SELECT MIN(completed_at) as first, MAX(completed_at) as last "
        "FROM videos WHERE status IN ('done','skipped') AND completed_at IS NOT NULL"
    ).fetchone()

    return {
        "total": dict(total_row),
        "channels": channels,
        "workers": workers,
        "rate_per_hour": rate_per_hour,
        "first_completed": time_row["first"] if time_row else None,
        "last_completed": time_row["last"] if time_row else None,
    }
