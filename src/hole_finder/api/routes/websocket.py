"""WebSocket endpoint for real-time job progress updates."""

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from hole_finder.db.engine import async_session_factory
from hole_finder.db.models import Job

router = APIRouter()


class ConnectionManager:
    """Manages active WebSocket connections for job progress."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                disconnected.append(conn)
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


@router.websocket("/ws/jobs")
async def job_progress_ws(websocket: WebSocket):
    """WebSocket for real-time job progress. Polls DB every 2 seconds."""
    await manager.connect(websocket)
    try:
        while True:
            # Poll for active jobs and send updates
            async with async_session_factory() as session:
                # Include recently completed/failed jobs so clients catch the transition
                from datetime import UTC, datetime, timedelta
                cutoff = datetime.now(UTC) - timedelta(minutes=5)
                stmt = select(Job).where(
                    (Job.status.in_(["PENDING", "RUNNING"]))
                    | ((Job.status.in_(["COMPLETED", "FAILED"])) & (Job.completed_at >= cutoff))
                ).order_by(Job.created_at.desc()).limit(50)
                result = await session.execute(stmt)
                jobs = result.scalars().all()

                updates = []
                for j in jobs:
                    summary = j.result_summary or {}
                    updates.append({
                        "id": str(j.id),
                        "status": j.status.value if j.status else "unknown",
                        "progress": j.progress or 0.0,
                        "job_type": j.job_type.value if j.job_type else "unknown",
                        "stage": summary.get("stage"),
                        "source": summary.get("source"),
                        "total_detections": summary.get("total_detections"),
                        "download_mb": summary.get("download_mb"),
                    })

                if updates:
                    await websocket.send_json({"type": "job_updates", "jobs": updates})

            # Also listen for client messages (e.g., subscribe to specific job)
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
                # Client can send {"subscribe": "job_id"} to filter
                msg = json.loads(data)
                if "ping" in msg:
                    await websocket.send_json({"type": "pong"})
            except TimeoutError:
                pass  # normal — poll loop continues

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
