"""Comment and saved detection endpoints."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.api.deps import get_db
from magic_eyes.db.models import Comment, SavedDetection, Detection

router = APIRouter(tags=["comments"])


class CommentCreate(BaseModel):
    text: str
    author: str | None = None


class SaveCreate(BaseModel):
    label: str | None = None
    color: str | None = None
    notes: str | None = None


# --- Comments ---

@router.get("/detections/{detection_id}/comments")
async def list_comments(
    detection_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(Comment)
        .where(Comment.detection_id == detection_id)
        .order_by(Comment.created_at.desc())
    )
    result = await db.execute(stmt)
    comments = result.scalars().all()
    return [
        {
            "id": str(c.id),
            "text": c.text,
            "author": c.author,
            "created_at": str(c.created_at),
        }
        for c in comments
    ]


@router.post("/detections/{detection_id}/comments")
async def add_comment(
    detection_id: uuid.UUID,
    body: CommentCreate,
    db: AsyncSession = Depends(get_db),
):
    detection = await db.get(Detection, detection_id)
    if not detection:
        raise HTTPException(status_code=404, detail="Detection not found")

    comment = Comment(
        detection_id=detection_id,
        text=body.text,
        author=body.author,
    )
    db.add(comment)
    await db.commit()
    await db.refresh(comment)
    return {"id": str(comment.id), "status": "created"}


@router.delete("/comments/{comment_id}")
async def delete_comment(
    comment_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    comment = await db.get(Comment, comment_id)
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")
    await db.delete(comment)
    await db.commit()
    return {"status": "deleted"}


# --- Saved/Highlighted Detections ---

@router.get("/saved")
async def list_saved(
    db: AsyncSession = Depends(get_db),
):
    stmt = select(SavedDetection).order_by(SavedDetection.created_at.desc()).limit(500)
    result = await db.execute(stmt)
    saves = result.scalars().all()
    return [
        {
            "id": str(s.id),
            "detection_id": str(s.detection_id),
            "label": s.label,
            "color": s.color,
            "notes": s.notes,
            "created_at": str(s.created_at),
        }
        for s in saves
    ]


@router.post("/detections/{detection_id}/save")
async def save_detection(
    detection_id: uuid.UUID,
    body: SaveCreate,
    db: AsyncSession = Depends(get_db),
):
    detection = await db.get(Detection, detection_id)
    if not detection:
        raise HTTPException(status_code=404, detail="Detection not found")

    save = SavedDetection(
        detection_id=detection_id,
        label=body.label,
        color=body.color,
        notes=body.notes,
    )
    db.add(save)
    await db.commit()
    await db.refresh(save)
    return {"id": str(save.id), "status": "saved"}


@router.delete("/saved/{save_id}")
async def unsave_detection(
    save_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    save = await db.get(SavedDetection, save_id)
    if not save:
        raise HTTPException(status_code=404, detail="Saved detection not found")
    await db.delete(save)
    await db.commit()
    return {"status": "removed"}
