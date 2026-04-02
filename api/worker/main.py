"""
Worker service entrypoint.

Exposes a minimal FastAPI app that accepts report generation jobs
from the main API via internal HTTP calls.
"""

import os
import logging
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Optional

from .report_runner import run_report_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Search Intel Worker", version="1.0.0")


class ReportJob(BaseModel):
    report_id: str
    user_id: str
    gsc_property: str
    ga4_property: Optional[str] = None
    domain: str


@app.get("/health")
def health():
    return {"status": "ok", "service": "worker"}


@app.post("/jobs/report")
async def create_report_job(job: ReportJob, background_tasks: BackgroundTasks):
    """Accept a report generation job and run it in the background."""
    logger.info(f"Received report job: {job.report_id}")
    background_tasks.add_task(
        run_report_pipeline,
        report_id=job.report_id,
        user_id=job.user_id,
        gsc_property=job.gsc_property,
        ga4_property=job.ga4_property,
        domain=job.domain
    )
    return {"status": "accepted", "report_id": job.report_id}
