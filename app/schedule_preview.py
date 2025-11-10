# -*- coding: utf-8 -*-
from flask import Blueprint, request, jsonify
from croniter import croniter
import pendulum
from datetime import datetime as _dt

schedule_preview_bp = Blueprint("schedule_preview_bp", __name__)

@schedule_preview_bp.post("/api/schedule/preview")
def preview():
    payload = request.get_json(silent=True) or {}
    cron = (payload.get("cron") or "").strip()
    tz = (payload.get("timezone") or "UTC").strip()
    count = int(payload.get("count") or 5)
    if not cron:
        return jsonify(error="Falta cron"), 400
    try:
        tzinfo = pendulum.timezone(tz)
    except Exception:
        return jsonify(error=f"Timezone inválido: {tz}"), 400
    try:
        now = pendulum.now(tzinfo)
        it = croniter(cron, now)
        runs = []
        for _ in range(max(1, min(20, count))):
            nxt = it.get_next(_dt)
            runs.append(pendulum.instance(nxt, tzinfo).to_datetime_string())
        return jsonify(next_runs=runs, base=str(now)), 200
    except Exception as e:
        return jsonify(error=str(e)), 400
