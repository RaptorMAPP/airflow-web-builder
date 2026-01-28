# app/calendars.py
from __future__ import annotations

import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, current_app, jsonify, render_template, request, send_file


calendars_bp = Blueprint("calendars", __name__)

# --- Airflow Timetable export (se empaqueta en el ZIP) ---
TIMETABLE_INIT = ""  # __init__.py vacío

TIMETABLE_PY = r'''from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pendulum
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


@lru_cache(maxsize=256)
def _load_calendar_dates(json_path: str) -> Tuple[str, Sequence[str], str]:
    p = Path(json_path)
    data = json.loads(p.read_text(encoding="utf-8"))
    name = str(data.get("name") or p.stem)
    tz = str(data.get("timezone") or "UTC")
    dates = data.get("dates") or []
    # Normaliza y ordena
    dates = sorted({str(d) for d in dates})
    return name, dates, tz


@dataclass(frozen=True)
class CalendarTimetable(Timetable):
    """
    Timetable basado en un calendario exportado (JSON) + hora fija o modo cíclico.

    JSON esperado (include/calendars/<calendar>.json):
      {
        "name": "...",
        "timezone": "America/Santiago",
        "dates": ["2026-01-02", ...]
      }

    Modos:
      - No cíclico: 1 corrida por cada fecha activa, a la hora 'at' (HH:MM).
      - Cíclico: múltiples corridas dentro de la ventana [window_from, window_to],
        en minutos u horas según 'unit'.
    """
    calendar: str
    at: str = "00:00"
    timezone: Optional[str] = None  # si None, usa el timezone del JSON

    # cyclic
    cyclic: bool = False
    every: int = 15
    unit: str = "minutes"  # minutes | hours
    window_from: int = 0
    window_to: int = 23
    at_minute: int = 0  # usado si unit=hours

    def serialize(self) -> dict:
        return {
            "calendar": self.calendar,
            "at": self.at,
            "timezone": self.timezone,
            "cyclic": self.cyclic,
            "every": self.every,
            "unit": self.unit,
            "window_from": self.window_from,
            "window_to": self.window_to,
            "at_minute": self.at_minute,
        }

    @classmethod
    def deserialize(cls, data: dict) -> "CalendarTimetable":
        return cls(**data)

    def _paths(self) -> Path:
        include_dir = Path(__file__).resolve().parents[1]
        return include_dir / "calendars" / f"{self.calendar}.json"

    def _get_dates_and_tz(self) -> Tuple[Sequence[str], str]:
        json_path = str(self._paths())
        _, dates, tz = _load_calendar_dates(json_path)
        return dates, (self.timezone or tz or "UTC")

    def _parse_at(self) -> Tuple[int, int]:
        try:
            hh, mm = self.at.split(":")[:2]
            return int(hh), int(mm)
        except Exception:
            return 0, 0

    def infer_manual_data_interval(self, run_after) -> DataInterval:
        return DataInterval(run_after, run_after)

    def _candidates_for_date(self, y: int, m: int, d: int, tz: str):
        if not self.cyclic:
            hour, minute = self._parse_at()
            yield pendulum.datetime(y, m, d, hour, minute, tz=tz)
            return

        every = max(1, int(self.every))
        unit = (self.unit or "minutes").strip()
        if unit not in ("minutes", "hours"):
            unit = "minutes"

        f = max(0, min(23, int(self.window_from)))
        t = max(0, min(23, int(self.window_to)))
        if f > t:
            f, t = t, f

        if unit == "hours":
            mm = max(0, min(59, int(self.at_minute)))
            step = max(1, min(23, every))
            for hh in range(f, t + 1, step):
                yield pendulum.datetime(y, m, d, hh, mm, tz=tz)
        else:
            step = max(1, min(59, every))
            for hh in range(f, t + 1):
                for mm in range(0, 60, step):
                    yield pendulum.datetime(y, m, d, hh, mm, tz=tz)

    def next_dagrun_info(
        self,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        dates, tz = self._get_dates_and_tz()
        if not dates:
            return None

        now_tz = pendulum.now(tz)
        after = restriction.earliest or now_tz
        if last_automated_data_interval:
            after = max(after, last_automated_data_interval.end)
        after = after.in_timezone(tz)

        latest = restriction.latest.in_timezone(tz) if restriction.latest else None

        for ds in dates:
            try:
                y, m, day = map(int, ds.split("-"))
            except Exception:
                continue

            for candidate in self._candidates_for_date(y, m, day, tz):
                if candidate < after:
                    continue
                if latest and candidate > latest:
                    return None
                return DagRunInfo.exact(candidate)

        return None
'''

# --- Utils almacenamiento en archivos JSON ---
NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_\-]{0,80}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _cal_dir() -> Path:
    base = current_app.config.get("CALENDARS_DIR")
    if base:
        p = Path(base)
    else:
        # instance/calendars por defecto
        p = Path(current_app.instance_path) / "calendars"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sanitize_name(name: str) -> str:
    name = (name or "").strip().replace(" ", "_")
    if not NAME_RE.match(name):
        raise ValueError("Nombre inválido. Usa letras/números/_/- y parte con alfanumérico.")
    return name


def _parse_date(s: str) -> date:
    if not DATE_RE.match(s):
        raise ValueError(f"Fecha inválida: {s} (usa YYYY-MM-DD)")
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def _dump_calendar_obj(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


def _load_one(name: str) -> Dict[str, Any]:
    name = _sanitize_name(name)
    f = _cal_dir() / f"{name}.json"
    if not f.exists():
        raise FileNotFoundError(name)
    return json.loads(f.read_text(encoding="utf-8"))


def _list_all() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for f in sorted(_cal_dir().glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            out.append({
                "name": data.get("name", f.stem),
                "timezone": data.get("timezone", "UTC"),
                "count": len(data.get("dates", []) or []),
                "updated_at": data.get("updated_at"),
            })
        except Exception:
            continue
    return out


def _generate_dates_business_days(
    year_from: int,
    year_to: int,
    weekdays: List[int],
    exclude: List[str],
    include: List[str],
) -> List[str]:
    if year_from > year_to:
        year_from, year_to = year_to, year_from

    wd = set(int(x) for x in weekdays or [0, 1, 2, 3, 4])
    excl = set(_parse_date(x).isoformat() for x in exclude or [])
    incl = set(_parse_date(x).isoformat() for x in include or [])

    start = date(year_from, 1, 1)
    end = date(year_to, 12, 31)

    cur = start
    dates: List[str] = []
    while cur <= end:
        if cur.weekday() in wd:
            iso = cur.isoformat()
            if iso not in excl:
                dates.append(iso)
        cur += timedelta(days=1)

    # inclusiones mandan (aunque sea fin de semana)
    for x in incl:
        if x not in dates:
            dates.append(x)

    dates = sorted(set(dates))
    return dates


def _build_calendar(payload: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
    name = _sanitize_name(payload.get("name") or "")
    tz = (payload.get("timezone") or "UTC").strip() or "UTC"
    mode = (payload.get("mode") or "business_days").strip()

    exclude = payload.get("exclude") or []
    include = payload.get("include") or []
    weekdays = payload.get("weekdays")
    if weekdays is None:
        weekdays = [0, 1, 2, 3, 4]
    year_from = int(payload.get("year_from") or date.today().year)
    year_to = int(payload.get("year_to") or year_from)

    if mode == "expanded_dates":
        dates_in = payload.get("dates") or []
        dates = sorted({_parse_date(x).isoformat() for x in dates_in})
        meta = {
            "type": "expanded_dates",
            "exclude": [*_normalize_lines(exclude)],
            "include": [*_normalize_lines(include)],
            "weekdays": [int(x) for x in weekdays],
            "year_from": year_from,
            "year_to": year_to,
        }
    else:
        dates = _generate_dates_business_days(year_from, year_to, weekdays, exclude, include)
        meta = {
            "type": "business_days",
            "exclude": [*_normalize_lines(exclude)],
            "include": [*_normalize_lines(include)],
            "weekdays": [int(x) for x in weekdays],
            "year_from": year_from,
            "year_to": year_to,
        }

    obj = {
        "name": name,
        "timezone": tz,
        "dates": dates,
        "meta": meta,
    }

    if not dry_run:
        obj["updated_at"] = datetime.utcnow().isoformat() + "Z"

    return obj


def _normalize_lines(v: Any) -> List[str]:
    if not v:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [x.strip() for x in v.splitlines() if x.strip()]
    return [str(v).strip()]


# --- Views ---
@calendars_bp.route("/calendars")
def calendars_page():
    return render_template("calendars.html")


# --- APIs ---
@calendars_bp.get("/api/calendars")
def api_list():
    return jsonify(_list_all())


@calendars_bp.get("/api/calendars/<name>")
def api_get(name: str):
    try:
        return jsonify(_load_one(name))
    except FileNotFoundError:
        return jsonify({"error": "not_found"}), 404


@calendars_bp.post("/api/calendars/preview")
def api_preview():
    payload = request.get_json(force=True) or {}
    obj = _build_calendar(payload, dry_run=True)
    return jsonify({"dates": obj["dates"], "meta": "Preview generado (no guardado)"})


@calendars_bp.post("/api/calendars")
def api_save():
    payload = request.get_json(force=True) or {}
    obj = _build_calendar(payload, dry_run=False)
    f = _cal_dir() / f"{obj['name']}.json"
    f.write_bytes(_dump_calendar_obj(obj))
    return jsonify(obj)


@calendars_bp.delete("/api/calendars/<name>")
def api_delete(name: str):
    name = _sanitize_name(name)
    f = _cal_dir() / f"{name}.json"
    if f.exists():
        f.unlink()
    return jsonify({"ok": True})


@calendars_bp.post("/api/calendars/import")
def api_import():
    if "file" not in request.files:
        return jsonify({"error": "missing_file"}), 400
    file = request.files["file"]
    data = json.loads(file.read().decode("utf-8"))
    name = _sanitize_name(data.get("name") or Path(file.filename or "calendar.json").stem)
    data["name"] = name
    if "timezone" not in data:
        data["timezone"] = "UTC"
    if "dates" not in data or not isinstance(data["dates"], list):
        return jsonify({"error": "invalid_json", "detail": "missing dates[]"}), 400
    # normaliza fechas
    data["dates"] = sorted({_parse_date(x).isoformat() for x in data["dates"]})
    data["updated_at"] = datetime.utcnow().isoformat() + "Z"
    if "meta" not in data:
        data["meta"] = {"type": "expanded_dates"}

    f = _cal_dir() / f"{name}.json"
    f.write_bytes(_dump_calendar_obj(data))
    return jsonify({"ok": True, "name": name})


@calendars_bp.get("/api/calendars/export_bundle")
def api_export_bundle():
    """
    ZIP listo para copiar dentro del repo de DAGs:
      dags/include/ctm_calendar/timetable.py
      dags/include/ctm_calendar/__init__.py
      dags/include/calendars/*.json
      dags/include/__init__.py
    """
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("dags/include/__init__.py", "")
        z.writestr("dags/include/ctm_calendar/__init__.py", TIMETABLE_INIT)
        z.writestr("dags/include/ctm_calendar/timetable.py", TIMETABLE_PY)

        for f in sorted(_cal_dir().glob("*.json")):
            z.writestr(f"dags/include/calendars/{f.name}", f.read_bytes())

    mem.seek(0)
    return send_file(
        mem,
        as_attachment=True,
        download_name="calendars_bundle.zip",
        mimetype="application/zip",
    )
