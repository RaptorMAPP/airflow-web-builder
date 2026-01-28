from __future__ import annotations

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

    def next_dagrun_info(self,last_automated_data_interval: Optional[DataInterval],restriction: TimeRestriction,) -> Optional[DagRunInfo]:
        dates, tz = self._get_dates_and_tz()
        if not dates:
            return None

        now_tz = pendulum.now(tz)

        after = restriction.earliest or now_tz

        # ✅ Si no hay catchup, no permitas que 'after' quede en el pasado
        if not restriction.catchup:
            after = max(after, now_tz)

        if last_automated_data_interval:
            # ✅ Asegura TZ consistente
            last_end = last_automated_data_interval.end.in_timezone(tz)
            after = max(after, last_end)

        after = after.in_timezone(tz)
        latest = restriction.latest.in_timezone(tz) if restriction.latest else None

        # ✅ Asegura orden (por si dates viene desordenado)
        for ds in sorted(dates):
            try:
                y, m, day = map(int, ds.split("-"))
            except Exception:
                continue

            for candidate in self._candidates_for_date(y, m, day, tz):
                # ✅ CLAVE: evitar candidate == after (esto te estaba “pegando” el next)
                if candidate <= after:
                    continue

                if latest and candidate > latest:
                    return None

                return DagRunInfo.exact(candidate)

        return None

