# app/__init__.py
import os
from pathlib import Path
from flask import Flask
from flask_babel import Babel
from dotenv import load_dotenv

babel = Babel()

def create_app():
    BASE_DIR = Path(__file__).resolve().parent.parent

    load_dotenv(BASE_DIR / ".env")

    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),   # <-- apunta a /templates (raíz)
        static_folder=str(BASE_DIR / "static")         # opcional
    )

    app.config.update(
        SECRET_KEY=os.getenv("SECRET_KEY", "dev_change_me"),
        BABEL_DEFAULT_LOCALE="es",
        BABEL_TRANSLATION_DIRECTORIES="translations",
    )

    babel.init_app(app)

    from .views import ui_bp
    from .api_airflow import api_airflow_bp
    from .validator import validator_bp
    from .builder import builder_bp
    from .schedule_preview import schedule_preview_bp
    from .calendars import calendars_bp
    app.register_blueprint(calendars_bp)
    app.register_blueprint(schedule_preview_bp)
    app.register_blueprint(builder_bp)
    app.register_blueprint(validator_bp)
    app.register_blueprint(ui_bp)
    app.register_blueprint(api_airflow_bp, url_prefix="/api/airflow")

    return app
