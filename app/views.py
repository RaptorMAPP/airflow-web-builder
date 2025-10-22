from flask import Blueprint, render_template

ui_bp = Blueprint("ui", __name__)

@ui_bp.get("/")
def home():
    return render_template("index.html")

@ui_bp.get("/monitor")
def monitor():
    return render_template("monitor.html")
