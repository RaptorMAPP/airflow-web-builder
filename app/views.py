from flask import Blueprint, render_template

ui_bp = Blueprint("ui_bp", __name__)

@ui_bp.get("/")
def home():
    return render_template("index.html")

@ui_bp.get("/builder")
def builder():
    return render_template("builder.html")

@ui_bp.get("/monitor")
def monitor():
    return render_template("monitor.html")

@ui_bp.get("/validate")
def validate_page():
    return render_template("validate.html")
