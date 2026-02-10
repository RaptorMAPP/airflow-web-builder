FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias del sistema (si necesitas compilar algunas libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Si usas gunicorn, expone el puerto interno
EXPOSE 8000

# Healthcheck opcional (si tienes endpoint /health)
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

# Ajusta el módulo según tu proyecto:
# - si tienes wsgi.py con "app = create_app()" o "app = Flask(__name__)"
CMD ["gunicorn", "-b", "0.0.0.0:8000", "wsgi:app", "--workers=2", "--threads=4", "--timeout=120"]
