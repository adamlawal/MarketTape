FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 markettape \
    && mkdir -p /data \
    && chown markettape:markettape /data
COPY markettape ./markettape

USER markettape
EXPOSE 8000
CMD ["uvicorn", "markettape.app:app", "--host", "0.0.0.0", "--port", "8000"]
