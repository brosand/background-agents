# Sandbox API Service
#
# Manages Kubernetes sandbox pods. Replaces Modal's web_api.py endpoints.
# Runs as a K8s Deployment with access to the K8s API for pod management.

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
# Copy shared auth modules from the existing codebase (they are portable)
COPY auth/ ./auth/

ENV PYTHONPATH=/app
EXPOSE 8080

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8080"]
