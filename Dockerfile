FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir ".[api]" && useradd --create-home appuser
USER appuser
EXPOSE 8000
CMD ["uvicorn", "bharatpay_mcp.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
