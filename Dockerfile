FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

COPY src ./src
COPY LICENSE ./

RUN python -c "import faiss, fastapi, uvicorn; import src.service.app"

RUN addgroup --system app \
    && adduser --system --ingroup app app
USER app

EXPOSE 8000

CMD ["python", "-m", "src.service"]
