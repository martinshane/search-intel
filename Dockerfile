# ----------  build stage (install Python deps with build tools)  ----------
FROM python:3.11-slim AS builder

# System deps needed to compile numpy, scipy, scikit-learn wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gfortran libopenblas-dev liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ----------  runtime stage (slim, no compilers)  ----------
FROM python:3.11-slim

# Minimal runtime libs for scipy / numpy linked against OpenBLAS
RUN apt-get update && apt-get install -y --no-install-recommends \
        libopenblas0 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app
COPY . .

# Railway injects $PORT at runtime (default 8000 for local dev)
ENV PORT=8000
EXPOSE ${PORT}

CMD uvicorn api.main:app --host 0.0.0.0 --port ${PORT}
