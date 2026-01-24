# Nutze ein leichtgewichtiges Python Image
FROM python:3.12-slim AS stage1
WORKDIR /app

# Installation of build-tools, then pip install, then delete the tools
RUN apt-get update && apt-get install -y \
    gcc \
    libmariadb-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim AS stage2
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libmariadb3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=stage1 /install /usr/local

# Copy health checker
COPY backend/health_check/ ./health_check/

# Get version information into environment
ARG APP_VERSION=unknown
ENV VERSION=$APP_VERSION

# Copy the source code from the src folder into the working directory
COPY backend/src/ .

# Port for backend
EXPOSE 8765

CMD ["python", "money_pilot.py"]
