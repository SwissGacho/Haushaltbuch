# Nutze ein leichtgewichtiges Python Image
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

# Installation of build-tools, then pip install, then delete the tools
RUN apt-get update && apt-get install -y \
    gcc \
    libmariadb-dev \
    pkg-config \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy the source code from the src folder into the working directory
COPY backend/src/ .

# Port for backend
EXPOSE 8765

CMD ["python", "money_pilot.py"]