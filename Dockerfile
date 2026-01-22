# Nutze ein leichtgewichtiges Python Image
FROM python:3.12-slim AS stage1

WORKDIR /app

COPY requirements.txt .

# Installation of build-tools, then pip install, then delete the tools
RUN apt-get update && apt-get install -y \
    gcc \
    libmariadb-dev \
    pkg-config \
    && pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim AS stage2
WORKDIR /app
COPY --from=stage1 /root/.local /root/.local
COPY --from=stage1 /usr/lib/aarch64-linux-gnu/libmariadb* /usr/lib/aarch64-linux-gnu/
ENV PATH=/root/.local/bin:$PATH

# Copy the source code from the src folder into the working directory
COPY backend/src/ .

# Port for backend
EXPOSE 8765

CMD ["python", "money_pilot.py"]