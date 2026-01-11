FROM python:3

WORKDIR /app

COPY .py .
COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3",  "/app/money_pilot.py"]


# Basis-Image für ARM64 (Orange Pi) mit Python 3.12
FROM python:3.12-slim

# Arbeitsverzeichnis im Container
WORKDIR /app

# System-Abhängigkeiten für git und asyncmy (Kompilierung)
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# RUN apt-get update && apt-get install -y \
#     git \
#     gcc libmariadb-dev pkg-config \
#     && rm -rf /var/lib/apt/lists/*

# Das Repository als Argument (kann in der compose überschrieben werden)
ARG REPO_URL="https://github.com/SwissGacho/Haushaltbuch.git"
# Standardmäßig nehmen wir 'main', man kann aber beim Build ein Tag/Branch übergeben
ARG VERSION="main"

# TRICK: Dieser ARG sorgt dafür, dass ab hier der Cache ungültig wird,
# wenn wir beim Build einen neuen Zeitstempel übergeben.
ARG CACHEBUST=1

# Code von GitHub klonen
RUN git clone --depth 1 -b ${VERSION} ${REPO_URL} .

# Abhängigkeiten installieren
RUN pip install --no-cache-dir -r backend/deployment/requirements.txt

# Dein Websocket-Port
EXPOSE 8765

# Start des Python-Servers
CMD ["python", "backend/src/money_pilot.py"]