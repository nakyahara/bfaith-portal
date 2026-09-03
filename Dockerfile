FROM node:20-slim

# Python + system dependencies for PyMuPDF, OpenCV, pyzbar
# fonts-takao-gothic: reportlab(ピッキング準備のTMP1 PDF注番)が日本語TTFを必要とするため
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    libzbar0 libgl1-mesa-glx libglib2.0-0 \
    fonts-takao-gothic \
    curl unzip \
    && rm -rf /var/lib/apt/lists/*

# rclone: Render一次データの Google Drive offsite バックアップ用 (apps/render-backup)
# バージョン固定 + SHA256検証 (再現性と改ざん検出。miniPC側と同じ v1.74.4)
RUN curl -fsSL https://downloads.rclone.org/v1.74.4/rclone-v1.74.4-linux-amd64.zip -o /tmp/rclone.zip \
    && echo "fe435e0c36228e7c2f116a8701f01127bb1f694005fc11d1f27186c8bca4115d  /tmp/rclone.zip" | sha256sum -c - \
    && unzip -j /tmp/rclone.zip -d /tmp/rclone \
    && mv /tmp/rclone/rclone /usr/local/bin/rclone \
    && chmod +x /usr/local/bin/rclone \
    && rm -rf /tmp/rclone.zip /tmp/rclone \
    && rclone version

WORKDIR /app

# Node.js dependencies
COPY package.json package-lock.json* ./
RUN npm ci --production

# Python dependencies (in virtual env)
COPY apps/aes-pdf-sorter/python/requirements.txt /app/apps/aes-pdf-sorter/python/requirements.txt
COPY apps/fba-replenishment/python/requirements.txt /app/apps/fba-replenishment/python/requirements.txt
COPY apps/fba-box/python/requirements.txt /app/apps/fba-box/python/requirements.txt
RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --no-cache-dir -r /app/apps/aes-pdf-sorter/python/requirements.txt && \
    /app/venv/bin/pip install --no-cache-dir -r /app/apps/fba-replenishment/python/requirements.txt && \
    /app/venv/bin/pip install --no-cache-dir -r /app/apps/fba-box/python/requirements.txt

# Copy application
COPY . .

# Set Python path for child process
ENV PATH="/app/venv/bin:$PATH"
ENV PORT=3000

EXPOSE 3000

CMD ["node", "server.js"]
