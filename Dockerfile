FROM node:20-slim

# Python + system dependencies for PyMuPDF, OpenCV, pyzbar
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    libzbar0 libgl1-mesa-glx libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Node.js dependencies
COPY package.json package-lock.json* ./
RUN npm ci --production

# Python dependencies (in virtual env)
COPY apps/aes-pdf-sorter/python/requirements.txt /app/apps/aes-pdf-sorter/python/requirements.txt
RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --no-cache-dir -r /app/apps/aes-pdf-sorter/python/requirements.txt

# Copy application
COPY . .

# Set Python path for child process
ENV PATH="/app/venv/bin:$PATH"
ENV PORT=3000

EXPOSE 3000

CMD ["node", "server.js"]
