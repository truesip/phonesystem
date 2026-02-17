# Phone.System – AI Agent Platform (Node.js)
# DigitalOcean App Platform: Dockerfile-based deploy for system deps (LibreOffice for DOCX→PDF).

FROM node:20-bookworm-slim

# System deps for DOCX -> PDF conversion (LibreOffice) + fonts
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libreoffice-writer \
    fontconfig \
    fonts-dejavu-core \
    fonts-dejavu-extra \
    fonts-liberation \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Node dependencies first (better layer caching)
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy app source
COPY . .

ENV NODE_ENV=production
# Optional: explicit path for the conversion helper
ENV LIBREOFFICE_BIN=/usr/bin/soffice

EXPOSE 8080

CMD ["node", "server.js"]
