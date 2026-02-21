# Pipecat Cloud agent image
# NOTE: Pipecat Cloud expects Linux/ARM64 images.
#
# Use the official Pipecat base image, which includes the HTTP runner and
# entrypoint logic. We only need to add our dependencies and bot.py.
FROM dailyco/pipecat-base:latest

# Install Python dependencies for this bot.
# The base image already includes core system deps (ffmpeg, etc.).
COPY requirements.txt ./requirements.txt
# Use BuildKit cache for faster rebuilds (cache is not persisted in the final image)
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir -r requirements.txt

# Application entrypoint consumed by the base image.
COPY bot.py ./bot.py

# Do not override CMD; base image starts the Pipecat runner and imports bot.py
