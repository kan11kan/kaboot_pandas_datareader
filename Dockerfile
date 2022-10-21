FROM python:3-slim
USER root

COPY requirements.txt /
RUN apt-get update && \
    apt-get install -y curl && \
    pip install --upgrade pip && \
    pip install --upgrade -r requirements.txt