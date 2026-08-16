FROM python:3.13-slim
WORKDIR /app
COPY . .
RUN mkdir -p run data/raw
CMD ["python", "-m", "src.pipeline", "--config", "config/pipeline.json"]