FROM python:3.12-slim

WORKDIR /app

COPY monitoring_agent ./monitoring_agent
RUN pip install --no-cache-dir -r monitoring_agent/requirements.txt


ENV PYTHONPATH=/app
ENV PORT=8080

CMD ["python", "-m", "monitoring_agent.app"]
