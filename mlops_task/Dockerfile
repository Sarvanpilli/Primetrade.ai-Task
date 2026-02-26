FROM python:3.9-slim
WORKDIR /app
COPY requirenments.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "run.py"]
CMD [ \
    "--input", "data.csv", \
    "--config", "config.yaml", \
    "--output", "metrics.json", \
    "--log-file", "run.log" \
]
