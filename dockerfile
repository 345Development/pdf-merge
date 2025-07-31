# docker build . -t gcr.io/core-345/pdf-merger --platform linux/amd64 --build-arg GIT_COMMIT=$(git rev-parse --short HEAD) --build-arg BUILD_DATE=$(date +'%Y%m%d')
# docker tag pdf-merger:latest gcr.io/core-345/pdf-merger
# docker push gcr.io/core-345/pdf-merger

FROM python:3.12-slim

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY run_job.py .
COPY utils utils
COPY vq vq

ENV PYTHONUNBUFFERED true

ARG BUILD_DATE
ENV BUILD_DATE=$BUILD_DATE

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

# CMD exec python3 run_job.py
CMD ["python3", "run_job.py"]