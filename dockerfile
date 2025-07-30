# docker build . -t unwrap --platform linux/amd64 --build-arg GIT_COMMIT=$(git rev-parse --short HEAD) --build-arg BUILD_DATE=$(date +'%Y%m%d')
# docker tag unwrap:latest us-central1-docker.pkg.dev/assetpipeline-345/util/product-capture
# docker push us-central1-docker.pkg.dev/assetpipeline-345/util/product-capture

# test locally
# specific folder:
# docker run -it --rm -e DEBUG='TRUE' -e ORGANISATION_UUID='5471ef92-5c66-4355-88fe-b33a9cebda09' -e VQ_FILES_FOLDER='c452b648-b041-43cb-acf9-4caaf77bf6b4' -e VQ_URL='https://api.345.global' -e VQ_KEY='insert key' unwrap
# get jobs from vq jobs (ensure there are no jobs waiting though??)
# docker run -it --rm -e DEBUG='TRUE' -e ORGANISATION_UUID='5471ef92-5c66-4355-88fe-b33a9cebda09' -e VQ_URL='https://api.345.global' -e VQ_KEY='insert key' unwrap

FROM nvidia/cudagl:11.4.1-base-ubuntu20.04

# Environment variables
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
&& apt-get install -y gcc git ffmpeg libx11-dev libsm6 libxext6 python3.10 python3-pip libvulkan-dev \
&& apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY run_job.py .
COPY productcapture productcapture
COPY utils utils
COPY vq vq

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "-vv", "--"]

ENV PYTHONUNBUFFERED true

ARG BUILD_DATE
ENV BUILD_DATE=$BUILD_DATE

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

# CMD exec python3 run_job.py --cloud
CMD ["python3", "run_job.py", "--cloud"]