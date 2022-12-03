
FROM ubuntu:20.04

ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN  --mount=type=cache,mode=0777,target=/var/cache/apt \
    apt-get update && \
    apt-get -y install git cmake libkrb5-dev make \
      openjdk-11-jdk python3 python3-pip python3-psycopg2 \
      python3-venv sqlite3 vim virtualenvwrapper

RUN --mount=type=cache,mode=0777,target=/var/cache/pip \
    pip install --upgrade pip

WORKDIR /opt/kclp

COPY requirements.txt .

RUN --mount=type=cache,mode=0777,target=/var/cache/pip \
    pip install -r requirements.txt

COPY . .

COPY docker-entrypoint.sh /

ENTRYPOINT ["./docker-entrypoint.sh"]
