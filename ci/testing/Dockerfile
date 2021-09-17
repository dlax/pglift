FROM python:3.6-slim
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update && \
	apt-get -qq -y install --no-install-recommends \
	curl \
	ca-certificates \
	gnupg \
	lsb-release \
	;
RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -c -s)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN curl --output /etc/apt/trusted.gpg.d/pgdg.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN apt-get -qq update && \
	apt-get -y install --no-install-recommends \
	build-essential \
	git \
	libpq-dev \
	procps \
	python3-dev \
	python3-pip \
	python3-venv \
	postgresql-10 \
	postgresql-13 \
	pgbackrest \
	prometheus-postgres-exporter \
	;
RUN pip3 install tox
RUN useradd --create-home runner

USER runner
WORKDIR /home/runner
