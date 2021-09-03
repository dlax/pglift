FROM python:3.6
RUN apt-get -qq update && \
    apt-get -qq -y install curl ca-certificates gnupg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN curl --output /etc/apt/trusted.gpg.d/pgdg.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN apt-get -qq update && \
    apt-get -y install --no-install-recommends postgresql-10 postgresql-13 pgbackrest prometheus-postgres-exporter
RUN pip3 install tox
RUN useradd --create-home runner

USER runner
WORKDIR /home/runner
