FROM debian:buster
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update && \
    apt-get -qq --no-install-recommends install devscripts equivs lintian python3-pip sudo
# Install not packaged dependencies with Pip.
RUN python3 -m pip install pgtoolkit
RUN useradd --create-home runner
RUN echo "runner ALL=NOPASSWD: ALL" >> /etc/sudoers.d/runner

USER runner
WORKDIR /home/runner
