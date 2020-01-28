FROM ubuntu
LABEL maintainer="Joe Koning koning1@llnl.gov"

ADD . /merlin
RUN \
  apt-get update && apt-get install -y python3.8 python3-pip python3.8-venv python3.8-distutils python3.8-dev git redis nano vim emacs libyaml-dev && \
  cd /merlin && \
  python3.8 -m pip install setuptools -U && \
  python3.8 -m pip install pip -U && \
  python3.8 setup.py install

ENTRYPOINT ["/usr/local/bin/merlin"]
