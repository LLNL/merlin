FROM ubuntu:18.04
LABEL maintainer="Joe Koning koning1@llnl.gov"

ARG USER=merlinu
ARG UID=1000

ADD . /merlin

RUN \
  apt-get update && \
  apt-get install --no-install-recommends -y python python-pip python3.8 python3-pip python3.8-venv python3.8-distutils python3.8-dev libyaml-dev sudo build-essential && \
  rm -rf /var/lib/apt/lists/* && \
  python3.8 -m pip install setuptools -U && \
  python3.8 -m pip install pip -U && \
  python3.8 -m pip install cffi cython -U && \
  groupadd -g ${UID} ${USER} && \
  useradd -ms /bin/bash -g ${USER} $USER && \
  sh -c "printf \"$USER ALL= NOPASSWD: ALL\\n\" >> /etc/sudoers" && \
  adduser $USER sudo && \
  cd /merlin && \
  python3.8 setup.py install && \
  python3.8 -m pip install -r requirements/dev.txt  && \
  python3.8 -m pip install -r merlin/examples/workflows/feature_demo/requirements.txt && \
  rm /usr/bin/python3 && ln -s /usr/bin/python3.8 /usr/bin/python3 && \
  apt-get remove -y build-essential && apt-get autoremove -y

USER $USER
WORKDIR /home/$USER
