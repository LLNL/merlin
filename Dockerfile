FROM ubuntu
LABEL maintainer="Joe Koning koning1@llnl.gov"

ARG USER=merlinu
ARG UID=1000

ADD . /merlin
RUN \
  apt-get update && \
  apt-get install -y python3.8 python3-pip python3.8-venv python3.8-distutils python3.8-dev git redis nano vim emacs libyaml-dev && \
  cd /merlin && \
  python3.8 -m pip install setuptools -U && \
  python3.8 -m pip install pip -U && \
  python3.8 -m pip install cffi -U && \
  python3.8 setup.py install && \
  groupadd -g $UID $USER && \
  useradd -g $USER -u $UID -d /home/$USER -m $USER && \
  sh -c "printf \"$USER ALL= NOPASSWD: ALL\\n\" >> /etc/sudoers" && \
  adduser $USER sudo  && \
  ln -s `which python3.8` /usr/bin/python

USER $USER
WORKDIR /home/$USER

RUN \
  merlin config

ENTRYPOINT ["/usr/local/bin/merlin"]
