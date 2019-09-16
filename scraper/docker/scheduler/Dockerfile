FROM phusion/baseimage:0.11

# Setup Pipenv
RUN install_clean \
  python3-pip \
  python3-setuptools \
  python3-wheel && \
  pip3 install pipenv

# Setup Python libraries dependencies
RUN install_clean \
  gcc \
  python3-dev \
  libpq-dev

# Install Supervisor
RUN install_clean supervisor

# Setup
COPY . /scraper
WORKDIR /scraper
RUN pipenv install

CMD supervisord -c docker/scheduler/supervisor-scheduler.conf
