FROM python:3.9.5

# Send stdout/stderr out, do not buffer.
ENV PYTHONUNBUFFERED 1

# Copy application dependency manifests to the container image.
# Copying this separately prevents re-running pip install on every code change.
COPY requirements.txt ./

RUN apt update; apt -y install cpuid stress build-essential gcc;

# Install production dependencies.
RUN set -ex; pip install -r requirements.txt;

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN make;

# The main.py uses gevent in the SocketIO app, omit gunicore
CMD exec python3 main.py
