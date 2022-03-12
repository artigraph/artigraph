FROM python:3.10

ENV VERSION=0.0.1
ENV WORKDIR=/usr/src/app
WORKDIR $WORKDIR

RUN python3 -m venv .venv
ENV VIRTUAL_ENV=$WORKDIR/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip3 install "arti==${VERSION}"

COPY demo.py .

CMD ["python3", "demo.py"]
