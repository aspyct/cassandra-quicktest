FROM alpine

RUN apk add python3
RUN python3 -m pip install cassandra-driver blessings docopt

COPY query_loop.py /usr/local/bin
COPY sleep_loop.py /usr/local/bin

CMD /usr/local/bin/sleep_loop.py
