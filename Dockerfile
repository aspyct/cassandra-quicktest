FROM alpine

RUN apk add python3
RUN python3 -m pip install cassandra-driver blessings docopt

COPY query_loop.py /root

CMD /bin/sh
