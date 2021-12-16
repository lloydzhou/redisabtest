FROM redis:alpine as builder

RUN apk add musl-dev gcc make

ADD . /server

RUN chown -R redis:redis /server && cd /server && make


FROM redis:alpine

COPY --from=builder /server/redisab.so /usr/local/bin/redisab.so

CMD ["redis-server", "--loadmodule", "/usr/local/bin/redisab.so"]



