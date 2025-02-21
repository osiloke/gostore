FROM alpine:latest
LABEL Osiloke Emoekpere <me@osiloke.com>
RUN apk update && apk add --no-cache bash ca-certificates && rm -rf /var/cache/apk/*
ADD cli /app/gostore
RUN chmod +x /app/gostore
COPY docker-entrypoint.sh /
VOLUME /app/db
WORKDIR /app
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/app/gostore", "server"]