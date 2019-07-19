FROM golang:1.12-alpine

LABEL maintainer="Karthic Rao<karthic@dgraph.io>"

ENV GO111MODULE on

WORKDIR /app

RUN  \
     apk add --no-cache git && \
     git clone https://github.com/dgraph-io/flock.git && cd flock && \
     go install -v

ENTRYPOINT ["/go/bin/flock"]

CMD ["-c","/app/credentials.json","-a","alpha1:9180,alpha2:9182,alpha3:9183"]

