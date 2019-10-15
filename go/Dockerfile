FROM golang:1.12-alpine

LABEL maintainer="Prashant Shahi <prashant@dgraph.io>"

ENV GO111MODULE on

WORKDIR /app

COPY . ./

RUN  \
    apk add --no-cache git && \
    go install -v

ENTRYPOINT ["/go/bin/flock"]

CMD ["-c","/app/credentials.json","-a","alpha1:9180,alpha2:9182,alpha3:9183"]
