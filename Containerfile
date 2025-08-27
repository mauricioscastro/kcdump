FROM golang:1.24.5 as builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
COPY pkg/ pkg/
COPY cmd/main.go cmd/main.go

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

FROM registry.access.redhat.com/ubi9-minimal
COPY --from=builder /workspace/manager /bin/kcdump
RUN adduser kcdump -u 1000 -d /tmp/kcdump && \
    mkdir -p /tmp/kcdump && \
    chown -R kcdump:kcdump /tmp/kcdump 

WORKDIR /tmp/kcdump 

USER kcdump:kcdump

ENV KCD_TARGETDIR=/tmp/kcdump

ENTRYPOINT ["/bin/kcdump"]
