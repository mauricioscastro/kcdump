FROM registry.access.redhat.com/hi/go:latest AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
COPY pkg/ pkg/
COPY cmd/main.go cmd/main.go

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o kcdump cmd/main.go

FROM registry.access.redhat.com/hi/static:latest
COPY --from=builder /workspace/kcdump /bin/

ENV KCD_TARGETDIR=/tmp/kcdump

ENTRYPOINT ["/bin/kcdump"]
