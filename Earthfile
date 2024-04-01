VERSION 0.8

FROM alpine:3.19
WORKDIR /buildkit

build:
    ARG RELEASE_VERSION=v0.0.0+earthlyunknown
    FROM DOCKERFILE --build-arg RELEASE_VERSION=$RELEASE_VERSION --target buildkit-linux .

code:
    COPY . .
    SAVE ARTIFACT /buildkit
