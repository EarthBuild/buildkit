
VERSION 0.6

FROM alpine:3.13
WORKDIR /buildkit

# build buildkit image
build:
    ARG RELEASE_VERSION=v0.0.0+earthlyunknown
    FROM DOCKERFILE --build-arg RELEASE_VERSION=$RELEASE_VERSION --target buildkit-linux .

code:
    COPY . .
    SAVE ARTIFACT /buildkit
