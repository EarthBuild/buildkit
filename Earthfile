
VERSION 0.6

FROM alpine:3.13
WORKDIR /buildkit

# build buildkit image
build:
    ARG RELEASE_VERSION=v0.0.0+earthlyunknown
    # REVISION is baked into `buildkitd --version` output. It used to be read
    # from .git inside the buildkit-version stage; see the comment there for why
    # that is now passed in instead. Callers that want a real revision should
    # pass the pinned buildkit git sha they are building.
    ARG REVISION=unknown
    FROM DOCKERFILE --build-arg RELEASE_VERSION=$RELEASE_VERSION --build-arg REVISION=$REVISION --target buildkit-linux .

code:
    COPY . .
    SAVE ARTIFACT /buildkit
