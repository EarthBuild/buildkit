
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

validate-skills:
    FROM python:3.15.0-slim@sha256:f48fda6d59b2b3437781d1c3ccf4aa0e26ae871de3cdd758b49ab45ee7291619
    WORKDIR /buildkit
    COPY . .
    RUN python3 .agents/skills/buildkit-learn/scripts/validate_skills.py --repo-root /buildkit --output /tmp/skill-validation.json
