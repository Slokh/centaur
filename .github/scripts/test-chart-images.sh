#!/usr/bin/env bash
set -euo pipefail

rendered="$(helm template image-matrix contrib/chart \
  --set discordbot.enabled=true \
  --set discordbot.eventSink.enabled=true \
  --set discordbot.eventSink.url=http://event-sink \
  --set discordbot.image.repository=example/discord \
  --set discordbot.image.digest=sha256:discord \
  --set sandbox.image.repository=example/sandbox \
  --set sandbox.image.tag=base \
  --set sandbox.image.digest=sha256:sandbox \
  --set repoCache.enabled=true \
  --set repoCache.repositories[0]=example/repo \
  --set repoCache.image.repository=example/cache \
  --set repoCache.image.tag=custom \
  --set toolServer.enabled=true \
  --set toolServer.repo=example/tools \
  --set toolServer.runnerImage.repository=example/runner \
  --set toolServer.runnerImage.tag=custom)"

grep -Fq 'image: "example/discord@sha256:discord"' <<<"${rendered}"
grep -Fq 'image: "example/cache:custom"' <<<"${rendered}"
grep -Fq 'value: "example/runner:custom"' <<<"${rendered}"

inherited="$(helm template image-inheritance contrib/chart \
  --set sandbox.image.repository=example/sandbox \
  --set sandbox.image.tag=base \
  --set sandbox.image.digest=sha256:sandbox \
  --set repoCache.enabled=true \
  --set repoCache.repositories[0]=example/repo \
  --set toolServer.enabled=true \
  --set toolServer.repo=example/tools)"

grep -Fq 'image: "example/sandbox@sha256:sandbox"' <<<"${inherited}"
grep -Fq 'value: "example/sandbox@sha256:sandbox"' <<<"${inherited}"
