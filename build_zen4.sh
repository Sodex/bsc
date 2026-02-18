#!/usr/bin/env bash
set -euo pipefail

export GOAMD64=v4
export CGO_ENABLED=1
export CGO_CFLAGS="-O3 -march=znver4 -mtune=znver4 -fno-plt"
export CGO_CXXFLAGS="-O3 -march=znver4 -mtune=znver4 -fno-plt"
export CGO_LDFLAGS="-Wl,-O2 -Wl,-z,noexecstack"

mkdir -p build/bin

go build \
  -trimpath \
  -v \
  -tags "urfave_cli_no_docs,ckzg" \
  -ldflags="
    --buildid=none
    -s -w
    -X github.com/ethereum/go-ethereum/internal/version.gitCommit=v1.6.6
    -X github.com/ethereum/go-ethereum/internal/version.gitDate=20260114
    -extldflags '-Wl,-z,stack-size=0x800000,--build-id=none,--strip-all'
  " \
  -o build/bin/geth \
  ./cmd/geth
