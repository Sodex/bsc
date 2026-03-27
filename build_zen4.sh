#!/usr/bin/env bash
set -euo pipefail

# 1. Автоматический захват метаданных из Git
# Берем имя последнего тега (например, v1.7.2) или короткий хэш
GIT_COMMIT=$(git describe --tags --abbrev=0 2>/dev/null || git rev-parse --short HEAD)
# Берем дату именно этого тега/коммита в формате ГГГГММДД
GIT_DATE=$(git show -s --format=%cd --date=format:%Y%m%d "$GIT_COMMIT")

echo "Building BSC-Geth: $GIT_COMMIT ($GIT_DATE) for Zen 4"

# 2. Настройки окружения для AMD Ryzen 9 7950X
export GOAMD64=v4
export CGO_ENABLED=1
export CGO_CFLAGS="-O3 -march=znver4 -mtune=znver4 -fno-plt"
export CGO_CXXFLAGS="-O3 -march=znver4 -mtune=znver4 -fno-plt"
export CGO_LDFLAGS="-Wl,-O2 -Wl,-z,noexecstack"

mkdir -p build/bin

# 3. Сборка
go build \
  -trimpath \
  -v \
  -tags "urfave_cli_no_docs,ckzg" \
  -ldflags="
    --buildid=none
    -s -w
    -X github.com/ethereum/go-ethereum/internal/version.gitCommit=${GIT_COMMIT}
    -X github.com/ethereum/go-ethereum/internal/version.gitDate=${GIT_DATE}
    -extldflags '-Wl,-z,stack-size=0x800000,--build-id=none,--strip-all'
  " \
  -o build/bin/geth \
  ./cmd/geth

echo "Build complete: ./build/bin/geth"
