#!/usr/bin/env bash
# 一键交叉编译并打包 Linux amd64 服务端（nps）
# 用法：./build.linux_amd64.server.sh
# 可选环境变量：OUTPUT_DIR=dist ARCHIVE_NAME=linux_amd64_server.tar.gz
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

OUTPUT_DIR="${OUTPUT_DIR:-dist}"
ARCHIVE_NAME="${ARCHIVE_NAME:-linux_amd64_server.tar.gz}"

command -v go >/dev/null 2>&1 || {
  echo "错误：未找到 go，请先安装 Go 1.24+ 并加入 PATH" >&2
  exit 1
}

for required in conf web/views web/static cmd/nps/nps.go; do
  if [ ! -e "$required" ]; then
    echo "错误：当前目录缺少 $required，请在项目根目录执行本脚本" >&2
    exit 1
  fi
done

echo "==> Go 版本: $(go version)"
echo "==> 编译 linux/amd64 服务端 ..."

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags "-s -w -extldflags -static" \
  -o "$STAGE/nps" ./cmd/nps/nps.go

echo "==> 准备发布目录 ..."
cp -R conf "$STAGE/conf"
mkdir -p "$STAGE/web"
cp -R web/views "$STAGE/web/"
cp -R web/static "$STAGE/web/"

mkdir -p "$OUTPUT_DIR"
ARCHIVE="$OUTPUT_DIR/$ARCHIVE_NAME"

echo "==> 打包 $ARCHIVE ..."
tar -czf "$ARCHIVE" -C "$STAGE" nps conf web

echo "完成：$ARCHIVE"
echo "内含：nps, conf/, web/views/, web/static/"
