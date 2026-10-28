#!/usr/bin/env bash
set -Eeuo pipefail

INSTALL_DIR="${CAMWALL_INSTALL_DIR:-/opt/camwall}"
SERVICE="${CAMWALL_SERVICE:-camwall}"
PURGE=0

usage() {
  cat <<'EOF'
CamWall uninstall

Usage:
  sudo bash uninstall.sh          Remove application, keep camera/settings JSON
  sudo bash uninstall.sh --purge  Remove application AND configuration files

Options:
  --purge    Permanently delete playlist.json and settings.json, and remove
             the camwall system user.
  -h, --help Show this help.
EOF
}

while (($#)); do
  case "$1" in
    --purge) PURGE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Невідомий параметр: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ $EUID -ne 0 ]]; then
  echo "Запусти від root: sudo bash uninstall.sh" >&2
  exit 1
fi

echo "Зупиняю та вимикаю службу $SERVICE..."
systemctl stop "$SERVICE" 2>/dev/null || true
systemctl disable "$SERVICE" 2>/dev/null || true
rm -f "/etc/systemd/system/$SERVICE.service"
systemctl daemon-reload
systemctl reset-failed "$SERVICE" 2>/dev/null || true

if (( PURGE )); then
  echo "Видаляю застосунок і конфігурацію..."
  rm -rf -- "$INSTALL_DIR"
  if id camwall >/dev/null 2>&1; then
    userdel camwall 2>/dev/null || true
  fi
  echo "CamWall і його конфігурацію повністю видалено."
else
  echo "Видаляю код і віртуальне середовище, залишаючи конфігурацію..."
  if [[ -d "$INSTALL_DIR" ]]; then
    find "$INSTALL_DIR" -mindepth 1 -maxdepth 1 \
      ! -name playlist.json \
      ! -name settings.json \
      -exec rm -rf -- {} +
  fi
  echo "CamWall видалено. Конфігурація збережена:"
  echo "  $INSTALL_DIR/playlist.json"
  echo "  $INSTALL_DIR/settings.json"
  echo "Для повторного встановлення запусти install.sh з клонованого репозиторію."
  echo "Для повного видалення конфігурації: sudo bash uninstall.sh --purge"
fi
