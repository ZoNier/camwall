#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Запусти: sudo bash install.sh" >&2
  exit 1
fi

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${CAMWALL_INSTALL_DIR:-/opt/camwall}"
SERVICE="${CAMWALL_SERVICE:-camwall}"
APP_USER="camwall"

for file in app.py requirements.txt playlist.example.json settings.example.json static/index.html static/favicon.svg systemd/camwall.service update.sh uninstall.sh; do
  if [[ ! -f "$APP_DIR/$file" ]]; then
    echo "Помилка: не знайдено $APP_DIR/$file" >&2
    exit 1
  fi
done

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates ffmpeg git python3 python3-pip python3-venv

if ! id "$APP_USER" >/dev/null 2>&1; then
  useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin "$APP_USER"
fi

install -d -o "$APP_USER" -g "$APP_USER" -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/static"

# Preserve existing camera and UI configuration on reinstall.
if [[ ! -f "$INSTALL_DIR/playlist.json" ]]; then
  install -o "$APP_USER" -g "$APP_USER" -m 0600 "$APP_DIR/playlist.example.json" "$INSTALL_DIR/playlist.json"
fi
if [[ ! -f "$INSTALL_DIR/settings.json" ]]; then
  install -o "$APP_USER" -g "$APP_USER" -m 0600 "$APP_DIR/settings.example.json" "$INSTALL_DIR/settings.json"
fi

install -o "$APP_USER" -g "$APP_USER" -m 0640 "$APP_DIR/app.py" "$INSTALL_DIR/app.py"
install -o "$APP_USER" -g "$APP_USER" -m 0640 "$APP_DIR/requirements.txt" "$INSTALL_DIR/requirements.txt"
install -o "$APP_USER" -g "$APP_USER" -m 0640 "$APP_DIR/static/index.html" "$INSTALL_DIR/static/index.html"
install -o root -g root -m 0644 "$APP_DIR/static/favicon.svg" "$INSTALL_DIR/static/favicon.svg"
install -o root -g root -m 0750 "$APP_DIR/update.sh" "$INSTALL_DIR/update.sh"
install -o root -g root -m 0750 "$APP_DIR/uninstall.sh" "$INSTALL_DIR/uninstall.sh"

python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

install -o root -g root -m 0644 "$APP_DIR/systemd/camwall.service" "/etc/systemd/system/$SERVICE.service"
systemctl daemon-reload
systemctl enable "$SERVICE"
systemctl restart "$SERVICE"

if ! systemctl is-active --quiet "$SERVICE"; then
  echo "Помилка: CamWall не запустився. Останні логи:" >&2
  journalctl -u "$SERVICE" -n 80 --no-pager >&2 || true
  exit 1
fi

echo
echo "CamWall успішно встановлено."
echo "Камери:          $INSTALL_DIR/playlist.json"
echo "Налаштування UI: $INSTALL_DIR/settings.json"
echo "Стан:            sudo systemctl status $SERVICE"
echo "Логи:            sudo journalctl -u $SERVICE -f"
echo "Web UI:          http://IP_СЕРВЕРА:8080"
echo "Увага: автентифікація не налаштована; обмеж доступ до порту 8080."
