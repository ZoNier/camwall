#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Запусти: sudo bash install.sh"
  exit 1
fi

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/opt/camwall"

apt-get update
apt-get install -y git python3 python3-venv python3-pip ffmpeg ca-certificates

if ! id camwall >/dev/null 2>&1; then
  useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin camwall
fi

install -d -o camwall -g camwall -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/static"
install -o camwall -g camwall -m 0640 "$APP_DIR/app.py" "$INSTALL_DIR/app.py"
install -o camwall -g camwall -m 0640 "$APP_DIR/requirements.txt" "$INSTALL_DIR/requirements.txt"
if [[ ! -f "$INSTALL_DIR/playlist.json" ]]; then
  if [[ ! -f "$INSTALL_DIR/playlist.json" ]]; then
  install -o camwall -g camwall -m 0600 "$APP_DIR/playlist.example.json" "$INSTALL_DIR/playlist.json"
fi
fi
if [[ ! -f "$INSTALL_DIR/settings.json" ]]; then
  install -o camwall -g camwall -m 0600 "$APP_DIR/settings.example.json" "$INSTALL_DIR/settings.json"
fi
install -o camwall -g camwall -m 0640 "$APP_DIR/static/index.html" "$INSTALL_DIR/static/index.html"
install -o root -g root -m 0644 "$APP_DIR/static/favicon.svg" "$INSTALL_DIR/static/favicon.svg"
install -o root -g root -m 0750 "$APP_DIR/update.sh" "$INSTALL_DIR/update.sh"

python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

install -m 0644 "$APP_DIR/systemd/camwall.service" /etc/systemd/system/camwall.service
systemctl daemon-reload
systemctl enable camwall

echo
echo "Встановлення завершено."
echo "Камери: $INSTALL_DIR/playlist.json"
echo "Налаштування UI: $INSTALL_DIR/settings.json"
echo "Запуск: sudo systemctl start camwall"
echo "Логи: sudo journalctl -u camwall -f"
echo "Web UI: http://IP_СЕРВЕРА:8080"
echo "Обмеж порт 8080 firewall-ом; автентифікація поки не налаштована."
