#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Запусти: sudo bash install.sh"
  exit 1
fi

APP_DIR="$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/opt/camwall"

apt-get update
apt-get install -y python3 python3-venv python3-pip ffmpeg ca-certificates

if ! id camwall >/dev/null 2>&1; then
  useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin camwall
fi

install -d -o camwall -g camwall -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/static"
install -o camwall -g camwall -m 0640 "$APP_DIR/app.py" "$INSTALL_DIR/app.py"
install -o camwall -g camwall -m 0640 "$APP_DIR/requirements.txt" "$INSTALL_DIR/requirements.txt"
install -o camwall -g camwall -m 0600 "$APP_DIR/playlist.json" "$INSTALL_DIR/playlist.json"
install -o camwall -g camwall -m 0640 "$APP_DIR/static/index.html" "$INSTALL_DIR/static/index.html"

python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

install -m 0644 "$APP_DIR/systemd/camwall.service" /etc/systemd/system/camwall.service
systemctl daemon-reload
systemctl enable camwall

echo
echo "Встановлення завершено."
echo "Перевір конфігурацію: $INSTALL_DIR/playlist.json"
echo "Запуск: sudo systemctl start camwall"
echo "Логи: sudo journalctl -u camwall -f"
echo "Web UI: http://IP_СЕРВЕРА:8080"
echo "Обмеж порт 8080 firewall-ом; автентифікація поки не налаштована."
