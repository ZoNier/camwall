#!/usr/bin/env bash
set -Eeuo pipefail

# Run this script as the normal user who owns the Git checkout:
#   bash update.sh
# sudo is used only for deployment operations.

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${CAMWALL_INSTALL_DIR:-/opt/camwall}"
SERVICE="${CAMWALL_SERVICE:-camwall}"

if [[ $EUID -eq 0 ]]; then
  echo "Запусти без sudo від користувача, який має Git-клон:"
  echo "  bash update.sh"
  exit 1
fi

command -v git >/dev/null || { echo "Помилка: git не встановлено."; exit 1; }
command -v sudo >/dev/null || { echo "Помилка: sudo не встановлено."; exit 1; }
git -C "$REPO_DIR" rev-parse --is-inside-work-tree >/dev/null

if [[ -n "$(git -C "$REPO_DIR" status --porcelain -- . ':(exclude)playlist.json')" ]]; then
  echo "Помилка: у Git-клоні є локальні зміни."
  echo "Збережи їх або прибери перед оновленням, щоб update не затер твою роботу."
  exit 1
fi

echo "Отримую зміни з Git..."
git -C "$REPO_DIR" pull --ff-only

for file in app.py requirements.txt static/index.html static/favicon.svg systemd/camwall.service; do
  if [[ ! -f "$REPO_DIR/$file" ]]; then
    echo "Помилка: у репозиторії немає $file"
    exit 1
  fi
done

echo "Встановлюю файли в $INSTALL_DIR (playlist.json не змінюється)..."
sudo install -d -o camwall -g camwall -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/static"
sudo install -o camwall -g camwall -m 0640 "$REPO_DIR/app.py" "$INSTALL_DIR/app.py"
sudo install -o camwall -g camwall -m 0640 "$REPO_DIR/requirements.txt" "$INSTALL_DIR/requirements.txt"
sudo install -o camwall -g camwall -m 0640 "$REPO_DIR/static/index.html" "$INSTALL_DIR/static/index.html"
sudo install -o root -g root -m 0644 "$REPO_DIR/static/favicon.svg" "$INSTALL_DIR/static/favicon.svg"
sudo install -o root -g root -m 0644 "$REPO_DIR/systemd/camwall.service" "/etc/systemd/system/camwall.service"

if [[ -x "$INSTALL_DIR/venv/bin/pip" ]]; then
  sudo "$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"
else
  echo "Помилка: не знайдено $INSTALL_DIR/venv/bin/pip."
  echo "Схоже, CamWall встановлено не через install.sh."
  exit 1
fi

sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE"
sudo systemctl --no-pager --full status "$SERVICE" || {
  echo "Сервіс не запустився. Перевір логи:"
  echo "  sudo journalctl -u $SERVICE -n 100 --no-pager"
  exit 1
}

echo
echo "CamWall оновлено."
echo "Конфігурацію камер не змінював: $INSTALL_DIR/playlist.json"
