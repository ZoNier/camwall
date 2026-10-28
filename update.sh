#!/usr/bin/env bash
set -Eeuo pipefail

# Self-contained updater. It can be run from the installed application directory:
#   sudo bash /opt/camwall/update.sh

if [[ $EUID -ne 0 ]]; then
  echo "Запусти від root: sudo bash /opt/camwall/update.sh"
  exit 1
fi

REPO_URL="${CAMWALL_REPO_URL:-https://github.com/ZoNier/camwall.git}"
BRANCH="${CAMWALL_BRANCH:-main}"
INSTALL_DIR="${CAMWALL_INSTALL_DIR:-/opt/camwall}"
SERVICE="${CAMWALL_SERVICE:-camwall}"
COMMIT_FILE="$INSTALL_DIR/.camwall-commit"
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

if ! command -v git >/dev/null 2>&1; then
  echo "Помилка: git не встановлено. Встанови його через apt install git."
  exit 1
fi
if ! id camwall >/dev/null 2>&1; then
  echo "Помилка: системний користувач camwall не існує. Спочатку встанови CamWall через install.sh."
  exit 1
fi
if [[ ! -x "$INSTALL_DIR/venv/bin/pip" ]]; then
  echo "Помилка: не знайдено $INSTALL_DIR/venv/bin/pip. Спочатку встанови CamWall через install.sh."
  exit 1
fi

echo "Перевіряю останню версію $REPO_URL ($BRANCH)..."
REMOTE_COMMIT="$(git ls-remote "$REPO_URL" "refs/heads/$BRANCH" | awk 'NR==1 {print $1}')"
if [[ -z "$REMOTE_COMMIT" ]]; then
  echo "Помилка: не вдалося отримати коміт з GitHub. Перевір мережу та назву гілки."
  exit 1
fi

CURRENT_COMMIT=""
if [[ -f "$COMMIT_FILE" ]]; then
  CURRENT_COMMIT="$(cat "$COMMIT_FILE")"
fi
if [[ "$CURRENT_COMMIT" == "$REMOTE_COMMIT" ]]; then
  echo "CamWall вже оновлений ($REMOTE_COMMIT). Змін немає."
  exit 0
fi

echo "Завантажую код..."
git clone --quiet --depth 1 --branch "$BRANCH" "$REPO_URL" "$TMP_DIR/repo"
SOURCE="$TMP_DIR/repo"
NEW_COMMIT="$(git -C "$SOURCE" rev-parse HEAD)"

for file in app.py requirements.txt settings.example.json playlist.example.json update.sh static/index.html static/favicon.svg systemd/camwall.service; do
  if [[ ! -f "$SOURCE/$file" ]]; then
    echo "Помилка: у репозиторії відсутній файл $file"
    exit 1
  fi
done

echo "Оновлюю файли застосунку (камери та settings.json не перезаписуються)..."
install -d -o camwall -g camwall -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/static"

if [[ ! -f "$INSTALL_DIR/playlist.json" ]]; then
  install -o camwall -g camwall -m 0600 "$SOURCE/playlist.example.json" "$INSTALL_DIR/playlist.json"
fi
if [[ ! -f "$INSTALL_DIR/settings.json" ]]; then
  install -o camwall -g camwall -m 0600 "$SOURCE/settings.example.json" "$INSTALL_DIR/settings.json"
fi

install -o camwall -g camwall -m 0640 "$SOURCE/app.py" "$INSTALL_DIR/app.py"
install -o camwall -g camwall -m 0640 "$SOURCE/requirements.txt" "$INSTALL_DIR/requirements.txt"
install -o camwall -g camwall -m 0640 "$SOURCE/static/index.html" "$INSTALL_DIR/static/index.html"
install -o root -g root -m 0644 "$SOURCE/static/favicon.svg" "$INSTALL_DIR/static/favicon.svg"
install -o root -g root -m 0750 "$SOURCE/update.sh" "$INSTALL_DIR/update.sh"
install -o root -g root -m 0644 "$SOURCE/systemd/camwall.service" "/etc/systemd/system/camwall.service"

echo "Оновлюю залежності Python..."
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

systemctl daemon-reload
echo "Перезапускаю сервіс..."
systemctl restart "$SERVICE"
if ! systemctl is-active --quiet "$SERVICE"; then
  echo "Помилка: сервіс не запустився. Логи:"
  journalctl -u "$SERVICE" -n 100 --no-pager
  exit 1
fi

printf '%s\n' "$NEW_COMMIT" > "$COMMIT_FILE"
chmod 0644 "$COMMIT_FILE"
echo
echo "CamWall успішно оновлено до $NEW_COMMIT"
echo "Файли конфігурації збережено:"
echo "  Камери:       $INSTALL_DIR/playlist.json"
echo "  Налаштування: $INSTALL_DIR/settings.json"
