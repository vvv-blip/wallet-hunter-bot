#!/usr/bin/env bash
# Run the bot in the background on macOS using launchd.
# Uses your Mac as the "host" — free, instant, but bot dies when Mac sleeps/off.
set -euo pipefail

: "${TELEGRAM_BOT_TOKEN:?Set TELEGRAM_BOT_TOKEN in your shell first}"
: "${ETHERSCAN_API_KEY:?Set ETHERSCAN_API_KEY in your shell first}"

BOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLIST="$HOME/Library/LaunchAgents/com.wallet.hunter.bot.plist"
LOG="$BOT_DIR/bot.log"

# Make sure venv exists
if [ ! -x "$BOT_DIR/venv/bin/python" ]; then
  echo ">> creating venv…"
  python3 -m venv "$BOT_DIR/venv"
  "$BOT_DIR/venv/bin/pip" install -q -r "$BOT_DIR/requirements.txt"
fi

mkdir -p "$HOME/Library/LaunchAgents"
cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.wallet.hunter.bot</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BOT_DIR/venv/bin/python</string>
    <string>$BOT_DIR/bot.py</string>
  </array>
  <key>WorkingDirectory</key><string>$BOT_DIR</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>TELEGRAM_BOT_TOKEN</key><string>$TELEGRAM_BOT_TOKEN</string>
    <key>ETHERSCAN_API_KEY</key><string>$ETHERSCAN_API_KEY</string>
    <key>BOT_CACHE_DIR</key><string>$BOT_DIR/cache</string>
    <key>PATH</key><string>/usr/local/bin:/usr/bin:/bin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict>
</plist>
EOF

launchctl unload "$PLIST" 2>/dev/null || true
launchctl load "$PLIST"

echo "✅ Bot started in background."
echo "   Logs:  tail -f $LOG"
echo "   Stop:  launchctl unload $PLIST"
echo "   Start: launchctl load $PLIST"
