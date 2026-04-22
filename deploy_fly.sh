#!/usr/bin/env bash
# One-shot Fly.io deploy. Run from /tmp/wallet_research/bot.
# Assumes you've already:
#   - installed flyctl (curl -L https://fly.io/install.sh | sh)
#   - run `flyctl auth login` (or signup)
#   - exported TELEGRAM_BOT_TOKEN and ETHERSCAN_API_KEY in this shell
set -euo pipefail

: "${TELEGRAM_BOT_TOKEN:?Set TELEGRAM_BOT_TOKEN in your shell first}"
: "${ETHERSCAN_API_KEY:?Set ETHERSCAN_API_KEY in your shell first}"

APP_NAME="${FLY_APP_NAME:-wallet-hunter-bot-$(openssl rand -hex 3)}"
REGION="${FLY_REGION:-iad}"

echo ">> Creating app '$APP_NAME' in region '$REGION'…"
flyctl apps create "$APP_NAME" --org personal || true

# Replace placeholder app name in fly.toml on the fly
sed -i.bak "s/^app = .*/app = \"$APP_NAME\"/" fly.toml
sed -i.bak "s/^primary_region = .*/primary_region = \"$REGION\"/" fly.toml
rm -f fly.toml.bak

echo ">> Setting secrets (values are not echoed)…"
flyctl secrets set --app "$APP_NAME" \
  TELEGRAM_BOT_TOKEN="$TELEGRAM_BOT_TOKEN" \
  ETHERSCAN_API_KEY="$ETHERSCAN_API_KEY" \
  --stage

echo ">> Creating 1GB cache volume (free)…"
flyctl volumes create bot_cache --app "$APP_NAME" --region "$REGION" --size 1 --yes || true

echo ">> Deploying…"
flyctl deploy --app "$APP_NAME" --ha=false

echo ""
echo "✅ Bot deployed. Logs:  flyctl logs --app $APP_NAME"
echo "   Open Telegram, /start the bot."
