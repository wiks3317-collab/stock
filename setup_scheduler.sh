#!/usr/bin/env bash
# 在 Cloud Shell 執行：bash setup_scheduler.sh
# 建立 2 個 Cloud Scheduler 工作（台股、美股），盤中每 5 分鐘呼叫 Cloud Run 的 /prefetch。
set -euo pipefail

REGION="asia-east1"
SERVICE_URL="https://stock-proxy-885754386445.asia-east1.run.app"   # 你的 Cloud Run 網址（結尾不要加 /）
TOKEN="${PREFETCH_TOKEN:?請先設定：export PREFETCH_TOKEN=你自訂的一串亂碼（要和部署時的 PREFETCH_TOKEN 相同）}"

gcloud services enable cloudscheduler.googleapis.com

make_job () {  # 名稱  市場  排程  時區
  local NAME="$1" M="$2" CRON="$3" TZ="$4"
  local ARGS=(--location="$REGION" --schedule="$CRON" --time-zone="$TZ"
              --uri="$SERVICE_URL/prefetch?m=$M" --http-method=GET
              --headers="X-Prefetch-Token=$TOKEN"
              --attempt-deadline=300s --max-retry-attempts=0)
  if gcloud scheduler jobs describe "$NAME" --location="$REGION" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "$NAME" "${ARGS[@]}"
  else
    gcloud scheduler jobs create http "$NAME" "${ARGS[@]}"
  fi
}

# 程式內會再檢查是否開盤（含前後 5 分鐘），休市時只回傳 skipped，不會去抓 Yahoo
make_job stock-prefetch-tw TW "*/5 8-13 * * 1-5" "Asia/Taipei"
make_job stock-prefetch-us US "*/5 9-16 * * 1-5" "America/New_York"

echo "完成。手動測試：gcloud scheduler jobs run stock-prefetch-tw --location=$REGION"
