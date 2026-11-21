set -e

DS=$1

if [ -z "$DS" ]; then
    echo "Error: execution date not provided."
    echo "Usage: bash extract_csv.sh <YYYY-MM-DD>"
    exit 1
fi

cd /meltano

rm -f /data/csv/order_details.jsonl

meltano run --force tap-csv target-jsonl--csv

mkdir -p "/data/csv/$DS"
mv /data/csv/order_details.jsonl "/data/csv/$DS/order_details.jsonl"
