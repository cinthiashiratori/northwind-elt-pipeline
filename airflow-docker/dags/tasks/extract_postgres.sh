set -e

DS=$1

if [ -z "$DS" ]; then
    echo "Error: execution date not provided."
    echo "Usage: bash extract_postgres.sh <YYYY-MM-DD>"
    exit 1
fi

cd /meltano

rm -f /data/postgres/*.jsonl

meltano run --force tap-postgres target-jsonl--postgres

find /data/postgres -maxdepth 1 -name "*.jsonl" | while read f; do
    table=$(basename "$f" .jsonl | sed 's/^public-//')
    mkdir -p "/data/postgres/$table/$DS"
    rm -f "/data/postgres/$table/$DS/${table}.jsonl"
    mv "$f" "/data/postgres/$table/$DS/${table}.jsonl"
done
