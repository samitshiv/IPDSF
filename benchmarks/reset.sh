. "$(dirname "$0")/run_functions.sh"

export PARTITIONS_COUNT=$1

export node=localhost

until recreate_kafka_topic; do
	echo "failed to recreate topic. retrying..."
done
