#!/bin/bash
KAFKA_TEST_ENV="$HOME/kafka/test/env"
echo "hi, i'm a worker on '`hostname`'! activating environment '$KAFKA_TEST_ENV"
. ~/.bashrc
conda activate $KAFKA_TEST_ENV


echo "hi, i'm a worker on '`hostname`'! starting main script"
set -ex
"$KAFKA_TEST_ENV/bin/python" main.py "$@"
