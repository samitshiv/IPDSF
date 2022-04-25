awk '/RESULTS/{printf "%s: %d consumers consumed %010f files/second\n", FILENAME, $4, $6 / $9}' consumer.log.*partitions
