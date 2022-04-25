#!/bin/sh

NODES=2

mkdir -p logs

./producer -n 2

if [ $NODES = 1 ]; then
	## one node
	./consumer -n 1 2>&1 | tee logs/consumer.log.node.$1partitions
else
	## two nodes
	srun --mpi=openmpi -n $NODES ./consumer -n 1 2>&1 | tee logs/consumer.log.${NODES}nodes.$1partitions
fi

for i in 2 4 8 16 32 64 128; do
	./producer -n $((i*NODES))

	if [ $NODES = 1]; then
		## one node
		./consumer -n $i 2>&1 | tee -a logs/consumer.log.1node.$1partitions
	else
		## two nodes
		srun --mpi=openmpi -n $NODES ./consumer -n $i 2>&1 | tee -a logs/consumer.log.${NODES}nodes.$1partitions
	fi
done
