#!/usr/bin/env bash


## Node Name/Number Management

count_nodes() {
	# sets environment variables useful for horovodrun

	NODE_COUNT=${#NODE_NAMES[@]}
	WORKER_COUNT=$((NODE_COUNT*CONSUMERS_PER_NODE))

	# build input line for -H
	NODES_HOSTS=""
	for node in ${NODE_NAMES[@]}; do
		# if a node is localhost, use the name 'localhost' to prevent ssh problems
		if [[ "$node" == "${HOSTNAME%%.*}" ]]; then
			if [[ "$node" == "${NODE_NAMES[0]}" ]]; then
				NODE_NAMES[0]="localhost"
			fi
			node="localhost"
		fi

		let WORKERS_${node}=$CONSUMERS_PER_NODE

		## add an extra worker on node 0 for the producer
		# don't. production is handled by produce.py now
		#if [[ "$node" == "${NODE_NAMES[0]}" ]]; then
		#	let WORKERS_${node}+=1
		#fi

		NODES_HOSTS="$NODES_HOSTS,${node}:$((WORKERS_${node}))"
	done
	# trim the comma at the beginning
	NODES_HOSTS="$(echo "$NODES_HOSTS" | sed 's/^,//')"
}


## Logging

prepare_logdir() {
	# removes $LOGDIR on all hosts and creates it on localhost
	# this gets synced to the other hosts in sync_files

	#LOGDIR="oldresults/$(date +"%Y-%m-%d_%H:%M:%S")"
	echo "preparing log location '$LOGDIR'"
	if [ "$DELETE_LOGDIR" = "true" ]; then
		rm -rf "$LOGDIR"
	fi
	mkdir -p "$LOGDIR"
	#ssh claude2 'rm "$HOME/kafka/test/benchmarks/results" || rm -vrf "$HOME/kafka/test/benchmarks/results"'
	for node in ${NODE_NAMES[@]}; do
		if [[ "$node" == "${HOSTNAME%%.*}" || "$node" == "localhost" ]]; then
			# skip localhost
			continue
		fi
		if [ "$DELETE_LOGDIR" = "true" ]; then
			ssh $node "rm -vrf $HOME/kafka/test/benchmarks/$LOGDIR"
		fi
		ssh $node 'rm "$HOME/kafka/test/benchmarks/results" || rm -vrf "$HOME/kafka/test/benchmarks/results"'
	done
	rm results || rm -vrf results
	ln -sf "$LOGDIR" results
	#rm -f results.*
}


## Process Management

sigint_main() {
	# send SIGINT (ctrl+c) to any processes including 'python main.py' from user 'samit1'
	ps auxww | awk '(!/ awk /&&/^samit/)&&/python main.py/{print $0; system("kill -2 " $2)}'
	for node in ${NODE_NAMES[@]}; do
		if [[ "$node" == "${HOSTNAME%%.*}" || "$node" == "localhost" ]]; then
			# skip localhost
			continue
		fi
		ssh $node 'ps auxww | awk '\''(!/ awk /&&/^samit/)&&/python main.py/{print $0; system("kill -2 " $2)}'\'
	done
}

sigint_producer() {
	# send SIGINT (ctrl+c) to any processes including 'producer/producer' from user 'samit1'
	ps auxww | awk '(!/ awk /&&/^samit/)&&/producer\/producer$/{print $0; system("kill -2 " $2)}'
	for node in ${NODE_NAMES[@]}; do
		if [[ "$node" == "${HOSTNAME%%.*}" || "$node" == "localhost" ]]; then
			# skip localhost
			continue
		fi
		ssh $node 'ps auxww | awk '\''(!/ awk /&&/^samit/)&&/producer\/producer$/{print $0; system("kill -2 " $2)}'\'
	done
}

sigint_running() {
	sigint_main;
	#sigint_producer;
}

kill_old_instances() {
	# old processes make kafka behave wrong
	echo "waiting to kill old instances"
	# sigint_running outputs a list of processes it is still waiting to kill
	# this while loop runs until the list is empty
	while sigint_running | grep -q .; do sleep 0.2; done
}


## Kafka Topic Workaround

recreate_kafka_topic() {
	# deleting the kafka topic deletes all old messages stuck in it
	# if messages have changed format, old ones can break consumers

	echo "recreating topics"
	if [[ "$node" == "${HOSTNAME%%.*}" || "$node" == "localhost" ]]; then
		for topic in "VATech-Blacksburg" "sync_VATech-Blacksburg"; do
			echo " deleting $topic..."
			sh "$HOME/kafka/kafka_2.12-2.4.1/bin/kafka-topics.sh" \
				--zookeeper localhost:2182 \
				--delete --topic "$topic" || echo 'topic already deleted'
		done

		echo "sleeping ten seconds to allow time for topic to delete"
		sleep 10
		python ~/kafka/test/benchmarks/reset.py "VATech-Blacksburg" $PARTITIONS_COUNT #$((WORKER_COUNT-1))
		r=$?
	else
		#python ~/kafka/test/benchmarks/checkreset.py "VATech-Blacksburg" $PARTITIONS_COUNT #$((WORKER_COUNT-1))
		sleep 12
	fi
	echo "sleeping five seconds to allow time for topic to propagate"
	sleep 5
	return $r
}


## Code Synchronization

sync_files() {
	# every worker needs a copy of the .py files
	# this also creates $LOGDIR on the other nodes
	echo "synchronizing files"
	for node in ${NODE_NAMES[@]}; do
		if [[ "$node" == "${HOSTNAME%%.*}" || "$node" == "localhost" ]]; then
			# skip localhost
			continue
		fi

		rsync -Prluv --exclude "benchmarks/horovod_logs" --exclude "data/umbc" --exclude "data/vatech" "$HOME/kafka/test/" "$node":"kafka/test/"
	done
}


## Running

run_producer() {
	echo "producing files to topic in background"
	/home/samit1/kafka/test/benchmarks/producer/producer
}

run_horovod() {
	# this script uses horovodrun to distribute the work, but the horovod
	# library has support for other runners as well
	echo "executing horovodrun"
	export OMPI_MCA_plm_rsh_no_tree_spawn=1
	export OMPI_MCA_pml=ob1
	export OMPI_MCA_btl_tcp_if_include=enp4s0
	"$HOME/kafka/test/env/bin/horovodrun" \
		-np $WORKER_COUNT \
		--mpi-args "--prefix $HOME/kafka/test/env" \
		-H "$NODES_HOSTS" \
		--mpi \
		\
	sh "$HOME/kafka/test/benchmarks/run_backend.sh" "$LOGDIR"
}
