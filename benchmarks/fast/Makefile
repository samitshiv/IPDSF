
clean:
	rm -f producer consumer

all: producer consumer

producer:
	go build pblh/v2/cmd/producer

consumer:
	go build pblh/v2/cmd/consumer

sync:
	sh ../sync.sh

run: all sync
	bash ../reset.sh 50
	./producer
	sh run_all.sh 50
	bash ../reset.sh 100
	./producer
	sh run_all.sh 100
	bash ../reset.sh 150
	./producer
	sh run_all.sh 150
