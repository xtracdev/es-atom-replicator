image:
	docker run --rm -v $(CURDIR)/../:/go/src/github.com/xtracdev/es-atom-replicator -w /go/src/github.com/xtracdev/es-atom-replicator xtracdev/goora bash -c "cd cmd; make -f Makefile.docker"
	docker build -t xtracdev/esatomreplicator .


push:
	docker push xtracdev/esatomreplicator

clean:
	rm -f esatomreplicator
