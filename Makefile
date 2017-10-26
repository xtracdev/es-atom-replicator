containerbin:
	export PKG_CONFIG_PATH=$GOPATH/src/github.com/xtracdev/es-atom-replicator/pkgconfig/
	go test
	gucumber
	GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o replicator ./cmd/
	cp replicator /artifacts
