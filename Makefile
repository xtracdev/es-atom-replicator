VERSION_BUILD ?= 1
containerbin:
	go get github.com/gucumber/gucumber/cmd/gucumber
	export PKG_CONFIG_PATH=$GOPATH/src/github.com/xtracdev/es-atom-replicator/pkgconfig/
	go test
	gucumber
	GOOS=linux GOARCH=amd64 go build -ldflags "-s -X github.com/xtracdev/es-atom-replicator/health.Version=$(VERSION_BUILD)" -a -installsuffix cgo -o replicator ./cmd/
	cp replicator /artifacts
