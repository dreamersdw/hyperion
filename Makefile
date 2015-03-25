env=GOPATH=$(shell pwd)/build
.PHONY: build
test:
	@$(env) go test ./build/src/github.com/dreamersdw/hyperion/...
bench:
	@$(env) go test ./build/src/github.com/dreamersdw/hyperion/... -bench .
build:
	@$(env) go build github.com/dreamersdw/hyperion
run: build
	./hyperion
prepare:
	mkdir -p build/{bin,pkg,src}
	mkdir -p build/src/github.com/dreamersdw
	(cd build/src/github.com/dreamersdw; ln -s ../../../../src hyperion)
	$(env) go get ./build/src/github.com/dreamersdw/hyperion/...
	$(env) go get github.com/stretchr/testify/assert
release:
	@$(env) GOOS=linux GOARCH=amd64 go build github.com/dreamersdw/hyperion
	@goupx ./hyperion
	webshare upload ./hyperion
clean:
	rm -rf build
