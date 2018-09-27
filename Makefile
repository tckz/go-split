.PHONY: dist test clean all
.SUFFIXES: .proto .pb.go .go

TARGETS=\
	go-split

SRCS_OTHER = \
	$(wildcard */*.go) \
	$(wildcard *.go)

all: $(TARGETS)
	@echo "$@ done."

clean:
	/bin/rm -f $(TARGETS)
	@echo "$@ done."

test:
	go test -v -coverprofile=reports/coverage.out -json > reports/test.json
	@echo "$@ done."

go-split: cmd/go-split/main.go $(SRCS_OTHER)
	go build -o $@ -ldflags "-X main.version=`git describe --tags --always`" $<
	@echo "$@ done."


