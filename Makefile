.PHONY: dist test clean all

ifeq ($(GO_CMD),)
GO_CMD:=go
endif

VERSION := $(shell git describe --always)
GO_BUILD := CGO_ENABLED=0 $(GO_CMD) build -ldflags "-X main.version=$(VERSION)"

TARGETS=\
	go-split

SRCS_OTHER := $(shell find . \
	-type d -name cmd -prune -o \
	-type f -name "*.go" -print) go.mod

DIR_BIN := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))/bin

TOOL_STRINGER = $(DIR_BIN)/stringer
TOOL_STATICCHECK = $(DIR_BIN)/staticcheck

TOOLS = \
	$(TOOL_STRINGER) \
	$(TOOL_STATICCHECK)

all: $(TARGETS)
	@echo "$@ done." 1>&2

clean:
	/bin/rm -f $(TARGETS)
	@echo "$@ done." 1>&2

test:
	$(GO_CMD) test -covermode atomic -cover `$(GO_CMD) list ./... | egrep -v '/cmd/'`
	@echo "$@ done." 1>&2

.PHONY: test-detail
test-detail:
	$(GO_CMD) test -coverprofile=reports/coverage.out -json `$(GO_CMD) list ./... | egrep -v '/cmd/'` > reports/test.json
	@echo "$@ done." 1>&2

.PHONY: sonar
sonar: test-detail
	./gradlew sonar
	@echo "$@ done." 1>&2

.PHONY: tools
tools: $(TOOLS)
	@echo "$@ done." 1>&2

.PHONY: lint
lint: $(TOOL_STATICCHECK)
	$(TOOL_STATICCHECK) ./...

$(TOOL_STATICCHECK): export GOBIN=$(DIR_BIN)
$(TOOL_STATICCHECK): $(TOOLS_DEP)
	@echo "### `basename $@` install destination=$(GOBIN)" 1>&2
	CGO_ENABLED=0 $(GO_CMD) install honnef.co/go/tools/cmd/staticcheck@v0.4.7

$(TOOL_STRINGER): export GOBIN=$(DIR_BIN)
$(TOOL_STRINGER): Makefile
	@echo "### `basename $@` install destination=$(GOBIN)" 1>&2
	CGO_ENABLED=0 $(GO_CMD) install golang.org/x/tools/cmd/stringer@v0.21.0


.PHONY: gen
TMP_PATH := $(DIR_BIN):$(PATH)
gen: export PATH=$(TMP_PATH)
gen: tools
	$(GO_CMD) generate ./...
	@echo "$@ done." 1>&2

go-split: cmd/go-split/* $(SRCS_OTHER)
	$(GO_BUILD) -o $@ ./cmd/go-split/


