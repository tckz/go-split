.PHONY: dist test clean all

export GO111MODULE=on

ifeq ($(GO_CMD),)
GO_CMD:=go
endif

TARGETS=\
	go-split

SRCS_OTHER := $(shell find . \
	-type d -name cmd -prune -o \
	-type f -name "*.go" -print) go.mod

DIR_TOOL := $(realpath $(shell dirname $(lastword $(MAKEFILE_LIST))))/bin

TOOL_STRINGER = $(DIR_TOOL)/stringer

TOOLS = \
	$(TOOL_STRINGER)

all: $(TARGETS)
	@echo "$@ done."

clean:
	/bin/rm -f $(TARGETS)
	@echo "$@ done."

test:
	$(GO_CMD) test -coverprofile=reports/coverage.out -json > reports/test.json
	@echo "$@ done."

.PHONY: tools
tools: $(TOOLS)
	@echo "$@ done." 1>&2

$(TOOL_STRINGER): tools/*
	cd tools && GOBIN=$(DIR_TOOL) CGO_ENABLED=0 $(GO_CMD) install golang.org/x/tools/cmd/stringer

.PHONY: gen
TMP_PATH := $(DIR_TOOL):$(PATH)
gen: export PATH=$(TMP_PATH)
gen: tools
	$(GO_CMD) generate `$(GO_CMD) list ./...`
	@echo "$@ done." 1>&2

go-split: cmd/go-split/* $(SRCS_OTHER)
	$(GO_CMD) build -o $@ -ldflags "-X main.version=`git describe --tags --always`" $<
	@echo "$@ done."


