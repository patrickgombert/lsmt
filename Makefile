GO_CMD?=go
FMT_CMD?=gofmt
SOURCE_FILES?=$$(find . -name '*.go')
TEST_SOURCES?=$$($(GO_CMD) list ./...)

fmt:
	@$(FMT_CMD) -w $(SOURCE_FILES)

test:
	@$(GO_CMD) test $(TEST_SOURCES)

.PHONY: fmt test
