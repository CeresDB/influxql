SHELL = /bin/bash

DIR=$(shell pwd)

ALLOWED_CLIPPY_LINTS=-A clippy::uninlined_format_args -A clippy::inconsistent_digit_grouping -A dead-code

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features --workspace -- -D warnings $(ALLOWED_CLIPPY_LINTS)

fmt:
	cd $(DIR); cargo fmt -- --check

test: 
	cd $(DIR); cargo test --all-targets --all-features --workspace
