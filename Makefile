SHELL = /bin/bash

DIR=$(shell pwd)

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features --workspace -- -D warnings

fmt:
	cd $(DIR); cargo fmt -- --check
