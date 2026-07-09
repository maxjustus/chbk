.PHONY: build-x86-linux

build-x86-linux:
	cargo zigbuild --release --target x86_64-unknown-linux-musl
