.PHONY: start broker publisher consumer

start:
	@echo "Usage: make start broker|publisher|consumer"

broker:
	cargo run -p broker

publisher:
	cargo run -p publisher -- $(topic) $(message)

consumer:
	cargo run -p consumer -- $(topic) $(offset) $(max)

