.PHONY: help install dev ui api build clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Install ────────────────────────────────────────────────

install:  ## Install all dependencies (Python + Node)
	pip install -r requirements.txt
	cd ui && npm install

# ── Run (Development) ─────────────────────────────────────

dev:  ## Start both API + UI in dev mode (run in separate terminals)
	@echo ""
	@echo "  Start these in TWO terminals:"
	@echo ""
	@echo "  Terminal 1 (API):   make api"
	@echo "  Terminal 2 (UI):    make ui"
	@echo ""

api:  ## Start Flask API backend on port 5001
	cd orchestrator && python app.py

ui:  ## Start React dev server on port 5173
	cd ui && npm run dev

# ── Build ──────────────────────────────────────────────────

build:  ## Build React UI for production
	cd ui && npm run build

# ── Clean ──────────────────────────────────────────────────

clean:  ## Clean build artifacts
	rm -rf ui/dist ui/node_modules/.vite
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
