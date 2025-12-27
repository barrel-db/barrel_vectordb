#!/usr/bin/env python3
"""
Embedding server for barrel_vectordb using JSON-lines protocol.

Communicates via stdin/stdout using JSON lines (one JSON object per line).
Uses sentence-transformers for CPU-based embeddings.

Usage:
    python embed_server.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "embed", "texts": ["hello", "world"]}

    Response (stdout):
        {"ok": true, "dimensions": 768, "model": "BAAI/bge-base-en-v1.5"}
        {"ok": true, "embeddings": [[...], [...]]}
        {"ok": false, "error": "error message"}
"""

import sys
import json
import logging

# Configure logging to stderr so it doesn't interfere with JSON output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Default model - good balance of quality and speed for code/text
DEFAULT_MODEL = "BAAI/bge-base-en-v1.5"


def load_model(model_name):
    """Load the sentence transformer model."""
    try:
        from sentence_transformers import SentenceTransformer
        logger.info(f"Loading model: {model_name}")
        model = SentenceTransformer(model_name)
        logger.info(f"Model loaded. Dimension: {model.get_sentence_embedding_dimension()}")
        return model
    except ImportError:
        logger.error("sentence-transformers not installed. Run: pip install sentence-transformers")
        return None
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        return None


def handle_info(model, model_name):
    """Handle info request."""
    return {
        "ok": True,
        "dimensions": model.get_sentence_embedding_dimension(),
        "model": model_name
    }


def handle_embed(model, texts):
    """Handle embed request."""
    if not isinstance(texts, list):
        return {"ok": False, "error": "texts must be a list"}

    if len(texts) == 0:
        return {"ok": True, "embeddings": []}

    # Validate all texts are strings
    for i, text in enumerate(texts):
        if not isinstance(text, str):
            return {"ok": False, "error": f"texts[{i}] must be a string"}

    try:
        # Encode with normalization (important for cosine similarity)
        embeddings = model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False
        )
        return {"ok": True, "embeddings": embeddings.tolist()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Load model
    model = load_model(model_name)
    if model is None:
        # Send error and exit
        print(json.dumps({"ok": False, "error": "Failed to load model"}), flush=True)
        sys.exit(1)

    logger.info("Ready to process requests")

    # Process requests line by line
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except json.JSONDecodeError as e:
            response = {"ok": False, "error": f"Invalid JSON: {e}"}
            print(json.dumps(response), flush=True)
            continue

        action = request.get("action")

        if action == "info":
            response = handle_info(model, model_name)
        elif action == "embed":
            texts = request.get("texts", [])
            response = handle_embed(model, texts)
        else:
            response = {"ok": False, "error": f"Unknown action: {action}"}

        # Send response
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
