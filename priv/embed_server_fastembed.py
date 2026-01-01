#!/usr/bin/env python3
"""
FastEmbed embedding server for barrel_vectordb using JSON-lines protocol.

Communicates via stdin/stdout using JSON lines (one JSON object per line).
Uses FastEmbed (ONNX-based) for lightweight, fast embeddings.

Requirements:
    pip install fastembed

Usage:
    python embed_server_fastembed.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "embed", "texts": ["hello", "world"]}

    Response (stdout):
        {"ok": true, "dimensions": 384, "model": "BAAI/bge-small-en-v1.5"}
        {"ok": true, "embeddings": [[...], [...]]}
        {"ok": false, "error": "error message"}

Supported models (see barrel_vectordb_models for full list):
    - BAAI/bge-small-en-v1.5 (384 dims, default)
    - BAAI/bge-base-en-v1.5 (768 dims)
    - sentence-transformers/all-MiniLM-L6-v2 (384 dims)
    - nomic-ai/nomic-embed-text-v1.5 (768 dims)
    - And many more from HuggingFace
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

# Default model - small and fast
DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"


def load_model(model_name):
    """Load the FastEmbed model."""
    try:
        from fastembed import TextEmbedding
        logger.info(f"Loading FastEmbed model: {model_name}")
        model = TextEmbedding(model_name=model_name)
        # Get dimension by embedding a test string
        test_embedding = list(model.embed(["test"]))[0]
        dimension = len(test_embedding)
        logger.info(f"Model loaded. Dimension: {dimension}")
        return model, dimension
    except ImportError:
        logger.error("fastembed not installed. Run: pip install fastembed")
        return None, None
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        return None, None


def handle_info(model, model_name, dimension):
    """Handle info request."""
    return {
        "ok": True,
        "dimensions": dimension,
        "model": model_name,
        "backend": "fastembed"
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
        # FastEmbed returns a generator, convert to list
        embeddings = list(model.embed(texts))
        # Convert numpy arrays to lists
        embeddings_list = [emb.tolist() for emb in embeddings]
        return {"ok": True, "embeddings": embeddings_list}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Load model
    model, dimension = load_model(model_name)
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
            response = handle_info(model, model_name, dimension)
        elif action == "embed":
            texts = request.get("texts", [])
            response = handle_embed(model, texts)
        else:
            response = {"ok": False, "error": f"Unknown action: {action}"}

        # Send response
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
