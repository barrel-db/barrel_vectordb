#!/usr/bin/env python3
"""
Cross-encoder reranking server for barrel_vectordb using JSON-lines protocol.

Cross-encoders score query-document pairs directly, providing more accurate
relevance scores than bi-encoder similarity for reranking candidate results.

Requirements:
    pip install transformers torch

Usage:
    python rerank_server.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "rerank", "query": "search query", "documents": ["doc1", "doc2", ...]}
        {"action": "rerank", "query": "search query", "documents": ["doc1", ...], "top_k": 5}

    Response (stdout):
        {"ok": true, "model": "cross-encoder/ms-marco-MiniLM-L-6-v2"}
        {"ok": true, "results": [{"index": 0, "score": 0.95}, {"index": 2, "score": 0.82}, ...]}
        {"ok": false, "error": "error message"}

Supported models:
    - cross-encoder/ms-marco-MiniLM-L-6-v2 (default, fast, good quality)
    - cross-encoder/ms-marco-MiniLM-L-12-v2 (better quality, slower)
    - BAAI/bge-reranker-base (good quality)
    - BAAI/bge-reranker-large (best quality, slowest)
"""

import sys
import json
import logging

# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Default model
DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class CrossEncoderModel:
    """Cross-encoder reranking model."""

    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None

    def load(self):
        """Load the model and tokenizer."""
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            logger.info(f"Loading cross-encoder model: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self.model.eval()

            logger.info(f"Model loaded successfully")
            return True

        except ImportError as e:
            logger.error(f"Missing dependency: {e}. Run: pip install transformers torch")
            return False
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def rerank(self, query, documents, top_k=None):
        """Rerank documents by relevance to query.

        Args:
            query: The search query string
            documents: List of document strings to rerank
            top_k: Optional limit on number of results to return

        Returns:
            List of {"index": int, "score": float} sorted by score descending
        """
        import torch

        if not documents:
            return []

        # Create query-document pairs
        pairs = [[query, doc] for doc in documents]

        # Tokenize all pairs
        inputs = self.tokenizer(
            pairs,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        )

        # Get scores
        with torch.no_grad():
            outputs = self.model(**inputs)
            # For sequence classification, logits shape is [batch_size, num_labels]
            # For reranking models, typically num_labels=1, so we squeeze
            if outputs.logits.shape[-1] == 1:
                scores = outputs.logits.squeeze(-1)
            else:
                # Some models output 2 classes (not relevant, relevant)
                # Use the "relevant" class score
                scores = outputs.logits[:, 1] if outputs.logits.shape[-1] == 2 else outputs.logits[:, 0]

        # Convert to list and pair with indices
        scores_list = scores.tolist()
        results = [{"index": i, "score": score} for i, score in enumerate(scores_list)]

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)

        # Apply top_k limit if specified
        if top_k is not None and top_k > 0:
            results = results[:top_k]

        return results


def handle_info(model):
    """Handle info request."""
    return {
        "ok": True,
        "model": model.model_name,
        "type": "rerank"
    }


def handle_rerank(model, query, documents, top_k=None):
    """Handle rerank request."""
    if not isinstance(query, str):
        return {"ok": False, "error": "query must be a string"}

    if not isinstance(documents, list):
        return {"ok": False, "error": "documents must be a list"}

    if len(documents) == 0:
        return {"ok": True, "results": []}

    # Validate all documents are strings
    for i, doc in enumerate(documents):
        if not isinstance(doc, str):
            return {"ok": False, "error": f"documents[{i}] must be a string"}

    try:
        results = model.rerank(query, documents, top_k)
        return {"ok": True, "results": results}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Load model
    model = CrossEncoderModel(model_name)
    if not model.load():
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
            response = handle_info(model)
        elif action == "rerank":
            query = request.get("query", "")
            documents = request.get("documents", [])
            top_k = request.get("top_k")
            response = handle_rerank(model, query, documents, top_k)
        else:
            response = {"ok": False, "error": f"Unknown action: {action}"}

        # Send response
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
