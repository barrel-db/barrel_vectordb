#!/usr/bin/env python3
"""
ColBERT embedding server for barrel_vectordb using JSON-lines protocol.

ColBERT produces multi-vector embeddings (one vector per token) for
fine-grained late interaction matching.

Requirements:
    pip install transformers torch

Usage:
    python colbert_server.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "embed", "texts": ["hello world", "test"]}

    Response (stdout):
        {"ok": true, "dimensions": 128, "model": "colbert-ir/colbertv2.0"}
        {"ok": true, "embeddings": [[[0.1, 0.2, ...], [0.3, 0.4, ...]], ...]}
        {"ok": false, "error": "error message"}

    Note: Each text produces a list of token vectors, not a single vector.

Supported models:
    - colbert-ir/colbertv2.0 (default, 128 dims)
    - answerdotai/answerai-colbert-small-v1 (96 dims)
    - jinaai/jina-colbert-v2 (128 dims, long context)
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
DEFAULT_MODEL = "colbert-ir/colbertv2.0"


class ColBERTModel:
    """ColBERT multi-vector embedding model."""

    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None
        self.dimension = None
        self.linear = None  # ColBERT projection layer

    def load(self):
        """Load the model and tokenizer."""
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer

            logger.info(f"Loading ColBERT model: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModel.from_pretrained(self.model_name)
            self.model.eval()

            # Get dimension from model config
            hidden_size = self.model.config.hidden_size

            # ColBERT typically projects to 128 dimensions
            # Check if model has a linear projection layer
            if hasattr(self.model, 'linear'):
                self.linear = self.model.linear
                self.dimension = self.linear.out_features
            else:
                # Default ColBERT dimension
                self.dimension = min(128, hidden_size)
                # Create projection if needed
                if hidden_size != self.dimension:
                    self.linear = torch.nn.Linear(hidden_size, self.dimension, bias=False)
                    torch.nn.init.xavier_uniform_(self.linear.weight)

            logger.info(f"Model loaded. Dimension: {self.dimension}")
            return True

        except ImportError as e:
            logger.error(f"Missing dependency: {e}. Run: pip install transformers torch")
            return False
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def encode(self, texts):
        """Encode texts to multi-vector embeddings."""
        import torch
        import torch.nn.functional as F

        results = []

        for text in texts:
            # Tokenize single text
            inputs = self.tokenizer(
                text,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt"
            )

            # Get model output
            with torch.no_grad():
                output = self.model(**inputs)

            # Get token embeddings (last hidden state)
            token_embeddings = output.last_hidden_state[0]  # [seq_len, hidden_size]

            # Apply projection if needed
            if self.linear is not None:
                token_embeddings = self.linear(token_embeddings)

            # Normalize embeddings (ColBERT uses L2 normalization)
            token_embeddings = F.normalize(token_embeddings, p=2, dim=-1)

            # Get attention mask to filter padding tokens
            attention_mask = inputs["attention_mask"][0]

            # Filter out padding tokens and special tokens ([CLS], [SEP])
            # Keep only actual content tokens
            valid_tokens = []
            for i, (emb, mask) in enumerate(zip(token_embeddings, attention_mask)):
                if mask == 1:  # Not padding
                    valid_tokens.append(emb.tolist())

            results.append(valid_tokens)

        return results


def handle_info(model):
    """Handle info request."""
    return {
        "ok": True,
        "dimensions": model.dimension,
        "model": model.model_name,
        "type": "multi_vector"
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
        embeddings = model.encode(texts)
        return {"ok": True, "embeddings": embeddings}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Load model
    model = ColBERTModel(model_name)
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
        elif action == "embed":
            texts = request.get("texts", [])
            response = handle_embed(model, texts)
        else:
            response = {"ok": False, "error": f"Unknown action: {action}"}

        # Send response
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
