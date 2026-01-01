#!/usr/bin/env python3
"""
Sparse embedding server for barrel_vectordb using JSON-lines protocol.

Supports SPLADE models for neural sparse embeddings.
Communicates via stdin/stdout using JSON lines.

Requirements:
    pip install transformers torch

Usage:
    python sparse_server.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "embed", "texts": ["hello", "world"]}

    Response (stdout):
        {"ok": true, "vocab_size": 30522, "model": "prithivida/Splade_PP_en_v1"}
        {"ok": true, "embeddings": [{"indices": [1, 5, 10], "values": [0.5, 0.3, 0.8]}, ...]}
        {"ok": false, "error": "error message"}

Supported models:
    - prithivida/Splade_PP_en_v1 (default, SPLADE++)
    - naver/splade-cocondenser-ensembledistil
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
DEFAULT_MODEL = "prithivida/Splade_PP_en_v1"


class SpladeModel:
    """SPLADE sparse embedding model."""

    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None
        self.vocab_size = None

    def load(self):
        """Load the model and tokenizer."""
        try:
            import torch
            from transformers import AutoModelForMaskedLM, AutoTokenizer

            logger.info(f"Loading SPLADE model: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForMaskedLM.from_pretrained(self.model_name)
            self.model.eval()

            self.vocab_size = self.tokenizer.vocab_size
            logger.info(f"Model loaded. Vocab size: {self.vocab_size}")

            return True
        except ImportError as e:
            logger.error(f"Missing dependency: {e}. Run: pip install transformers torch")
            return False
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def encode(self, texts):
        """Encode texts to sparse vectors."""
        import torch

        # Tokenize
        inputs = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        )

        # Get model output
        with torch.no_grad():
            output = self.model(**inputs)

        # SPLADE: log(1 + ReLU(logits)) * attention_mask, then max pool
        logits = output.logits
        relu_log = torch.log1p(torch.relu(logits))

        # Apply attention mask
        attention_mask = inputs["attention_mask"].unsqueeze(-1)
        weighted = relu_log * attention_mask

        # Max pooling over sequence length
        sparse_vecs, _ = torch.max(weighted, dim=1)

        # Convert to sparse format (indices and values)
        results = []
        for vec in sparse_vecs:
            # Get non-zero indices and values
            non_zero_mask = vec > 0
            indices = torch.where(non_zero_mask)[0].tolist()
            values = vec[non_zero_mask].tolist()

            results.append({
                "indices": indices,
                "values": values
            })

        return results


def handle_info(model):
    """Handle info request."""
    return {
        "ok": True,
        "vocab_size": model.vocab_size,
        "model": model.model_name,
        "type": "sparse"
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
    model = SpladeModel(model_name)
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
