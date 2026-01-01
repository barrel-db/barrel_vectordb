#!/usr/bin/env python3
"""
CLIP image embedding server for barrel_vectordb using JSON-lines protocol.

CLIP (Contrastive Language-Image Pre-training) encodes both images and text
into the same embedding space, enabling cross-modal search.

Requirements:
    pip install transformers torch pillow

Usage:
    python image_server.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "embed_image", "images": ["base64_data", ...]}
        {"action": "embed_text", "texts": ["hello world", ...]}

    Response (stdout):
        {"ok": true, "dimensions": 512, "model": "openai/clip-vit-base-patch32"}
        {"ok": true, "embeddings": [[0.1, 0.2, ...], ...]}
        {"ok": false, "error": "error message"}

Supported models:
    - openai/clip-vit-base-patch32 (default, 512 dims)
    - openai/clip-vit-base-patch16 (512 dims, higher quality)
    - openai/clip-vit-large-patch14 (768 dims, best quality)
    - laion/CLIP-ViT-B-32-laion2B-s34B-b79K (512 dims, LAION trained)
"""

import sys
import json
import base64
import logging
from io import BytesIO

# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Default model
DEFAULT_MODEL = "openai/clip-vit-base-patch32"


class CLIPModel:
    """CLIP image/text embedding model."""

    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None
        self.processor = None
        self.dimension = None

    def load(self):
        """Load the model and processor."""
        try:
            import torch
            from transformers import CLIPModel as HFCLIPModel, CLIPProcessor

            logger.info(f"Loading CLIP model: {self.model_name}")

            self.processor = CLIPProcessor.from_pretrained(self.model_name)
            self.model = HFCLIPModel.from_pretrained(self.model_name)
            self.model.eval()

            # Get dimension from model config
            self.dimension = self.model.config.projection_dim

            logger.info(f"Model loaded. Dimension: {self.dimension}")
            return True

        except ImportError as e:
            logger.error(f"Missing dependency: {e}. Run: pip install transformers torch pillow")
            return False
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def encode_images(self, image_data_list):
        """Encode images to embeddings.

        Args:
            image_data_list: List of base64-encoded image data

        Returns:
            List of embedding vectors
        """
        import torch
        from PIL import Image

        results = []

        for image_data in image_data_list:
            try:
                # Decode base64 image
                image_bytes = base64.b64decode(image_data)
                image = Image.open(BytesIO(image_bytes)).convert("RGB")

                # Process image
                inputs = self.processor(images=image, return_tensors="pt")

                # Get image embedding
                with torch.no_grad():
                    image_features = self.model.get_image_features(**inputs)
                    # Normalize
                    image_features = image_features / image_features.norm(p=2, dim=-1, keepdim=True)

                results.append(image_features[0].tolist())

            except Exception as e:
                logger.error(f"Failed to encode image: {e}")
                raise ValueError(f"Failed to encode image: {e}")

        return results

    def encode_texts(self, texts):
        """Encode texts to embeddings (for cross-modal search).

        Args:
            texts: List of text strings

        Returns:
            List of embedding vectors
        """
        import torch

        results = []

        for text in texts:
            # Process text
            inputs = self.processor(text=text, return_tensors="pt", padding=True, truncation=True)

            # Get text embedding
            with torch.no_grad():
                text_features = self.model.get_text_features(**inputs)
                # Normalize
                text_features = text_features / text_features.norm(p=2, dim=-1, keepdim=True)

            results.append(text_features[0].tolist())

        return results


def handle_info(model):
    """Handle info request."""
    return {
        "ok": True,
        "dimensions": model.dimension,
        "model": model.model_name,
        "type": "image"
    }


def handle_embed_image(model, images):
    """Handle image embed request."""
    if not isinstance(images, list):
        return {"ok": False, "error": "images must be a list"}

    if len(images) == 0:
        return {"ok": True, "embeddings": []}

    # Validate all images are strings (base64)
    for i, img in enumerate(images):
        if not isinstance(img, str):
            return {"ok": False, "error": f"images[{i}] must be a base64 string"}

    try:
        embeddings = model.encode_images(images)
        return {"ok": True, "embeddings": embeddings}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def handle_embed_text(model, texts):
    """Handle text embed request (for cross-modal search)."""
    if not isinstance(texts, list):
        return {"ok": False, "error": "texts must be a list"}

    if len(texts) == 0:
        return {"ok": True, "embeddings": []}

    # Validate all texts are strings
    for i, text in enumerate(texts):
        if not isinstance(text, str):
            return {"ok": False, "error": f"texts[{i}] must be a string"}

    try:
        embeddings = model.encode_texts(texts)
        return {"ok": True, "embeddings": embeddings}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Load model
    model = CLIPModel(model_name)
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
        elif action == "embed_image":
            images = request.get("images", [])
            response = handle_embed_image(model, images)
        elif action == "embed_text":
            texts = request.get("texts", [])
            response = handle_embed_text(model, texts)
        else:
            response = {"ok": False, "error": f"Unknown action: {action}"}

        # Send response
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
