#!/usr/bin/env python3
"""
Cross-encoder reranking server for barrel_vectordb using async JSON-lines protocol.

Cross-encoders score query-document pairs directly, providing more accurate
relevance scores than bi-encoder similarity for reranking candidate results.

Uses asyncio for non-blocking I/O with optional uvloop for better performance.

Requirements:
    pip install transformers torch
    pip install uvloop  # optional, for better performance

Usage:
    python rerank_server.py [model_name]

Protocol:
    Request (stdin):
        {"action": "info"}
        {"action": "rerank", "query": "search query", "documents": ["doc1", "doc2", ...]}
        {"action": "rerank", "query": "search query", "documents": ["doc1", ...], "top_k": 5, "id": 123}

    Response (stdout):
        {"ok": true, "model": "cross-encoder/ms-marco-MiniLM-L-6-v2"}
        {"ok": true, "results": [{"index": 0, "score": 0.95}, {"index": 2, "score": 0.82}, ...], "id": 123}
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
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Try to use uvloop for better performance
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    HAS_UVLOOP = True
except ImportError:
    HAS_UVLOOP = False

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

    def rerank_sync(self, query, documents, top_k=None):
        """Rerank documents by relevance to query (synchronous).

        Args:
            query: The search query string
            documents: List of document strings to rerank
            top_k: Optional limit on number of results to return

        Returns:
            dict with 'ok' and 'results' keys
        """
        import torch

        if not documents:
            return {"ok": True, "results": []}

        try:
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

            return {"ok": True, "results": results}
        except Exception as e:
            return {"ok": False, "error": str(e)}


class AsyncRerankServer:
    """Async reranking server matching barrel_embed style."""

    def __init__(self, model: CrossEncoderModel, max_workers: int = 4):
        self.model = model
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.write_lock = asyncio.Lock()
        self._max_workers = max_workers

    async def run(self):
        """Main event loop - read requests, dispatch, write responses."""
        loop = asyncio.get_event_loop()

        # Setup async stdin reader
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        # Setup async stdout writer
        writer_transport, writer_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout
        )
        writer = asyncio.StreamWriter(
            writer_transport, writer_protocol, reader, loop
        )

        logger.info(f"Async server ready (uvloop={HAS_UVLOOP}, workers={self._max_workers})")

        # Process requests
        while True:
            line = await reader.readline()
            if not line:
                break
            # Fire and forget - handle_request manages its own response
            asyncio.create_task(self.handle_request(line, writer))

    async def handle_request(self, line: bytes, writer):
        """Handle a single request."""
        request_id = None
        try:
            request = json.loads(line.decode())
            request_id = request.get("id")
            response = await self.dispatch(request)
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            response = {"ok": False, "error": str(e)}

        # Always include request ID in response if provided
        if request_id is not None:
            response["id"] = request_id

        # Serialize writes to prevent interleaving
        async with self.write_lock:
            output = json.dumps(response) + "\n"
            writer.write(output.encode())
            await writer.drain()

    async def dispatch(self, request: dict) -> dict:
        """Dispatch request to appropriate handler."""
        action = request.get("action")

        if action == "info":
            return self.handle_info()
        elif action == "rerank":
            query = request.get("query", "")
            documents = request.get("documents", [])
            top_k = request.get("top_k")
            return await self.handle_rerank(query, documents, top_k)
        else:
            return {"ok": False, "error": f"Unknown action: {action}"}

    def handle_info(self) -> dict:
        """Handle info request."""
        return {
            "ok": True,
            "model": self.model.model_name,
            "type": "rerank",
            "async": True,
            "uvloop": HAS_UVLOOP,
            "workers": self._max_workers
        }

    async def handle_rerank(self, query: str, documents: list, top_k=None) -> dict:
        """Handle rerank request - run in thread pool."""
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

        # Run CPU-bound reranking in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor,
            self.model.rerank_sync,
            query,
            documents,
            top_k
        )
        return result


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Load model
    model = CrossEncoderModel(model_name)
    if not model.load():
        print(json.dumps({"ok": False, "error": "Failed to load model"}), flush=True)
        sys.exit(1)

    # Create and run async server
    server = AsyncRerankServer(model)
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()
