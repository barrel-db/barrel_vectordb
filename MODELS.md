# Supported Models

This document lists all models in the barrel_vectordb model registry.

## Text Embedding Models

Dense embedding models for semantic text search.

| Model | Dimensions | Max Tokens | Description |
|-------|------------|------------|-------------|
| `BAAI/bge-small-en-v1.5` | 384 | 512 | Small English model, fast inference |
| `BAAI/bge-base-en-v1.5` | 768 | 512 | Base English model, good quality/speed balance **(default)** |
| `BAAI/bge-large-en-v1.5` | 1024 | 512 | Large English model, best quality |
| `BAAI/bge-small-zh-v1.5` | 512 | 512 | Small Chinese model |
| `BAAI/bge-small-en` | 384 | 512 | Small English model (v1) |
| `BAAI/bge-base-en` | 768 | 512 | Base English model (v1) |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | 256 | Fast general purpose model |
| `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` | 384 | 128 | Multilingual paraphrase model |
| `sentence-transformers/paraphrase-multilingual-mpnet-base-v2` | 768 | 128 | Multilingual paraphrase model (larger) |
| `sentence-transformers/all-mpnet-base-v2` | 768 | 384 | High quality general purpose model |
| `nomic-ai/nomic-embed-text-v1` | 768 | 8192 | Long context model (v1) |
| `nomic-ai/nomic-embed-text-v1.5` | 768 | 8192 | Long context model (v1.5) |
| `nomic-ai/nomic-embed-text-v1.5-Q` | 768 | 8192 | Long context model, quantized |
| `jinaai/jina-embeddings-v2-small-en` | 512 | 8192 | Jina small English, long context |
| `jinaai/jina-embeddings-v2-base-en` | 768 | 8192 | Jina base English, long context |
| `jinaai/jina-embeddings-v2-base-de` | 768 | 8192 | Jina base German, long context |
| `jinaai/jina-embeddings-v2-base-code` | 768 | 8192 | Jina base for code, long context |
| `snowflake/snowflake-arctic-embed-xs` | 384 | 512 | Snowflake extra small model |
| `snowflake/snowflake-arctic-embed-s` | 384 | 512 | Snowflake small model |
| `snowflake/snowflake-arctic-embed-m` | 768 | 512 | Snowflake medium model |
| `snowflake/snowflake-arctic-embed-m-long` | 768 | 2048 | Snowflake medium, long context |
| `snowflake/snowflake-arctic-embed-l` | 1024 | 512 | Snowflake large model |
| `mixedbread-ai/mxbai-embed-large-v1` | 1024 | 512 | Mixedbread large model, high quality |
| `thenlper/gte-large` | 1024 | 512 | GTE large model |
| `intfloat/multilingual-e5-large` | 1024 | 512 | Multilingual E5 large model |
| `Qdrant/clip-ViT-B-32-text` | 512 | 77 | CLIP text encoder |

## Sparse Embedding Models

Sparse models for lexical and hybrid search.

| Model | Type | Description |
|-------|------|-------------|
| `bm25` | statistical | BM25 sparse embeddings (pure Erlang, no model required) |
| `Qdrant/bm25` | statistical | BM25 sparse embeddings |
| `Qdrant/bm42-all-minilm-l6-v2-attentions` | neural | BM42 with attention weights |
| `prithivida/Splade_PP_en_v1` | neural | SPLADE++ English model |

## Late Interaction Models

Multi-vector models for fine-grained matching (ColBERT-style).

| Model | Dimensions | Max Tokens | Description |
|-------|------------|------------|-------------|
| `colbert-ir/colbertv2.0` | 128 | 512 | ColBERT v2 for late interaction retrieval |
| `answerdotai/answerai-colbert-small-v1` | 96 | 512 | Small ColBERT model |
| `jinaai/jina-colbert-v2` | 128 | 8192 | Jina ColBERT v2, long context |

## Image Embedding Models

Models for image search and cross-modal retrieval.

| Model | Dimensions | Description |
|-------|------------|-------------|
| `Qdrant/clip-ViT-B-32-vision` | 512 | CLIP vision encoder |
| `Qdrant/resnet50-onnx` | 2048 | ResNet50 image embeddings |
| `Qdrant/Unicom-ViT-B-32` | 512 | Unicom ViT-B-32 image model |
| `Qdrant/Unicom-ViT-B-16` | 768 | Unicom ViT-B-16 image model |

## Reranking Models

Cross-encoder models for reranking search results.

| Model | Description |
|-------|-------------|
| `Xenova/ms-marco-MiniLM-L-6-v2` | MS MARCO MiniLM reranker (small) |
| `Xenova/ms-marco-MiniLM-L-12-v2` | MS MARCO MiniLM reranker (medium) |
| `BAAI/bge-reranker-base` | BGE reranker base model |
| `jinaai/jina-reranker-v1-tiny-en` | Jina tiny English reranker |
| `jinaai/jina-reranker-v1-turbo-en` | Jina turbo English reranker |
| `jinaai/jina-reranker-v2-base-multilingual` | Jina multilingual reranker |

## Provider Mapping

| Model Type | Provider | Module |
|------------|----------|--------|
| Text | `local` | `barrel_vectordb_embed_local` |
| Text | `fastembed` | `barrel_vectordb_embed_fastembed` |
| Text | `ollama` | `barrel_vectordb_embed_ollama` |
| Text | `openai` | `barrel_vectordb_embed_openai` |
| Sparse | `splade` | `barrel_vectordb_embed_splade` |
| Sparse | `bm25` | `barrel_vectordb_bm25` |
| Late Interaction | `colbert` | `barrel_vectordb_embed_colbert` |
| Image | `clip` | `barrel_vectordb_embed_clip` |
| Rerank | - | `barrel_vectordb_rerank` |

## Querying the Registry

```erlang
%% List all model types
barrel_vectordb_models:types().
%% => [text, sparse, late_interaction, image, rerank]

%% List models by type
{ok, Models} = barrel_vectordb_models:list(text).

%% Get model info
{ok, Info} = barrel_vectordb_models:info(<<"BAAI/bge-base-en-v1.5">>).

%% Get default model for a type
{ok, Default} = barrel_vectordb_models:default(text).

%% Check if model is known
barrel_vectordb_models:is_known(<<"BAAI/bge-base-en-v1.5">>).
%% => true
```

## Adding Custom Models

The model registry is loaded from `priv/models.json`. You can add custom models by editing this file:

```json
{
  "models": {
    "text": [
      {
        "name": "my-org/my-custom-model",
        "dimensions": 768,
        "max_tokens": 512,
        "description": "My custom model",
        "source": "huggingface"
      }
    ]
  }
}
```

Then reload the registry:

```erlang
barrel_vectordb_models:reload().
```
