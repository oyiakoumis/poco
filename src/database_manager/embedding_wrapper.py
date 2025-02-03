from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, runtime_checkable

from langchain_core.embeddings import Embeddings


@runtime_checkable
class Embeddable(Protocol):
    """Protocol for objects that can be embedded."""

    def get_content_for_embedding(self) -> str:
        """Get content that will be used for generating embeddings."""
        ...


@dataclass
class EmbeddingConfig:
    """Configuration for embeddings."""

    dimension: int = 1536
    index_name: str = "embedding_index"
    field_name: str = "_embedding"
    similarity: str = "cosine"
    num_candidates_multiplier: int = 10


class EmbeddingWrapper:
    """
    Wrapper for managing embeddings with consistent configuration and behavior.
    Provides centralized embedding functionality for documents and collections.
    """

    def __init__(self, embeddings: Embeddings, config: EmbeddingConfig = EmbeddingConfig()):
        """
        Initialize the embedding wrapper.

        Args:
            embeddings: Embeddings model to use
            config: Configuration for embeddings
        """
        self.embeddings = embeddings
        self.config = config

    def get_index_definition(self) -> Dict[str, Any]:
        """
        Get the vector search index definition.

        Returns:
            Dict containing the index definition for MongoDB
        """
        return {
            "mappings": {
                "dynamic": True,
                "fields": {self.config.field_name: {"type": "knnVector", "dimensions": self.config.dimension, "similarity": self.config.similarity}},
            }
        }

    def get_search_pipeline(
        self, query_vector: List[float], num_results: int = 5, min_score: float = 0.0, filter_dict: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Get the aggregation pipeline for vector search.

        Args:
            query_vector: Vector to search for
            num_results: Maximum number of results to return
            min_score: Minimum similarity score (0-1)
            filter_dict: Optional additional filter criteria

        Returns:
            List of pipeline stages for MongoDB aggregation
        """
        pipeline = [
            {
                "$vectorSearch": {
                    "index": self.config.index_name,
                    "path": self.config.field_name,
                    "queryVector": query_vector,
                    "numCandidates": num_results * self.config.num_candidates_multiplier,
                    "limit": num_results,
                    "exact": False,
                }
            },
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            {"$match": {"score": {"$gte": min_score}}},
        ]

        if filter_dict:
            pipeline.append({"$match": filter_dict})

        pipeline.append({"$project": {"score": 0}})
        return pipeline

    def embed(self, obj: Embeddable) -> List[float]:
        """
        Generate embeddings for an object implementing the Embeddable protocol.

        Args:
            obj: Object that provides content for embedding

        Returns:
            List of floats representing the embedding vector
        """
        content = obj.get_content_for_embedding()
        return self.embeddings.embed_query(content)

    def embed_batch(self, objects: List[Embeddable]) -> List[List[float]]:
        """
        Generate embeddings for multiple objects in batch.

        Args:
            objects: List of objects that provide content for embedding

        Returns:
            List of embedding vectors
        """
        contents = [obj.get_content_for_embedding() for obj in objects]
        return self.embeddings.embed_documents(contents)

    def embed_json(self, data: Dict[str, Any]) -> List[float]:
        """
        Generate embeddings for a JSON-serializable dictionary.

        Args:
            data: Dictionary to embed

        Returns:
            List of floats representing the embedding vector
        """
        content = json.dumps(data)
        return self.embeddings.embed_query(content)
