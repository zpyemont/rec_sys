"""
TensorFlow Serving client for calling Monolith model predictions
"""

import grpc
import numpy as np
from tensorflow_serving.apis import predict_pb2, prediction_service_pb2_grpc
import tensorflow as tf
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class MonolithClient:
    """Client for calling Monolith model via TensorFlow Serving gRPC"""

    def __init__(
        self,
        host: str,
        port: int = 2223,
        model_name: str = 'fashion_two_tower',
        timeout: float = 5.0
    ):
        self.host = host
        self.port = port
        self.model_name = model_name
        self.timeout = timeout
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = prediction_service_pb2_grpc.PredictionServiceStub(self.channel)
        logger.info(f"Monolith client initialized: {host}:{port}, model={model_name}")

    def predict(
        self,
        user_id: str,
        product_ids: List[str]
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray], Dict[str, float]]:
        """
        Call Monolith for predictions

        Args:
            user_id: User identifier
            product_ids: List of product identifiers to score

        Returns:
            Tuple of:
            - user_embedding: 128-dim numpy array
            - product_embeddings: dict mapping product_id -> 128-dim embedding
            - scores: dict mapping product_id -> score

        Raises:
            grpc.RpcError: If prediction call fails
        """
        if not product_ids:
            logger.warning("Empty product_ids list provided")
            return np.zeros(128), {}, {}

        # Convert IDs to int64 (Monolith uses hash of string IDs)
        user_id_hash = self._hash_id(user_id)
        product_id_hashes = [self._hash_id(pid) for pid in product_ids]

        # Create TensorFlow Example protos
        examples = []
        for pid_hash in product_id_hashes:
            example = tf.train.Example()
            example.features.feature['user_id'].int64_list.value.append(user_id_hash)
            example.features.feature['product_id'].int64_list.value.append(pid_hash)
            # Label is required by input schema but ignored during inference
            example.features.feature['label'].float_list.value.append(0.0)
            examples.append(example.SerializeToString())

        # Create prediction request
        request = predict_pb2.PredictRequest()
        request.model_spec.name = self.model_name
        request.model_spec.signature_name = 'serving_default'

        # Set input
        request.inputs['examples'].CopyFrom(
            tf.make_tensor_proto(examples, dtype=tf.string)
        )

        try:
            # Call model
            result = self.stub.Predict(request, timeout=self.timeout)
            logger.debug(f"Prediction successful for user={user_id}, {len(product_ids)} products")

            # Extract outputs
            user_embedding = self._extract_user_embedding(result)
            product_embeddings = self._extract_product_embeddings(result, product_ids)
            scores = self._extract_scores(result, product_ids)

            return user_embedding, product_embeddings, scores

        except grpc.RpcError as e:
            logger.error(f"Monolith prediction failed: {e.code()} - {e.details()}")
            raise

    def get_user_embedding(self, user_id: str) -> np.ndarray:
        """
        Call user tower to get 128-dim user embedding.

        Args:
            user_id: User identifier

        Returns:
            128-dim numpy array (L2-normalized)

        Raises:
            grpc.RpcError: If prediction call fails
        """
        user_id_hash = self._hash_id(user_id)

        example = tf.train.Example()
        example.features.feature['user_id'].int64_list.value.append(user_id_hash)

        request = predict_pb2.PredictRequest()
        request.model_spec.name = self.model_name
        request.model_spec.signature_name = 'user_tower'

        request.inputs['examples'].CopyFrom(
            tf.make_tensor_proto([example.SerializeToString()], dtype=tf.string))

        try:
            result = self.stub.Predict(request, timeout=self.timeout)
            user_vec = tf.make_ndarray(result.outputs['user_vec'])
            logger.debug(f"User tower prediction successful for user={user_id}")
            return user_vec[0]  # Shape: (128,)
        except grpc.RpcError as e:
            logger.error(f"User tower prediction failed: {e.code()} - {e.details()}")
            raise

    def _hash_id(self, id_str: str) -> int:
        """
        Hash string ID to int64 using FarmHash.

        Must match Monolith training which uses tf.strings.to_hash_bucket_fast
        (FarmHash-based). Python's hash() is non-deterministic across processes.
        """
        import farmhash
        return farmhash.fingerprint64(id_str) % (2**63 - 1)

    def _extract_user_embedding(self, result) -> np.ndarray:
        """
        Extract user embedding from prediction result

        Args:
            result: PredictResponse from TensorFlow Serving

        Returns:
            128-dim numpy array
        """
        try:
            # User embedding should be shape (batch_size, 128)
            # We take the first one since all examples have the same user
            user_emb_array = tf.make_ndarray(result.outputs['user_embedding'])
            return user_emb_array[0]  # Shape: (128,)
        except KeyError:
            logger.error("'user_embedding' not found in model outputs")
            return np.zeros(128)

    def _extract_product_embeddings(
        self,
        result,
        product_ids: List[str]
    ) -> Dict[str, np.ndarray]:
        """
        Extract product embeddings from prediction result

        Args:
            result: PredictResponse from TensorFlow Serving
            product_ids: Original product IDs (to map back)

        Returns:
            Dict mapping product_id -> 128-dim embedding
        """
        try:
            # Product embeddings: shape (batch_size, 128)
            product_embs_array = tf.make_ndarray(result.outputs['product_embedding'])

            # Map back to product IDs
            return {
                pid: emb
                for pid, emb in zip(product_ids, product_embs_array)
            }
        except KeyError:
            logger.error("'product_embedding' not found in model outputs")
            return {pid: np.zeros(128) for pid in product_ids}

    def _extract_scores(self, result, product_ids: List[str]) -> Dict[str, float]:
        """
        Extract scores from prediction result

        Args:
            result: PredictResponse from TensorFlow Serving
            product_ids: Original product IDs (to map back)

        Returns:
            Dict mapping product_id -> score
        """
        try:
            # Scores: shape (batch_size, 1) or (batch_size,)
            scores_array = tf.make_ndarray(result.outputs['score']).flatten()

            # Map back to product IDs
            return {
                pid: float(score)
                for pid, score in zip(product_ids, scores_array)
            }
        except KeyError:
            logger.error("'score' not found in model outputs")
            return {pid: 0.0 for pid in product_ids}

    def close(self):
        """Close the gRPC channel"""
        if self.channel:
            self.channel.close()
            logger.info("Monolith client closed")


# Singleton instance
_monolith_client: MonolithClient | None = None


def get_monolith_client(settings) -> MonolithClient:
    """
    Get or create singleton Monolith client instance

    Args:
        settings: Application settings containing Monolith configuration

    Returns:
        MonolithClient instance
    """
    global _monolith_client
    if _monolith_client is None:
        _monolith_client = MonolithClient(
            host=settings.monolith_host,
            port=settings.monolith_port,
            model_name=settings.monolith_model_name,
            timeout=settings.monolith_timeout
        )
    return _monolith_client
