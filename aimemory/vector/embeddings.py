from __future__ import annotations

import hashlib
import math
import re
from typing import Protocol


TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")


class Embedder(Protocol):
    @property
    def dimension(self) -> int: ...

    @property
    def model_name(self) -> str: ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]: ...


class HashEmbedder:
    def __init__(self, dimension: int = 32):
        self._dimension = dimension
        self._model_name = f"hash-{dimension}"

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        vector = [0.0] * self._dimension
        tokens = TOKEN_RE.findall(text.lower())
        if not tokens:
            return vector
        features: list[tuple[str, float]] = []
        for token in tokens:
            features.append((token, 1.0))
            if len(token) >= 3:
                for index in range(len(token) - 2):
                    features.append((token[index : index + 3], 0.35))
        for token, base_weight in features:
            digest = hashlib.sha1(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self._dimension
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            weight = base_weight * (1.0 + (digest[5] / 255.0))
            vector[index] += sign * weight
        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            return vector
        return [value / norm for value in vector]
