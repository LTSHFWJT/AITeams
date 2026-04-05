from __future__ import annotations

import unittest
import warnings

from langchain_core.messages import AIMessage, AnyMessage
from litellm.types.utils import PromptTokensDetailsWrapper, Usage
from pydantic import BaseModel

from aiteams.deepagents.middleware import sanitize_message_for_checkpoint


class _Holder(BaseModel):
    messages: list[AnyMessage]


def _dirty_message() -> AIMessage:
    usage = Usage(completion_tokens=1, prompt_tokens=2, total_tokens=3)
    usage.prompt_tokens_details = PromptTokensDetailsWrapper(cached_tokens=0)
    return AIMessage(content="test", response_metadata={"usage": usage})


class DeepAgentsMessageSanitizerTests(unittest.TestCase):
    def test_sanitize_message_removes_pydantic_serializer_warnings(self) -> None:
        dirty = _dirty_message()

        with warnings.catch_warnings(record=True) as dirty_warnings:
            warnings.simplefilter("always")
            _Holder(messages=[dirty]).model_dump()

        self.assertTrue(
            any("Pydantic serializer warnings" in str(item.message) for item in dirty_warnings),
            "Expected the unsanitized AIMessage to trigger serializer warnings.",
        )

        cleaned = sanitize_message_for_checkpoint(dirty)

        with warnings.catch_warnings(record=True) as clean_warnings:
            warnings.simplefilter("always")
            dumped = _Holder(messages=[cleaned]).model_dump()

        self.assertFalse(
            any("Pydantic serializer warnings" in str(item.message) for item in clean_warnings),
            "Sanitized AIMessage should not trigger serializer warnings.",
        )
        usage = dumped["messages"][0]["response_metadata"]["usage"]
        self.assertEqual(usage["completion_tokens"], 1)
        self.assertEqual(usage["prompt_tokens_details"]["cached_tokens"], 0)


if __name__ == "__main__":
    unittest.main()
