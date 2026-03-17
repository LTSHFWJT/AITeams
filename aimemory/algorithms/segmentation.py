from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from aimemory.core.text import cosine_similarity, hash_embedding, split_sentences


MARKDOWN_HEADING_RE = re.compile(r"^\s*(#{1,6})\s+(.+?)\s*$")
NUMBERED_HEADING_RE = re.compile(r"^\s*(\d+(?:\.\d+){0,5})[\.\)]?\s+(.+?)\s*$")
LIST_ITEM_RE = re.compile(r"^\s*((?:[-*+])|(?:\d+[\.\)]))\s+(.+?)\s*$")
CODE_FENCE_RE = re.compile(r"^\s*```")
TABLE_LINE_RE = re.compile(r"^\s*\|.*\|\s*$")
CLAUSE_SPLIT_RE = re.compile(r"(?<=[,，;；:：])\s+")


@dataclass(slots=True)
class TextUnit:
    id: str
    level: str
    text: str
    title_path: list[str] = field(default_factory=list)
    section_index: int = 0
    paragraph_index: int = 0
    sentence_index: int | None = None
    start_offset: int = 0
    end_offset: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TextChunk:
    id: str
    text: str
    title_path: list[str] = field(default_factory=list)
    section_titles: list[str] = field(default_factory=list)
    levels: list[str] = field(default_factory=list)
    unit_ids: list[str] = field(default_factory=list)
    start_offset: int = 0
    end_offset: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TextBlock:
    block_type: str
    text: str
    depth: int | None = None
    title: str | None = None
    lines: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SentenceRecord:
    text: str
    sentence_index: int
    start_offset: int
    end_offset: int
    embedding: list[float] = field(default_factory=list)


def segment_text(text: str | None, *, source_id: str = "text") -> list[TextUnit]:
    source = str(text or "").replace("\r\n", "\n").strip()
    if not source:
        return []

    blocks = _blocks(source)
    units: list[TextUnit] = []
    title_path: list[str] = []
    cursor = 0
    section_index = -1
    paragraph_index = 0

    for block in blocks:
        block_text = block.text.strip()
        if not block_text:
            continue
        start_offset = source.find(block_text, cursor)
        if start_offset < 0:
            start_offset = cursor
        end_offset = start_offset + len(block_text)
        cursor = end_offset

        if block.block_type == "heading":
            section_index += 1
            title_path = _updated_title_path(title_path, str(block.title or ""), int(block.depth or 1))
            continue

        base_metadata = {
            "block_type": block.block_type,
            "title_path": list(title_path),
            **dict(block.metadata),
        }

        if block.block_type in {"code_block", "table_block"}:
            units.append(
                TextUnit(
                    id=f"{source_id}:{block.block_type}:{len(units)}",
                    level=block.block_type,
                    text=block_text,
                    title_path=list(title_path),
                    section_index=max(section_index, 0),
                    paragraph_index=paragraph_index,
                    sentence_index=None,
                    start_offset=start_offset,
                    end_offset=end_offset,
                    metadata=base_metadata,
                )
            )
            paragraph_index += 1
            continue

        block_level = "list_item" if block.block_type == "list_item" else "paragraph"
        units.append(
            TextUnit(
                id=f"{source_id}:{block_level}:{len(units)}",
                level=block_level,
                text=block_text,
                title_path=list(title_path),
                section_index=max(section_index, 0),
                paragraph_index=paragraph_index,
                sentence_index=None,
                start_offset=start_offset,
                end_offset=end_offset,
                metadata=base_metadata,
            )
        )

        sentence_records = _sentence_records(source, block_text, start_offset=start_offset, end_offset=end_offset)
        for group in _semantic_sentence_groups(sentence_records):
            cleaned = source[group[0].start_offset : group[-1].end_offset].strip()
            if not cleaned:
                cleaned = " ".join(record.text for record in group)
            if not cleaned:
                continue
            sentence_start = group[0].start_offset
            sentence_end = group[-1].end_offset
            sentence_index = group[0].sentence_index
            units.append(
                TextUnit(
                    id=f"{source_id}:sentence:{len(units)}",
                    level="sentence",
                    text=cleaned,
                    title_path=list(title_path),
                    section_index=max(section_index, 0),
                    paragraph_index=paragraph_index,
                    sentence_index=sentence_index,
                    start_offset=sentence_start,
                    end_offset=sentence_end,
                    metadata={
                        **base_metadata,
                        "sentence_span_start": group[0].sentence_index,
                        "sentence_span_end": group[-1].sentence_index,
                        "sentence_count": len(group),
                    },
                )
            )
        paragraph_index += 1

    return units


def unit_order_key(unit: TextUnit) -> tuple[int, int, int, int]:
    sentence_index = unit.sentence_index if unit.sentence_index is not None else -1
    level_rank = {
        "heading": 0,
        "paragraph": 1,
        "list_item": 2,
        "sentence": 3,
        "table_block": 4,
        "code_block": 5,
    }.get(unit.level, 9)
    return (unit.section_index, unit.paragraph_index, sentence_index, level_rank)


def chunk_text_units(
    text: str | None,
    *,
    source_id: str = "text",
    chunk_size: int = 500,
    overlap: int = 80,
) -> list[TextChunk]:
    source = str(text or "").replace("\r\n", "\n").strip()
    if not source:
        return []

    safe_chunk_size = max(80, int(chunk_size or 0))
    safe_overlap = max(0, min(int(overlap or 0), safe_chunk_size // 2))
    block_units = sorted(
        [
            unit
            for unit in segment_text(source, source_id=source_id)
            if unit.level in {"paragraph", "list_item", "table_block", "code_block"}
        ],
        key=unit_order_key,
    )
    if not block_units:
        return []

    chunks: list[TextChunk] = []
    pending: list[TextUnit] = []
    for unit in block_units:
        if len(unit.text) > safe_chunk_size:
            if pending:
                chunks.append(_materialize_chunk(pending, source_id=source_id, index=len(chunks)))
                pending = []
            chunks.extend(
                _split_oversized_unit(
                    unit,
                    source_id=source_id,
                    start_index=len(chunks),
                    chunk_size=safe_chunk_size,
                    overlap=safe_overlap,
                )
            )
            continue
        if pending and _units_text_length([*pending, unit]) > safe_chunk_size:
            chunks.append(_materialize_chunk(pending, source_id=source_id, index=len(chunks)))
            pending = _overlap_tail(pending, overlap_chars=safe_overlap)
            while pending and _units_text_length([*pending, unit]) > safe_chunk_size:
                pending = pending[1:]
        pending.append(unit)
    if pending:
        chunks.append(_materialize_chunk(pending, source_id=source_id, index=len(chunks)))
    return chunks


def _updated_title_path(current: list[str], title: str, depth: int) -> list[str]:
    normalized = title.strip()
    if not normalized:
        return list(current)
    safe_depth = max(1, min(depth, 6))
    trimmed = list(current[: safe_depth - 1])
    trimmed.append(normalized)
    return trimmed


def _units_text_length(units: list[TextUnit]) -> int:
    if not units:
        return 0
    return sum(len(str(unit.text or "").strip()) for unit in units) + (max(0, len(units) - 1) * 2)


def _overlap_tail(units: list[TextUnit], *, overlap_chars: int) -> list[TextUnit]:
    if overlap_chars <= 0 or not units:
        return []
    selected: list[TextUnit] = []
    used_chars = 0
    for unit in reversed(units):
        text = str(unit.text or "").strip()
        if not text:
            continue
        selected.append(unit)
        used_chars += len(text) + (2 if len(selected) > 1 else 0)
        if used_chars >= overlap_chars:
            break
    return list(reversed(selected))


def _materialize_chunk(units: list[TextUnit], *, source_id: str, index: int) -> TextChunk:
    cleaned_units = [unit for unit in units if str(unit.text or "").strip()]
    if not cleaned_units:
        return TextChunk(id=f"{source_id}:chunk:{index}", text="")

    title_paths = [list(unit.title_path) for unit in cleaned_units if unit.title_path]
    section_titles = list(dict.fromkeys(path[-1] for path in title_paths if path))
    section_label = " / ".join(section_titles[:2])
    if len(section_titles) > 2:
        section_label = f"{section_label} / ..."
    levels = list(dict.fromkeys(str(unit.level) for unit in cleaned_units if str(unit.level).strip()))
    start_offset = min(unit.start_offset for unit in cleaned_units)
    end_offset = max(unit.end_offset for unit in cleaned_units)
    return TextChunk(
        id=f"{source_id}:chunk:{index}",
        text="\n\n".join(str(unit.text or "").strip() for unit in cleaned_units if str(unit.text or "").strip()),
        title_path=list(title_paths[-1]) if title_paths else [],
        section_titles=section_titles,
        levels=levels,
        unit_ids=[unit.id for unit in cleaned_units],
        start_offset=start_offset,
        end_offset=end_offset,
        metadata={
            "title_path": list(title_paths[-1]) if title_paths else [],
            "section_titles": section_titles,
            "section_label": section_label,
            "levels": levels,
            "unit_ids": [unit.id for unit in cleaned_units],
            "start_offset": start_offset,
            "end_offset": end_offset,
        },
    )


def _split_oversized_unit(
    unit: TextUnit,
    *,
    source_id: str,
    start_index: int,
    chunk_size: int,
    overlap: int,
) -> list[TextChunk]:
    if unit.level in {"paragraph", "list_item"}:
        separator = " "
        parts = _semantic_parts(str(unit.text or ""), max_chars=max(96, min(chunk_size, 220)))
    else:
        separator = "\n"
        parts = [line.rstrip() for line in str(unit.text or "").splitlines() if line.strip()]

    packed_parts = _pack_parts(parts, chunk_size=chunk_size, separator=separator, overlap=overlap)
    if not packed_parts:
        packed_parts = _window_slices(str(unit.text or ""), chunk_size=chunk_size, overlap=overlap)

    chunks: list[TextChunk] = []
    cursor = unit.start_offset
    for offset, part in enumerate(packed_parts):
        cleaned = str(part or "").strip()
        if not cleaned:
            continue
        relative_start = str(unit.text or "").find(cleaned, max(0, cursor - unit.start_offset))
        start_offset = unit.start_offset + max(0, relative_start)
        end_offset = min(unit.end_offset, start_offset + len(cleaned))
        cursor = end_offset
        pseudo_unit = TextUnit(
            id=f"{unit.id}:split:{offset}",
            level=unit.level,
            text=cleaned,
            title_path=list(unit.title_path),
            section_index=unit.section_index,
            paragraph_index=unit.paragraph_index,
            sentence_index=offset,
            start_offset=start_offset,
            end_offset=end_offset,
            metadata={**dict(unit.metadata), "split_from": unit.id},
        )
        chunks.append(_materialize_chunk([pseudo_unit], source_id=source_id, index=start_index + len(chunks)))
    return chunks


def _pack_parts(parts: list[str], *, chunk_size: int, separator: str, overlap: int) -> list[str]:
    cleaned_parts = [str(part or "").strip() for part in parts if str(part or "").strip()]
    if not cleaned_parts:
        return []

    windows: list[str] = []
    pending: list[str] = []
    for part in cleaned_parts:
        if len(part) > chunk_size:
            if pending:
                windows.append(separator.join(pending))
                pending = []
            windows.extend(_window_slices(part, chunk_size=chunk_size, overlap=overlap))
            continue
        candidate = separator.join([*pending, part]) if pending else part
        if pending and len(candidate) > chunk_size:
            windows.append(separator.join(pending))
            pending = _overlap_text_tail(pending, separator=separator, overlap=overlap)
            candidate = separator.join([*pending, part]) if pending else part
            while pending and len(candidate) > chunk_size:
                pending = pending[1:]
                candidate = separator.join([*pending, part]) if pending else part
        pending.append(part)
    if pending:
        windows.append(separator.join(pending))
    return windows


def _overlap_text_tail(parts: list[str], *, separator: str, overlap: int) -> list[str]:
    if overlap <= 0 or not parts:
        return []
    selected: list[str] = []
    used_chars = 0
    for part in reversed(parts):
        selected.append(part)
        used_chars += len(part) + (len(separator) if len(selected) > 1 else 0)
        if used_chars >= overlap:
            break
    return list(reversed(selected))


def _window_slices(text: str, *, chunk_size: int, overlap: int) -> list[str]:
    source = str(text or "").strip()
    if not source:
        return []
    if len(source) <= chunk_size:
        return [source]
    safe_overlap = max(0, min(overlap, chunk_size // 2))
    results: list[str] = []
    start = 0
    while start < len(source):
        end = min(len(source), start + chunk_size)
        results.append(source[start:end].strip())
        if end >= len(source):
            break
        start = max(start + 1, end - safe_overlap)
    return [item for item in results if item]


def _sentence_records(source: str, block_text: str, *, start_offset: int, end_offset: int) -> list[SentenceRecord]:
    records: list[SentenceRecord] = []
    local_cursor = start_offset
    for sentence_index, sentence in enumerate(split_sentences(block_text)):
        cleaned = sentence.strip()
        if not cleaned:
            continue
        sentence_start = source.find(cleaned, local_cursor, end_offset + 1)
        if sentence_start < 0:
            sentence_start = local_cursor
        sentence_end = min(end_offset, sentence_start + len(cleaned))
        local_cursor = sentence_end
        records.append(
            SentenceRecord(
                text=cleaned,
                sentence_index=sentence_index,
                start_offset=sentence_start,
                end_offset=sentence_end,
                embedding=hash_embedding(cleaned),
            )
        )
    return records


def _semantic_sentence_groups(records: list[SentenceRecord], *, target_chars: int = 180, max_chars: int = 320) -> list[list[SentenceRecord]]:
    if not records:
        return []
    if len(records) == 1:
        return [[records[0]]]

    groups: list[list[SentenceRecord]] = []
    pending: list[SentenceRecord] = [records[0]]
    short_break_chars = max(24, target_chars // 5)
    medium_break_chars = max(56, target_chars // 2)
    for record in records[1:]:
        previous = pending[-1]
        previous_similarity = _embedding_similarity(previous.embedding, record.embedding)
        group_similarity = _embedding_similarity(_average_embedding([item.embedding for item in pending]), record.embedding)
        surface_chars = record.end_offset - pending[0].start_offset
        should_break = False
        if surface_chars > max_chars and pending:
            should_break = True
        else:
            cohesion = max(previous_similarity, group_similarity)
            pending_chars = pending[-1].end_offset - pending[0].start_offset
            if pending_chars >= short_break_chars and cohesion < 0.145:
                should_break = True
            elif len(pending) >= 2 and pending_chars >= medium_break_chars and cohesion < 0.18:
                should_break = True
            elif pending_chars >= target_chars and cohesion < 0.24:
                should_break = True
        if should_break:
            groups.append(list(pending))
            pending = [record]
            continue
        pending.append(record)
    if pending:
        groups.append(list(pending))
    return groups


def _semantic_parts(text: str, *, max_chars: int) -> list[str]:
    sentences = [sentence.strip() for sentence in split_sentences(text) if sentence.strip()]
    if not sentences:
        return []

    micro_parts: list[str] = []
    clause_limit = max(64, min(max_chars, 180))
    for sentence in sentences:
        if len(sentence) <= clause_limit:
            micro_parts.append(sentence)
            continue
        clauses = [part.strip() for part in CLAUSE_SPLIT_RE.split(sentence) if part.strip()]
        if len(clauses) <= 1:
            micro_parts.extend(_window_slices(sentence, chunk_size=clause_limit, overlap=max(16, clause_limit // 6)))
            continue
        pending: list[str] = []
        for clause in clauses:
            candidate = " ".join([*pending, clause]) if pending else clause
            if pending and len(candidate) > clause_limit:
                micro_parts.append(" ".join(pending))
                pending = [clause]
            else:
                pending.append(clause)
        if pending:
            micro_parts.append(" ".join(pending))
    if len(micro_parts) <= 1:
        return micro_parts

    embeddings = [hash_embedding(part) for part in micro_parts]
    grouped: list[str] = []
    pending_parts: list[str] = [micro_parts[0]]
    pending_embeddings: list[list[float]] = [embeddings[0]]
    for part, embedding in zip(micro_parts[1:], embeddings[1:]):
        previous_similarity = _embedding_similarity(pending_embeddings[-1], embedding)
        group_similarity = _embedding_similarity(_average_embedding(pending_embeddings), embedding)
        candidate = " ".join([*pending_parts, part])
        should_break = False
        if len(candidate) > max_chars and pending_parts:
            should_break = True
        elif len(" ".join(pending_parts)) >= max(96, max_chars // 2) and max(previous_similarity, group_similarity) < 0.22:
            should_break = True
        if should_break:
            grouped.append(" ".join(pending_parts))
            pending_parts = [part]
            pending_embeddings = [embedding]
            continue
        pending_parts.append(part)
        pending_embeddings.append(embedding)
    if pending_parts:
        grouped.append(" ".join(pending_parts))
    return grouped


def _average_embedding(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        return []
    dims = len(vectors[0])
    if dims <= 0:
        return []
    totals = [0.0] * dims
    count = 0
    for vector in vectors:
        if len(vector) != dims:
            continue
        count += 1
        for index, value in enumerate(vector):
            totals[index] += value
    if count <= 0:
        return []
    averaged = [value / count for value in totals]
    norm = sum(value * value for value in averaged) ** 0.5
    if norm <= 0:
        return averaged
    return [value / norm for value in averaged]


def _embedding_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return max(0.0, cosine_similarity(left, right))


def _blocks(text: str) -> list[TextBlock]:
    lines = text.split("\n")
    blocks: list[TextBlock] = []
    buffer: list[str] = []
    in_code_block = False

    def flush_buffer() -> None:
        nonlocal buffer
        if not buffer:
            return
        chunk = "\n".join(buffer).strip()
        buffer = []
        if not chunk:
            return
        for block in _materialize_block(chunk):
            blocks.append(block)

    for line in lines:
        if CODE_FENCE_RE.match(line):
            if in_code_block:
                buffer.append(line)
                chunk = "\n".join(buffer).strip()
                buffer = []
                in_code_block = False
                if chunk:
                    blocks.append(TextBlock(block_type="code_block", text=chunk, lines=chunk.splitlines()))
                continue
            flush_buffer()
            in_code_block = True
            buffer = [line]
            continue
        if in_code_block:
            buffer.append(line)
            continue
        if not line.strip():
            flush_buffer()
            continue
        buffer.append(line)
    flush_buffer()
    return blocks


def _materialize_block(chunk: str) -> list[TextBlock]:
    lines = chunk.splitlines()
    if not lines:
        return []

    if len(lines) == 1:
        heading = _heading_block(lines[0])
        if heading is not None:
            return [heading]

    if len(lines) >= 2 and all(TABLE_LINE_RE.match(line) for line in lines):
        return [TextBlock(block_type="table_block", text=chunk, lines=lines)]

    materialized: list[TextBlock] = []
    pending_lines: list[str] = []
    for line in lines:
        heading = _heading_block(line)
        if heading is not None:
            if pending_lines:
                materialized.extend(_plain_blocks_from_lines(pending_lines))
                pending_lines = []
            materialized.append(heading)
            continue
        pending_lines.append(line)
    if pending_lines:
        materialized.extend(_plain_blocks_from_lines(pending_lines))
    return materialized


def _heading_block(line: str) -> TextBlock | None:
    markdown = MARKDOWN_HEADING_RE.match(line)
    if markdown:
        return TextBlock(
            block_type="heading",
            text=str(markdown.group(2)).strip(),
            depth=len(markdown.group(1)),
            title=str(markdown.group(2)).strip(),
            lines=[line],
        )
    numbered = NUMBERED_HEADING_RE.match(line)
    if numbered and "." in str(numbered.group(1)) and len(str(numbered.group(2)).strip()) <= 120:
        return TextBlock(
            block_type="heading",
            text=str(numbered.group(2)).strip(),
            depth=str(numbered.group(1)).count(".") + 1,
            title=str(numbered.group(2)).strip(),
            lines=[line],
        )
    return None


def _plain_blocks_from_lines(lines: list[str]) -> list[TextBlock]:
    blocks: list[TextBlock] = []
    paragraph_lines: list[str] = []
    current_list_block: TextBlock | None = None

    def flush_list_block() -> None:
        nonlocal current_list_block
        if current_list_block is None:
            return
        text = "\n".join(line.rstrip() for line in current_list_block.lines if str(line).strip()).strip()
        if text:
            current_list_block.text = text
            current_list_block.metadata["continued_lines"] = max(0, len(current_list_block.lines) - 1)
            blocks.append(current_list_block)
        current_list_block = None

    def flush_paragraph() -> None:
        nonlocal paragraph_lines
        if paragraph_lines:
            paragraph_text = "\n".join(paragraph_lines).strip()
            if paragraph_text:
                blocks.append(TextBlock(block_type="paragraph", text=paragraph_text, lines=list(paragraph_lines)))
            paragraph_lines = []

    for line in lines:
        list_match = LIST_ITEM_RE.match(line)
        if list_match:
            flush_paragraph()
            flush_list_block()
            marker = str(list_match.group(1)).strip()
            item_text = str(list_match.group(2)).strip()
            if item_text:
                ordinal_text = marker[:-1] if marker[:-1].isdigit() else ""
                metadata = {
                    "list_marker": marker,
                    "list_kind": "ordered" if ordinal_text else "unordered",
                }
                if ordinal_text:
                    metadata["list_ordinal"] = int(ordinal_text)
                current_list_block = TextBlock(block_type="list_item", text=item_text, lines=[item_text], metadata=metadata)
            continue
        if current_list_block is not None:
            if line[:1].isspace():
                continuation = str(line).strip()
                if continuation:
                    current_list_block.lines.append(continuation)
                continue
            flush_list_block()
        paragraph_lines.append(line)
    flush_list_block()
    flush_paragraph()
    return blocks
