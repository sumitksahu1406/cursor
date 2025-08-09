from __future__ import annotations

import re
from typing import List

_WORD_RE = re.compile(r"\b[\w']+\b", re.UNICODE)


def word_count(text: str) -> int:
    return len(_WORD_RE.findall(text or ""))


def char_count(text: str, include_spaces: bool = True) -> int:
    if include_spaces:
        # Include spaces and tabs, but ignore newline characters for friendlier counts
        return sum(1 for c in text if c != "\n")
    return sum(1 for c in text if not c.isspace())


def sentence_count(text: str) -> int:
    # Split by sentence-ending punctuation; filter empty tokens
    parts = re.split(r"[.!?]+", text)
    return len([p for p in parts if p.strip()])


def find_replace(text: str, find: str, replace: str) -> str:
    return text.replace(find, replace)


def to_upper(text: str) -> str:
    return text.upper()


def to_lower(text: str) -> str:
    return text.lower()


def capitalize_sentences(text: str) -> str:
    # Keep delimiters while capitalizing the first character after them
    tokens = re.split(r"([.!?]\s*)", text)
    result_parts: List[str] = []
    for i in range(0, len(tokens), 2):
        sentence = tokens[i]
        delimiter = tokens[i + 1] if i + 1 < len(tokens) else ""
        sentence_stripped = sentence.lstrip()
        leading_ws_len = len(sentence) - len(sentence_stripped)
        leading_ws = sentence[:leading_ws_len]
        if sentence_stripped:
            sentence = leading_ws + sentence_stripped[0].upper() + sentence_stripped[1:]
        result_parts.append(sentence + delimiter)
    return "".join(result_parts)


def unique_words(text: str, lower: bool = True) -> List[str]:
    words = _WORD_RE.findall(text)
    if lower:
        words = [w.lower() for w in words]
    return sorted(set(words))


def reverse_words(text: str) -> str:
    words = re.findall(r"\S+", text)
    return " ".join(reversed(words))


def is_palindrome(text: str, ignore_case: bool = True, ignore_non_alnum: bool = True) -> bool:
    candidate = text
    if ignore_non_alnum:
        candidate = re.sub(r"[^0-9A-Za-z]+", "", candidate)
    if ignore_case:
        candidate = candidate.lower()
    return candidate == candidate[::-1]