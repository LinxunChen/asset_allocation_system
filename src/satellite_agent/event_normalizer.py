from __future__ import annotations

import html
import hashlib
import re
from typing import Optional

from .models import SourceEvent, ensure_utc


SPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
HTML_TAG_RE = re.compile(r"<[^>]+>")
STOP_WORDS = {
    "the", "and", "for", "with", "after", "into", "from", "over", "under", "says", "say",
    "stock", "shares", "company", "corp", "inc", "plc", "ltd", "co", "reports", "report",
    "files", "filed", "will", "would", "could", "should", "amid", "near", "into", "about",
}


class EventNormalizer:
    def normalize(self, event: SourceEvent) -> SourceEvent:
        symbol = event.symbol.strip().upper()
        headline = self._clean_text(event.headline)
        summary = self._clean_text(event.summary)
        event_id = event.event_id or self.build_event_id(symbol, headline, event.source, event.url)
        event_fingerprint = event.event_fingerprint or self.build_event_fingerprint(symbol, headline, summary)
        return SourceEvent(
            event_id=event_id,
            source=event.source.strip() or "unknown",
            source_type=event.source_type.strip() or "news",
            symbol=symbol,
            headline=headline,
            summary=summary,
            published_at=ensure_utc(event.published_at),
            url=event.url.strip(),
            sentiment_hint=event.sentiment_hint,
            event_fingerprint=event_fingerprint,
            metadata=event.metadata,
            raw_payload=event.raw_payload,
        )

    def build_event_id(self, symbol: str, headline: str, source: str, url: str) -> str:
        normalized = "|".join(
            [
                symbol.strip().upper(),
                self._clean_text(headline).lower(),
                source.strip().lower(),
                url.strip().lower(),
            ]
        )
        return hashlib.sha1(normalized.encode("utf-8")).hexdigest()

    def build_event_fingerprint(self, symbol: str, headline: str, summary: str) -> str:
        tokens = self._fingerprint_tokens(headline, symbol)
        if len(tokens) < 5:
            tokens.extend(self._fingerprint_tokens(summary[:140], symbol, existing=tokens))
        basis = "|".join([symbol.strip().upper()] + tokens)
        return hashlib.sha1(basis.encode("utf-8")).hexdigest()

    def _fingerprint_tokens(self, value: str, symbol: str, existing: Optional[list[str]] = None) -> list[str]:
        cleaned = NON_ALNUM_RE.sub(" ", value.lower())
        tokens = list(existing or [])
        for token in cleaned.split():
            if len(token) <= 2 or token in STOP_WORDS or token == symbol.lower() or token in tokens:
                continue
            tokens.append(token)
            if len(tokens) == 10:
                break
        return tokens

    def _clean_text(self, value: Optional[str]) -> str:
        if not value:
            return ""
        text = html.unescape(value)
        text = HTML_TAG_RE.sub(" ", text)
        return SPACE_RE.sub(" ", text).strip()
