from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import time
import json
import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib import parse, request
from urllib.error import HTTPError, URLError
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
import email.utils
from http.client import IncompleteRead

from .models import SourceEvent, SourceHealthCheck


class SourceAdapter(ABC):
    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        raise NotImplementedError

    def health_check(self) -> SourceHealthCheck:
        return SourceHealthCheck(source_name=self.name, status="healthy", detail="No health check implemented.")

    def child_adapters(self) -> list["SourceAdapter"]:
        return [self]

    def should_fetch_after_unhealthy_healthcheck(self) -> bool:
        return False

    def skips_pre_fetch_health_check(self) -> bool:
        return False


class CompositeSourceAdapter(SourceAdapter):
    def __init__(self, adapters: Iterable[SourceAdapter]) -> None:
        self.adapters = list(adapters)

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        for adapter in self.adapters:
            events.extend(adapter.fetch_since(ts))
        return sorted(events, key=lambda event: event.published_at)

    def child_adapters(self) -> list[SourceAdapter]:
        return list(self.adapters)


class JsonlReplaySourceAdapter(SourceAdapter):
    def __init__(self, path: Path) -> None:
        self.path = path

    @property
    def name(self) -> str:
        return f"jsonl_replay:{self.path.name}"

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        if not self.path.exists():
            return []
        events: list[SourceEvent] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                item = json.loads(line)
                published_at = datetime.fromisoformat(item["published_at"])
                if published_at <= ts:
                    continue
                events.append(
                    SourceEvent(
                        event_id=item.get("event_id", ""),
                        source=item.get("source", "replay"),
                        source_type=item.get("source_type", "news"),
                        symbol=item["symbol"],
                        headline=item["headline"],
                        summary=item.get("summary", item["headline"]),
                        published_at=published_at,
                        url=item.get("url", ""),
                        sentiment_hint=item.get("sentiment_hint"),
                        metadata=item.get("metadata", {}),
                        raw_payload=item,
                    )
                )
        return sorted(events, key=lambda event: event.published_at)

    def health_check(self) -> SourceHealthCheck:
        if self.path.exists():
            return SourceHealthCheck(source_name=self.name, status="healthy", detail="Replay file is available.")
        return SourceHealthCheck(source_name=self.name, status="unhealthy", detail="Replay file does not exist.")


class StaticSourceAdapter(SourceAdapter):
    def __init__(self, events: Iterable[SourceEvent]) -> None:
        self.events = list(events)

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        return [event for event in self.events if event.published_at > ts]

    def health_check(self) -> SourceHealthCheck:
        return SourceHealthCheck(
            source_name=self.name,
            status="healthy",
            detail=f"Static source loaded with {len(self.events)} event(s).",
        )


class SecFilingsSourceAdapter(SourceAdapter):
    ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}

    def __init__(self, symbols: Iterable[str], user_agent: str, forms: tuple[str, ...] = ("8-K", "10-Q", "10-K")) -> None:
        self.symbols = [symbol.upper() for symbol in symbols]
        self.user_agent = user_agent
        self.forms = forms
        self.base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        self.last_fetch_errors: list[dict[str, str]] = []

    @property
    def name(self) -> str:
        return "sec_edgar"

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        errors: list[dict[str, str]] = []
        with ThreadPoolExecutor(max_workers=min(len(self.symbols), 8) or 1) as executor:
            futures = {
                executor.submit(self._fetch_symbol_feed, symbol, ts): symbol
                for symbol in self.symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    events.extend(future.result())
                except Exception as exc:
                    errors.append(
                        {
                            "symbol": symbol,
                            "error": exc.__class__.__name__,
                            "detail": str(exc),
                        }
                    )
                    continue
        self.last_fetch_errors = errors[:20]
        return sorted(events, key=lambda event: event.published_at)

    def health_check(self) -> SourceHealthCheck:
        if not self.symbols:
            return SourceHealthCheck(source_name=self.name, status="unhealthy", detail="No symbols configured.")
        symbol = self.symbols[0]
        started = time.monotonic()
        try:
            req = request.Request(
                f"{self.base_url}?{parse.urlencode({'action': 'getcompany', 'CIK': symbol, 'owner': 'exclude', 'count': '5', 'output': 'atom'})}",
                headers={"User-Agent": self.user_agent, "Accept-Encoding": "identity"},
            )
            with request.urlopen(req, timeout=10) as response:
                payload = response.read().decode("utf-8")
            ET.fromstring(payload)
            latency_ms = int((time.monotonic() - started) * 1000)
            return SourceHealthCheck(
                source_name=self.name,
                status="healthy",
                detail=f"SEC feed reachable for {symbol}.",
                latency_ms=latency_ms,
            )
        except Exception as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            return SourceHealthCheck(
                source_name=self.name,
                status="unhealthy",
                detail=f"{exc.__class__.__name__}: {exc}",
                latency_ms=latency_ms,
            )

    def _fetch_symbol_feed(self, symbol: str, ts: datetime) -> list[SourceEvent]:
        query = parse.urlencode(
            {
                "action": "getcompany",
                "CIK": symbol,
                "owner": "exclude",
                "count": "20",
                "output": "atom",
            }
        )
        req = request.Request(
            f"{self.base_url}?{query}",
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "identity",
            },
        )
        with request.urlopen(req, timeout=20) as response:
            payload = response.read().decode("utf-8")
        try:
            root = ET.fromstring(payload)
        except ParseError as exc:
            preview = payload.strip().replace("\n", " ")[:120]
            raise ValueError(f"Malformed SEC payload for {symbol}: {preview}") from exc
        events: list[SourceEvent] = []
        for entry in root.findall("atom:entry", self.ATOM_NS):
            updated_raw = self._entry_text(entry, "atom:updated")
            if not updated_raw:
                continue
            published_at = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
            if published_at <= ts:
                continue
            title = self._entry_text(entry, "atom:title")
            summary = self._entry_text(entry, "atom:summary") or title
            form = self._entry_form(title, summary)
            if form not in self.forms:
                continue
            link = ""
            link_node = entry.find("atom:link", self.ATOM_NS)
            if link_node is not None:
                link = link_node.attrib.get("href", "")
            accession = self._entry_text(entry, "atom:id") or link or f"{symbol}:{form}:{updated_raw}"
            events.append(
                SourceEvent(
                    event_id=accession,
                    source="SEC Edgar",
                    source_type="filing",
                    symbol=symbol,
                    headline=title or f"{symbol} filed {form}",
                    summary=summary,
                    published_at=published_at,
                    url=link,
                    metadata={"form": form},
                    raw_payload={"feed": "sec_atom", "symbol": symbol, "form": form},
                )
            )
        return events

    def _entry_form(self, title: str, summary: str) -> str:
        candidates = [title or "", summary or ""]
        for candidate in candidates:
            prefix = candidate.split(" - ", 1)[0].strip().upper()
            if prefix in self.forms:
                return prefix
        return ""

    def _entry_text(self, entry: ET.Element, selector: str) -> str:
        node = entry.find(selector, self.ATOM_NS)
        return (node.text or "").strip() if node is not None and node.text else ""


class GenericFeedSourceAdapter(SourceAdapter):
    ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}

    def __init__(
        self,
        symbols: Iterable[str],
        url_builder,
        source_name: str,
        source_type: str,
        user_agent: str = "satellite-agent/0.1",
        request_timeout_seconds: int = 12,
        health_timeout_seconds: int = 6,
        max_retries: int = 2,
        max_workers: int = 6,
    ) -> None:
        self.symbols = [symbol.upper() for symbol in symbols]
        self.url_builder = url_builder
        self.source_name = source_name
        self.source_type = source_type
        self.user_agent = user_agent
        self.request_timeout_seconds = request_timeout_seconds
        self.health_timeout_seconds = health_timeout_seconds
        self.max_retries = max(max_retries, 1)
        self.max_workers = max(max_workers, 1)
        self.last_fetch_errors: list[dict[str, str]] = []
        self.max_events_per_symbol = 0

    @property
    def name(self) -> str:
        return self.source_name.lower().replace(" ", "_")

    def should_fetch_after_unhealthy_healthcheck(self) -> bool:
        # Feed-based sources occasionally fail health checks with transient SSL/EOF
        # errors even though a subsequent full fetch succeeds on retry.
        return True

    def skips_pre_fetch_health_check(self) -> bool:
        # For feed-based sources, the actual fetch is a better probe than an extra
        # health-check request and saves one network round-trip per run.
        return True

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        errors: list[dict[str, str]] = []
        with ThreadPoolExecutor(max_workers=min(len(self.symbols), self.max_workers) or 1) as executor:
            futures = {
                executor.submit(self._fetch_symbol_feed, symbol, ts): symbol
                for symbol in self.symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    events.extend(future.result())
                except Exception as exc:
                    errors.append(
                        {
                            "symbol": symbol,
                            "error": exc.__class__.__name__,
                            "detail": str(exc),
                        }
                    )
                    continue
        self.last_fetch_errors = errors[:20]
        return sorted(events, key=lambda event: event.published_at)

    def health_check(self) -> SourceHealthCheck:
        if not self.symbols:
            return SourceHealthCheck(source_name=self.name, status="unhealthy", detail="No symbols configured.")
        started = time.monotonic()
        symbol = self.symbols[0]
        try:
            payload = self._read_url(
                self.url_builder(symbol),
                timeout=self.health_timeout_seconds,
                retries=1,
            )
            root = ET.fromstring(payload)
            tag = self._strip_namespace(root.tag).lower()
            if tag not in {"feed", "rss"}:
                raise ValueError("Unsupported feed type")
            latency_ms = int((time.monotonic() - started) * 1000)
            return SourceHealthCheck(
                source_name=self.name,
                status="healthy",
                detail=f"Feed reachable for {symbol}.",
                latency_ms=latency_ms,
            )
        except Exception as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            return SourceHealthCheck(
                source_name=self.name,
                status="unhealthy",
                detail=f"{exc.__class__.__name__}: {exc}",
                latency_ms=latency_ms,
            )

    def _fetch_symbol_feed(self, symbol: str, ts: datetime) -> list[SourceEvent]:
        payload = self._read_url(
            self.url_builder(symbol),
            timeout=self.request_timeout_seconds,
            retries=self.max_retries,
        )
        root = ET.fromstring(payload)
        tag = self._strip_namespace(root.tag).lower()
        if tag == "feed":
            return self._parse_atom(root, symbol, ts)
        if tag == "rss":
            return self._parse_rss(root, symbol, ts)
        raise ValueError("Unsupported feed type")

    def _read_url(self, url: str, *, timeout: int, retries: int) -> bytes:
        last_error: Exception | None = None
        for attempt in range(max(retries, 1)):
            req = request.Request(
                url,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept-Encoding": "identity",
                    "Connection": "close",
                },
            )
            try:
                with request.urlopen(req, timeout=timeout) as response:
                    return response.read()
            except (URLError, HTTPError, IncompleteRead, TimeoutError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                time.sleep(0.5 * (attempt + 1))
        if last_error is not None:
            raise last_error
        raise RuntimeError("Feed fetch failed without a captured error.")

    def _parse_atom(self, root: ET.Element, symbol: str, ts: datetime) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        max_events = max(getattr(self, "max_events_per_symbol", 0), 0)
        for entry in root.findall("atom:entry", self.ATOM_NS):
            published_at = self._parse_date(
                self._atom_text(entry, "atom:updated") or self._atom_text(entry, "atom:published")
            )
            if published_at is None or published_at <= ts:
                continue
            title = self._atom_text(entry, "atom:title")
            summary = self._atom_text(entry, "atom:summary") or title
            link = ""
            link_node = entry.find("atom:link", self.ATOM_NS)
            if link_node is not None:
                link = link_node.attrib.get("href", "")
            identifier = self._atom_text(entry, "atom:id") or link or f"{symbol}:{published_at.isoformat()}:{title}"
            events.append(
                SourceEvent(
                    event_id=identifier,
                    source=self.source_name,
                    source_type=self.source_type,
                    symbol=symbol,
                    headline=title,
                    summary=summary,
                    published_at=published_at,
                    url=link,
                    metadata={"feed_symbol": symbol},
                    raw_payload={"feed": self.source_name.lower().replace(" ", "_")},
                )
            )
            if max_events and len(events) >= max_events:
                break
        return events

    def _parse_rss(self, root: ET.Element, symbol: str, ts: datetime) -> list[SourceEvent]:
        channel = root.find("channel")
        if channel is None:
            return []
        events: list[SourceEvent] = []
        max_events = max(getattr(self, "max_events_per_symbol", 0), 0)
        for item in channel.findall("item"):
            published_at = self._parse_date(self._rss_text(item, "pubDate"))
            if published_at is None or published_at <= ts:
                continue
            title = self._rss_text(item, "title")
            summary = self._rss_text(item, "description") or title
            link = self._rss_text(item, "link")
            guid = self._rss_text(item, "guid") or link or f"{symbol}:{published_at.isoformat()}:{title}"
            events.append(
                SourceEvent(
                    event_id=guid,
                    source=self.source_name,
                    source_type=self.source_type,
                    symbol=symbol,
                    headline=title,
                    summary=summary,
                    published_at=published_at,
                    url=link,
                    metadata={"feed_symbol": symbol},
                    raw_payload={"feed": self.source_name.lower().replace(" ", "_")},
                )
            )
            if max_events and len(events) >= max_events:
                break
        return events

    def _parse_date(self, raw: str) -> datetime | None:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            parsed = email.utils.parsedate_to_datetime(raw)
            if parsed is None:
                return None
            return parsed

    def _atom_text(self, entry: ET.Element, selector: str) -> str:
        node = entry.find(selector, self.ATOM_NS)
        return (node.text or "").strip() if node is not None and node.text else ""

    def _rss_text(self, entry: ET.Element, selector: str) -> str:
        node = entry.find(selector)
        return (node.text or "").strip() if node is not None and node.text else ""

    def _strip_namespace(self, tag: str) -> str:
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag


class GoogleNewsSourceAdapter(GenericFeedSourceAdapter):
    MAX_RESULTS_PER_SYMBOL = 6
    LOW_SIGNAL_SUBSTRINGS = (
        "stock price today",
        "stock split history",
        "most profitable companies",
        "battle royale:",
        "how do you like them apples",
        "stocks to buy",
        "best stocks",
        "buy before",
        "buy now",
        "sell before",
        "price prediction",
        "stock forecast",
        "stock analysis",
        "market cap",
        "all-time high",
        "all time high",
        "52-week high",
        "52 week high",
        "52-week low",
        "52 week low",
        "dividend yield",
        "chart analysis",
        "technical analysis",
    )
    LOW_SIGNAL_PATTERNS = (
        re.compile(r"^top\s+\d+\b", re.IGNORECASE),
        re.compile(r"^should\s+.+\binvestors\b", re.IGNORECASE),
        re.compile(r"^\d+\s+dirt\s+cheap\b", re.IGNORECASE),
        re.compile(r"^(is|are)\s+.+\s+(a|an)\s+buy\b", re.IGNORECASE),
        re.compile(r"^(why|when)\s+.+\s+stock\b", re.IGNORECASE),
        re.compile(r"\b(vs\.?|versus)\b", re.IGNORECASE),
    )
    SYMBOL_ALIASES = {
        "AAPL": ("apple",),
        "ADBE": ("adobe",),
        "ABNB": ("airbnb",),
        "ADP": ("automatic data processing", "adp"),
        "AMD": ("advanced micro devices", "amd"),
        "AMAT": ("applied materials",),
        "AMZN": ("amazon",),
        "ARM": ("arm holdings",),
        "ASML": ("asml", "asml holding"),
        "AVGO": ("broadcom",),
        "AXP": ("american express", "amex"),
        "BLK": ("blackrock",),
        "CAT": ("caterpillar",),
        "CL": ("colgate", "colgate-palmolive", "colgate palmolive"),
        "COST": ("costco",),
        "CRM": ("salesforce",),
        "DE": ("deere", "john deere"),
        "DELL": ("dell", "dell technologies"),
        "DHR": ("danaher",),
        "DIS": ("disney", "walt disney"),
        "DOCU": ("docusign",),
        "DDOG": ("datadog",),
        "ELV": ("elevance", "elevance health"),
        "GE": ("ge aerospace", "general electric"),
        "GLD": ("spdr gold shares", "gold etf"),
        "GS": ("goldman sachs",),
        "HOOD": ("robinhood",),
        "INTC": ("intel",),
        "JPM": ("jpmorgan", "jp morgan", "chase"),
        "KO": ("coca-cola", "coca cola"),
        "LLY": ("eli lilly", "lilly"),
        "LOW": ("lowe's", "lowes"),
        "MA": ("mastercard",),
        "MCD": ("mcdonald's", "mcdonalds"),
        "MDB": ("mongodb",),
        "MELI": ("mercadolibre", "mercado libre"),
        "META": ("meta", "facebook"),
        "MRK": ("merck",),
        "MS": ("morgan stanley",),
        "NET": ("cloudflare",),
        "NKE": ("nike",),
        "NFLX": ("netflix",),
        "NOW": ("servicenow",),
        "NVDA": ("nvidia",),
        "ORCL": ("oracle",),
        "PEP": ("pepsico", "pepsi"),
        "PG": ("procter & gamble", "procter and gamble", "p&g"),
        "QQQ": ("invesco qqq", "nasdaq 100"),
        "QCOM": ("qualcomm",),
        "ROKU": ("roku",),
        "SHOP": ("shopify",),
        "SBUX": ("starbucks",),
        "SNOW": ("snowflake",),
        "SOFI": ("sofi", "social finance"),
        "SQ": ("block", "square"),
        "TSM": ("taiwan semiconductor", "tsmc"),
        "TEAM": ("atlassian",),
        "UBER": ("uber",),
        "UBS": ("ubs",),
        "V": ("visa",),
        "XOM": ("exxon", "exxon mobil"),
        "XLK": ("technology select sector",),
        "ZS": ("zscaler",),
    }

    def __init__(self, symbols: Iterable[str], mode: str = "news") -> None:
        source_type = "research" if mode == "research" else "news"
        source_name = "Google News Research" if mode == "research" else "Google News"

        def build_url(symbol: str) -> str:
            query = self._query_for(symbol, mode)
            return (
                "https://news.google.com/rss/search?"
                + parse.urlencode({"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"})
            )

        super().__init__(
            symbols=symbols,
            url_builder=build_url,
            source_name=source_name,
            source_type=source_type,
        )
        self.max_events_per_symbol = self.MAX_RESULTS_PER_SYMBOL

    def _fetch_symbol_feed(self, symbol: str, ts: datetime) -> list[SourceEvent]:
        events = super()._fetch_symbol_feed(symbol, ts)
        return [event for event in events if self._is_relevant_result(symbol, event)]

    def _query_for(self, symbol: str, mode: str) -> str:
        aliases = self.SYMBOL_ALIASES.get(symbol, ())
        if aliases:
            symbol_query = " OR ".join([f'"{symbol}"', *[f'"{alias}"' for alias in aliases]])
            symbol_query = f"({symbol_query})"
        else:
            symbol_query = symbol
        if mode == "research":
            return f'{symbol_query} stock analyst OR downgrade OR upgrade OR "price target" when:1d'
        return (
            f"{symbol_query} "
            '(earnings OR filing OR guidance OR launch OR approval OR partnership OR acquisition OR outlook) '
            "when:1d"
        )

    def _is_relevant_result(self, symbol: str, event: SourceEvent) -> bool:
        if self._is_low_signal_result(event):
            return False
        aliases = self.SYMBOL_ALIASES.get(symbol, ())
        text = self._relevance_text(event)
        if self._contains_symbol_token(text, symbol):
            return True
        if any(alias.lower() in text for alias in aliases):
            return True
        # Precision-first for short tickers: if neither ticker token nor alias appears,
        # the result is more likely to be query spillover than a true symbol match.
        if symbol.isalpha() and len(symbol) <= 5:
            return False
        return True

    def _contains_symbol_token(self, text: str, symbol: str) -> bool:
        tokens = {
            token.strip(".,:;!?()[]{}<>/\\'\"")
            for token in text.replace("-", " ").split()
        }
        return symbol.lower() in tokens

    def _relevance_text(self, event: SourceEvent) -> str:
        return self._normalized_text(f"{event.headline} {event.summary}")

    def _normalized_text(self, value: str) -> str:
        text = html.unescape(value or "")
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(text.lower().split())

    def _is_low_signal_result(self, event: SourceEvent) -> bool:
        title = self._normalized_text(event.headline)
        if any(snippet in title for snippet in self.LOW_SIGNAL_SUBSTRINGS):
            return True
        return any(pattern.search(title) for pattern in self.LOW_SIGNAL_PATTERNS)
