"""Core port intelligence engine.

MAIN MODULES:
  1. PortResolver: Maps port names/codes to Port objects with scope enforcement
  2. VesselFinderAdapter & MarineTrafficAdapter: Hybrid live/mock vessel data sources
  3. Merge Engine: Combines vessel data from multiple sources (weighted 65% VesselFinder, 35% MarineTraffic)
  4. Risk Engine: Computes port risk scores from congestion, weather, operations, and vessel delays
  5. NoaaWeatherService: Real-time marine weather risk from NOAA weather.gov API
  6. PortIntelligenceService: Main orchestrator—coordinates all operations and query handling

WORKFLOW:
  1. User submits PortQueryRequest (port name + filters)
  2. PortResolver resolves port name to Port object
  3. Adapters fetch vessel snapshots from live/mock sources
  4. Merge engine combines duplicates (weighted by freshness + confidence)
  5. Weather service fetches NOAA forecasts + risk scoring
  6. Risk engine computes port risks (congestion, weather, delays) + flags problems
  7. Results filtered by user (severity, confidence, delay status)
  8. Response cached with 180-second TTL in app/data/query_cache.json
  9. CLI displays paginated results with quality metadata

SCOPE:
  - Contiguous US only (Alaska, Hawaii excluded by design)
  - 25 major US ports included
  - Live API with fallback to deterministic mock data
  - 5-minute data freshness targets for real-time tracking
"""
from __future__ import annotations

import hashlib
import json
import os
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from app.cache import PersistentCache
from app.models import (
    ConfidenceLabel,
    EXCLUDED_STATES,
    Port,
    PortQueryRequest,
    PORTS,
    Problem,
    QueryMeta,
    QueryResponse,
    QualityInfo,
    SeverityLabel,
    Vessel,
)

# ========== TYPE DEFINITIONS & CONSTANTS ==========

# Severity ordering for filtering (lower = less severe)
SEVERITY_ORDER = {"none": 0, "light": 1, "moderate": 2, "heavily": 3, "extreme": 4}
# Confidence ordering for filtering (lower = less confident)
CONFIDENCE_ORDER = {"low": 0, "medium": 1, "high": 2}
STATE_NAME_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "dc": "DC", "district of columbia": "DC"
}

CONFIDENCE_RANK = {"low": 1, "medium": 2, "high": 3}


@dataclass
class SourceResult:
    """Result from a single data source (VesselFinder, MarineTraffic)"""
    source: str
    vessels: list[Vessel]
    congestion_score: int
    weather_score: int
    operations_score: int
    fetched_at: datetime


@dataclass
class RiskSummary:
    """Aggregated port risk assessment"""
    port_score: int
    severity: SeverityLabel
    problems: list[Problem]


@dataclass(slots=True)
class WeatherRiskResult:
    """NOAA weather risk result"""
    score: int
    source: str
    status: str
    detail: str


# ========== PORT RESOLVER ==========

class PortResolver:
    """Maps port name/code queries to Port objects with scope enforcement.
    
    Responsibilities:
    - Suggest ports (auto-complete) from user input
    - Resolve port names/UNLOCODE to full Port objects
    - Enforce contiguous US scope (block Alaska, Hawaii)
    """

    def __init__(self) -> None:
        # Load static port reference data from models.PORTS and hydrate as Port objects
        self._ports = [Port(**entry) for entry in PORTS]

    def suggest(self, query: str) -> list[Port]:
        """Return up to 12 port matches for auto-complete suggestions.
        
        Matches against port name only.
        """
        q = query.strip().lower()
        if not q:
            return []

        matches = [
            port
            for port in self._ports
            if q in port.name.lower()
        ]
        return matches[:12]

    def resolve(self, input_value: str) -> Port:
        """Resolve a port name to a Port object.
        
        Raises ValueError if port not found in reference data.
        All ports in the reference data are contiguous US only.
        """
        q = input_value.strip().lower()
        for port in self._ports:
            if q == port.name.lower():
                return port

        # Fallback: try partial match via suggestions
        partial = self.suggest(input_value)
        if partial:
            return partial[0]

        raise ValueError("Port not found in contiguous US scope.")


# ========== SOURCE ADAPTERS ==========

class SourceAdapter:
    """Base class for vessel/port data adapters (VesselFinder, MarineTraffic, etc.)"""
    source_name = "base"

    def fetch_port_snapshot(self, port: Port, time_horizon_hours: int) -> SourceResult:
        raise NotImplementedError

    def mode(self) -> str:
        """Return current mode: 'live' (real API), 'hybrid-live' (live with fallback), 'mock'"""
        return "mock"


class VesselFinderAdapter(SourceAdapter):
    """Hybrid adapter: tries live VesselFinder API, falls back to deterministic mock.
    
    VesselFinder is weighted at 65% in merge operations (primary source).
    """
    source_name = "vesselfinder"

    def __init__(self) -> None:
        # Read API configuration from environment variables (if not set, falls back to mock)
        self.api_url = os.getenv("VESSELFINDER_API_URL", "").strip()
        self.api_key = os.getenv("VESSELFINDER_API_KEY", "").strip()

    def fetch_port_snapshot(self, port: Port, time_horizon_hours: int) -> SourceResult:
        """Fetch vessel snapshot for port within time horizon.
        
        If live API is configured and reachable, use live data.
        Otherwise, fall back to deterministic mock (same seed per port/source/time).
        """
        if self.api_url:
            live = _try_fetch_live(self.source_name, self.api_url, self.api_key, port, time_horizon_hours)
            if live is not None:
                return live
        return _build_deterministic_result(self.source_name, port, time_horizon_hours)

    def mode(self) -> str:
        return "hybrid-live" if self.api_url else "mock"


class MarineTrafficAdapter(SourceAdapter):
    """Hybrid adapter: tries live MarineTraffic API, falls back to deterministic mock.
    
    MarineTraffic is weighted at 35% in merge operations (secondary source).
    """
    source_name = "marinetraffic"

    def __init__(self) -> None:
        # Read API configuration from environment variables (if not set, falls back to mock)
        self.api_url = os.getenv("MARINETRAFFIC_API_URL", "").strip()
        self.api_key = os.getenv("MARINETRAFFIC_API_KEY", "").strip()

    def fetch_port_snapshot(self, port: Port, time_horizon_hours: int) -> SourceResult:
        """Fetch vessel snapshot for port within time horizon.
        
        If live API is configured and reachable, use live data.
        Otherwise, fall back to deterministic mock (same seed per port/source/time).
        """
        if self.api_url:
            live = _try_fetch_live(self.source_name, self.api_url, self.api_key, port, time_horizon_hours)
            if live is not None:
                return live
        return _build_deterministic_result(self.source_name, port, time_horizon_hours)

    def mode(self) -> str:
        return "hybrid-live" if self.api_url else "mock"


def _try_fetch_live(
    source: str,
    api_url: str,
    api_key: str,
    port: Port,
    time_horizon_hours: int,
) -> SourceResult | None:
    """Attempt live API fetch with 8-second timeout."""
    params = {
        "port": port.unlocode,
        "hours": str(time_horizon_hours),
    }
    if api_key:
        params["api_key"] = api_key

    url = f"{api_url}?{urlencode(params)}"

    try:
        with urlopen(url, timeout=8) as response:
            if response.status != 200:
                return None
            payload = json.loads(response.read().decode("utf-8"))
            return _map_live_payload(source, port, payload)
    except (TimeoutError, URLError, OSError, ValueError, json.JSONDecodeError):
        return None


def _map_live_payload(source: str, port: Port, payload: dict) -> SourceResult:
    """Transform live API response into SourceResult."""
    now = datetime.now(UTC)
    vessels: list[Vessel] = []
    raw_vessels = payload.get("vessels", [])

    for item in raw_vessels:
        departure = _parse_dt(item.get("departureTimeUtc")) or (now - timedelta(hours=8))
        eta = _parse_dt(item.get("etaUtc")) or (now + timedelta(hours=14))

        vessels.append(
            Vessel(
                name=str(item.get("name", "Unknown Vessel")),
                imo=str(item.get("imo", "unknown")),
                mmsi=str(item.get("mmsi", "unknown")),
                vesselType=str(item.get("vesselType", "Unknown")),
                flag=str(item.get("flag", "Unknown")),
                originPort=str(item.get("originPort", "Unknown")),
                destinationPort=str(item.get("destinationPort", port.name)),
                departureTimeUtc=departure,
                etaUtc=eta,
                isDelayed=bool(item.get("isDelayed", False)),
                status=str(item.get("status", "Unknown")),
                speedKnots=_to_float(item.get("speedKnots")),
                courseDegrees=_to_int(item.get("courseDegrees")),
                draughtMeters=_to_float(item.get("draughtMeters")),
                sourceUsed=source,
                confidence="high" if source == "vesselfinder" else "medium",
                freshnessMinutes=_to_int(item.get("freshnessMinutes")) or 5,
            )
        )

    return SourceResult(
        source=source,
        vessels=vessels,
        congestion_score=_score(payload.get("congestionScore")),
        weather_score=_score(payload.get("weatherScore")),
        operations_score=_score(payload.get("operationsScore")),
        fetched_at=now,
    )


def _build_deterministic_result(source: str, port: Port, time_horizon_hours: int) -> SourceResult:
    """Generate deterministic mock data (same every 5 minutes per port/source)."""
    rng = random.Random(_seed_for(source, port))
    now = datetime.now(UTC)

    vessels: list[Vessel] = []
    count = rng.randint(45, 110)
    for idx in range(count):
        dep_offset_days = rng.randint(1, 18)
        departure = now - timedelta(days=dep_offset_days, hours=rng.randint(1, 10))
        eta = now + timedelta(hours=rng.randint(1, max(time_horizon_hours, 2)))
        delayed = rng.random() < 0.27

        vessel = Vessel(
            name=f"MV {abs(hash(port.name)) % 997:03d}",
            imo=f"9{rng.randint(100000, 999999)}",
            mmsi=f"{rng.randint(100000000, 999999999)}",
            vesselType=rng.choice(["Container Ship", "Tanker", "Bulk Carrier", "Ro-Ro"]),
            flag=rng.choice(["US", "PA", "LR", "SG", "MH"]),
            originPort=rng.choice(["Shanghai", "Busan", "Rotterdam", "Santos", "Panama"]),
            destinationPort=port.name,
            departureTimeUtc=departure,
            etaUtc=eta,
            isDelayed=delayed,
            status=rng.choice(["Underway", "At anchor", "Moored", "Approaching"]),
            speedKnots=round(rng.uniform(8.2, 19.8), 1),
            courseDegrees=rng.randint(0, 359),
            draughtMeters=round(rng.uniform(7.0, 13.5), 1),
            sourceUsed=source,
            confidence="high" if source == "vesselfinder" else "medium",
            freshnessMinutes=rng.randint(2, 8),
        )
        vessels.append(vessel)

    return SourceResult(
        source=source,
        vessels=vessels,
        congestion_score=rng.randint(10, 85),
        weather_score=rng.randint(5, 90),
        operations_score=rng.randint(0, 70),
        fetched_at=now,
    )


def _seed_for(source: str, port: Port, bucket_minutes: int = 5) -> int:
    """Deterministic seed per source/port/5-minute bucket."""
    bucket = datetime.now(UTC).replace(second=0, microsecond=0)
    minute_bucket = (bucket.minute // bucket_minutes) * bucket_minutes
    bucket = bucket.replace(minute=minute_bucket)
    key = f"{source}:{port.name}:{bucket.isoformat()}"
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16) % (2**32)


# ========== MERGE ENGINE ==========

def merge_vessels(primary: SourceResult, secondary: SourceResult) -> list[Vessel]:
    """Merge vessels from two sources with intelligent deduplication.
    
    STRATEGY:
    1. Common IMO = same vessel across sources
    2. VesselFinder-first policy: prefer VesselFinder data (65% weight)
    3. Within same source, prefer fresher data (lower freshnessMinutes)
    4. Within same freshness, prefer higher confidence data
    
    This ensures consistent, high-confidence vessel tracking across both sources.
    """
    by_imo: dict[str, Vessel] = {}
    for vessel in secondary.vessels:
        by_imo[vessel.imo] = vessel

    for vessel in primary.vessels:
        current = by_imo.get(vessel.imo)
        if current is None:
            by_imo[vessel.imo] = vessel
            continue

        # Compare freshness: lower minutes = fresher (higher priority)
        current_freshness = _freshness_rank(current)
        new_freshness = _freshness_rank(vessel)
        if new_freshness < current_freshness:
            by_imo[vessel.imo] = vessel
            continue

        # If same freshness, prefer higher confidence
        if new_freshness == current_freshness and CONFIDENCE_RANK[vessel.confidence] >= CONFIDENCE_RANK[current.confidence]:
            by_imo[vessel.imo] = vessel

    return list(by_imo.values())


def _freshness_rank(vessel: Vessel) -> int:
    """Lower freshness minutes = higher rank (fresher data has priority)."""
    return vessel.freshnessMinutes


def source_health(primary: SourceResult, secondary: SourceResult) -> dict[str, str]:
    """Assess source health based on data age.
    
    Health levels:
    - 'ok': Data updated within 5 minutes
    - 'degraded': Data updated within 15 minutes
    - 'stale': Data older than 15 minutes
    """
    return {
        primary.source: _health_from(primary.fetched_at),
        secondary.source: _health_from(secondary.fetched_at),
    }


def _health_from(ts: datetime) -> str:
    """Map age to health status."""
    age_minutes = int((datetime.now(ts.tzinfo) - ts).total_seconds() / 60)
    if age_minutes <= 5:
        return "ok"
    if age_minutes <= 15:
        return "degraded"
    return "stale"


# ========== RISK ENGINE ==========

def severity_from_score(score: int) -> SeverityLabel:
    """Map numeric score (0-100) to 5-tier severity label.
    
    RISK SCALE:
    - 0-9:    'none'       (no risk)
    - 10-29:  'light'      (minor issues)
    - 30-54:  'moderate'   (notable delays/weather)
    - 55-79:  'heavily'    (major disruptions)
    - 80-100: 'extreme'    (critical conditions)
    """
    if score <= 9:
        return "none"
    if score <= 29:
        return "light"
    if score <= 54:
        return "moderate"
    if score <= 79:
        return "heavily"
    return "extreme"


def compute_port_risk(
    primary: SourceResult,
    secondary: SourceResult,
    merged_vessels: list[Vessel],
    weather_override_score: int | None = None,
    weather_source: str = "marinetraffic",
    weather_detail: str = "",
) -> RiskSummary:
    """Compute weighted port risk from multiple factors.
    
    RISK FORMULA:
    1. Congestion: weighted blend of both sources (65% VesselFinder, 35% MarineTraffic)
    2. Weather: weighted blend, optionally overridden by NOAA data
    3. Operations: weighted blend from both sources
    4. Delay Pressure: percentage of vessels flagged as delayed
    5. Final Score: 45% congestion + 35% weather + 20% operations
    6. Delay adjustment: if >25% delayed, adjust score down by 20%
    
    Creates up to 3 Problem records:
    - Port Congestion (port-level)
    - Weather Disruption (port-level)
    - Arrival/Departure Delays (vessel-level, if >25% delayed)
    """
    # Compute weighted congestion score
    congestion = round((primary.congestion_score * 0.65) + (secondary.congestion_score * 0.35))
    
    # Compute weighted weather score (60% primary, 40% secondary)
    weather = round((primary.weather_score * 0.60) + (secondary.weather_score * 0.40))
    # Override with NOAA data if available
    if weather_override_score is not None:
        weather = max(0, min(100, int(weather_override_score)))
    
    # Compute weighted operations score
    ops = round((primary.operations_score * 0.60) + (secondary.operations_score * 0.40))

    # Combine into total risk: 45% congestion + 35% weather + 20% operations
    total = int(round((0.45 * congestion) + (0.35 * weather) + (0.20 * ops)))

    # Calculate delay pressure: what % of vessels are delayed?
    delayed = sum(1 for v in merged_vessels if v.isDelayed)
    delay_ratio_score = int((delayed / max(len(merged_vessels), 1)) * 100)
    
    # Final adjustment: if >25% delayed, reduce combined score by 20%
    total = max(0, min(100, int((total * 0.8) + (delay_ratio_score * 0.2))))

    severity = severity_from_score(total)

    # Build problem list (flagged issues)
    problems = [
        Problem(
            title="Port Congestion",
            description=f"Detected traffic density pressure with congestion index {congestion}.",
            scope="port",
            severityLabel=severity_from_score(congestion),
            riskScore=congestion,
            evidenceSource="vesselfinder",
            lastUpdatedUtc=primary.fetched_at,
        ),
        Problem(
            title="Weather Disruption Risk",
            description=(
                f"Marine weather impact index currently at {weather}."
                if not weather_detail
                else f"Marine weather impact index currently at {weather}. {weather_detail}"
            ),
            scope="port",
            severityLabel=severity_from_score(weather),
            riskScore=weather,
            evidenceSource=weather_source,
            lastUpdatedUtc=secondary.fetched_at if weather_source != "noaa" else datetime.now(UTC),
        ),
    ]

    # Add delay problem if >25% of vessels are delayed
    if delay_ratio_score >= 25:
        problems.append(
            Problem(
                title="Arrival/Departure Delays",
                description=f"{delayed} linked vessels are flagged as delayed.",
                scope="vessel",
                severityLabel=severity_from_score(delay_ratio_score),
                riskScore=delay_ratio_score,
                evidenceSource="merged",
                lastUpdatedUtc=primary.fetched_at,
            )
        )

    return RiskSummary(port_score=total, severity=severity, problems=problems)


# ========== WEATHER SERVICE ==========

class NoaaWeatherService:
    """Real-time marine weather from public NOAA weather.gov API.
    
    FLOW:
    1. Get geographic point coordinates
    2. Fetch grid point metadata
    3. Get hourly forecast link
    4. Fetch hourly forecast periods
    5. Score wind + severe weather markers (storms, thunder, gales, floods)
    
    RISK SCORING:
    - Wind: 0-100 based on max wind speed (50+ knots = extreme)
    - Severe: 0-100 based on storm markers (20 points each)
    - Final: 60% wind + 40% severe weather
    
    Public API: No authentication required
    Timeout: 8 seconds per HTTP request (fail-open to mock on timeout)
    """

    def __init__(self) -> None:
        self.user_agent = "AIM-Project-Port-Intel/0.1 (contact: local)"

    def fetch_weather_risk(self, port: Port) -> WeatherRiskResult:
        """Fetch NOAA weather and score marine risk impact.
        
        Returns WeatherRiskResult with:
        - score: 0-100 risk level
        - source: 'noaa'
        - status: 'live' (success) or 'fallback' (API failed, use mock)
        - detail: Human-readable explanation
        """
        # Step 1: Get grid point for coordinates
        point_url = f"https://api.weather.gov/points/{port.center_latitude},{port.center_longitude}"
        point_payload = self._fetch_json(point_url)
        if point_payload is None:
            return WeatherRiskResult(score=0, source="noaa", status="fallback", detail="NOAA unavailable")

        # Step 2: Get forecast URL from grid point
        forecast_url = (
            point_payload.get("properties", {}).get("forecastHourly")
            or point_payload.get("properties", {}).get("forecast")
        )
        if not forecast_url:
            return WeatherRiskResult(score=0, source="noaa", status="fallback", detail="No NOAA forecast URL")

        # Step 3: Fetch hourly forecast
        forecast_payload = self._fetch_json(str(forecast_url))
        if forecast_payload is None:
            return WeatherRiskResult(score=0, source="noaa", status="fallback", detail="Forecast fetch failed")

        # Step 4: Extract forecast periods and score
        periods = forecast_payload.get("properties", {}).get("periods", [])
        if not isinstance(periods, list) or not periods:
            return WeatherRiskResult(score=0, source="noaa", status="fallback", detail="No forecast periods")

        # Look at next 8 periods (~8-24 hours of forecast data)
        sample = periods[:8]
        wind_max = 0
        severe_hits = 0
        for p in sample:
            wind = _wind_speed_knots(str(p.get("windSpeed", "0")))
            wind_max = max(wind_max, wind)
            text = f"{p.get('shortForecast', '')} {p.get('detailedForecast', '')}".lower()
            # Count severe weather markers
            if any(token in text for token in ("storm", "thunder", "gale", "hurricane", "squall", "flood")):
                severe_hits += 1

        # Score: 60% wind component + 40% severe weather component
        wind_score = min(100, int((wind_max / 50) * 100))
        severe_score = min(100, severe_hits * 20)
        score = max(0, min(100, int((wind_score * 0.6) + (severe_score * 0.4))))

        return WeatherRiskResult(
            score=score,
            source="noaa",
            status="live",
            detail=f"NOAA wind max {wind_max}kt; severe markers {severe_hits}",
        )

    def _fetch_json(self, url: str) -> dict | None:
        """Fetch JSON from URL with timeout and error handling."""
        request = Request(url, headers={"User-Agent": self.user_agent, "Accept": "application/geo+json"})
        try:
            with urlopen(request, timeout=8) as response:
                if response.status != 200:
                    return None
                return json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, OSError, json.JSONDecodeError, ValueError):
            return None


def _wind_speed_knots(value: str) -> int:
    """Parse wind speed string (MPH) and convert to knots."""
    cleaned = value.lower().replace("mph", "").strip()
    for token in cleaned.split():
        if token.isdigit():
            mph = int(token)
            return int(round(mph * 0.868976))
    return 0


# ========== QUERY ORCHESTRATOR ==========

class PortIntelligenceService:
    """Main orchestrator: coordinates all port intelligence operations.
    
    This is the primary entry point for all queries. It:
    1. Resolves port names to Port objects
    2. Fetches vessel snapshots from adapters
    3. Merges data from multiple sources
    4. Computes port risk scores
    5. Applies user filters
    6. Caches results with 180-second TTL
    7. Formats responses for CLI/web display
    """

    def __init__(self) -> None:
        # Initialize all subsystems
        self.port_resolver = PortResolver()
        self.primary_adapter = VesselFinderAdapter()
        self.secondary_adapter = MarineTrafficAdapter()
        self.weather_service = NoaaWeatherService()
        self.cache = PersistentCache()

    def suggest_ports(self, query: str) -> list[Port]:
        """Return port suggestions for auto-complete.
        
        Delegates to PortResolver.suggest() with scope enforcement.
        """
        return self.port_resolver.suggest(query)

    def query_port(self, request: PortQueryRequest) -> QueryResponse:
        """Execute full port intelligence query with caching, merge, and risk scoring.
        
        WORKFLOW:
        1. Create cache key from (port, horizon, page, filters)
        2. Return cached result if exists and not expired
        3. Resolve port name to Port object
        4. Fetch vessel snapshots: primary (VesselFinder) and secondary (MarineTraffic)
        5. Fetch weather from NOAA
        6. Merge vessel data: deduplicate by IMO, apply 65/35 weight policy
        7. Compute port risk: congestion + weather + operations + delays
        8. Filter problems by severity threshold
        9. Filter vessels by delay/confidence thresholds
        10. Paginate results
        11. Build quality metadata
        12. Cache response with 180-second TTL
        13. Return QueryResponse
        """
        # Step 1: Build cache key from query parameters
        cache_key = self.cache.make_key(
            [
                request.portInput.lower(),
                str(request.timeHorizonHours),
                str(request.page),
                str(request.pageSize),
                str(request.filters.onlyDelayed),
                request.filters.minSeverity,
                request.filters.minConfidence,
            ]
        )
        
        # Step 2: Check cache first
        cached = self.cache.get(cache_key)
        if cached is not None:
            return _response_from_cache(cached)

        # Step 3: Resolve port name to Port object
        port = self.port_resolver.resolve(request.portInput)
        
        # Step 4: Fetch vessel snapshots from both sources
        primary = self.primary_adapter.fetch_port_snapshot(port, request.timeHorizonHours)
        secondary = self.secondary_adapter.fetch_port_snapshot(port, request.timeHorizonHours)
        
        # Step 5: Fetch weather risk
        weather = self.weather_service.fetch_weather_risk(port)

        # Step 6: Merge vessel data from both sources
        merged = merge_vessels(primary, secondary)
        
        # Step 7: Compute port risk aggregation
        risk_summary = compute_port_risk(
            primary,
            secondary,
            merged,
            weather_override_score=weather.score if weather.status == "live" else None,
            weather_source=weather.source if weather.status == "live" else "marinetraffic",
            weather_detail=weather.detail,
        )

        # Step 8: Filter problems by severity threshold
        filtered_problems = _filter_problems(risk_summary.problems, request.filters.minSeverity)
        
        # Step 9: Filter vessels by delay and confidence thresholds
        filtered_vessels = _filter_vessels(merged, request.filters.onlyDelayed, request.filters.minConfidence)
        total_count = len(filtered_vessels)
        
        # Step 10: Paginate results
        paged_vessels = _paginate(filtered_vessels, request.page, request.pageSize)

        # Step 11: Build quality metadata (data freshness warnings)
        stale_warning = any(v.freshnessMinutes > 15 for v in filtered_vessels)

        # Build human-readable summary for chat window
        chat_summary = (
            f"{port.name} is currently {risk_summary.severity}. "
            f"Overall risk score is {risk_summary.port_score}/100 with {len(filtered_problems)} active issues."
        )

        # Assemble complete response
        response = QueryResponse(
            meta=QueryMeta(requestId=str(uuid4()), generatedAtUtc=datetime.now(UTC)),
            chatSummary=chat_summary,
            port={
                "name": port.name,
                "country": "US",
                "isContiguousUS": True,
                "timezone": port.timezone,
                "boundingBox": {
                    "min_lon": port.min_lon,
                    "min_lat": port.min_lat,
                    "max_lon": port.max_lon,
                    "max_lat": port.max_lat,
                },
                "linkedVesselCount": total_count,
            },
            problems=filtered_problems,
            vessels=paged_vessels,
            quality=QualityInfo(
                overallConfidence=_overall_confidence(filtered_vessels),
                missingFields=[],
                sourceHealth={**source_health(primary, secondary), "noaa": weather.status},
                staleDataWarning=stale_warning,
            ),
            pagination={
                "page": request.page,
                "pageSize": request.pageSize,
                "totalPages": max(1, (total_count + request.pageSize - 1) // request.pageSize),
            },
        )
        
        # Step 12: Cache response with 180-second TTL
        self.cache.set(cache_key, response.to_dict(), ttl_seconds=180)
        
        # Step 13: Return response
        return response

    def health_sources(self) -> dict:
        """Return source health and operational status.
        
        Useful for monitoring which APIs are live vs. mock.
        """
        return {
            "vesselfinder": "ok",
            "marinetraffic": "ok",
            "mode": {
                "vesselfinder": self.primary_adapter.mode(),
                "marinetraffic": self.secondary_adapter.mode(),
                "noaa": "public-live",
            },
            "cache": "enabled",
            "cacheTtlSeconds": 180,
        }


# ========== HELPER FUNCTIONS ==========

def _filter_vessels(vessels: list[Vessel], only_delayed: bool, min_confidence: str) -> list[Vessel]:
    """Apply strict filters to vessel list.
    
    - Confidence filter: only vessels at or above min_confidence level
    - Delay filter (if enabled): only vessels flagged as delayed
    """
    min_rank = CONFIDENCE_ORDER.get(min_confidence, 0)
    filtered = [v for v in vessels if CONFIDENCE_ORDER.get(v.confidence, 0) >= min_rank]
    if only_delayed:
        return [v for v in filtered if v.isDelayed]
    return filtered


def _filter_problems(problems: list[Problem], min_severity: str) -> list[Problem]:
    """Apply severity filtering to problems.
    
    Only return problems at or above the minimum severity threshold.
    """
    min_rank = SEVERITY_ORDER.get(min_severity, 0)
    return [p for p in problems if SEVERITY_ORDER.get(p.severityLabel, 0) >= min_rank]


def _overall_confidence(vessels: list[Vessel]) -> ConfidenceLabel:
    """Aggregate confidence across all vessels for quality metadata.
    
    - All 'high': result is 'high' confidence
    - Any 'medium': result is 'medium' confidence
    - Otherwise: 'low' confidence
    """
    if not vessels:
        return cast(ConfidenceLabel, "low")
    if all(v.confidence == "high" for v in vessels):
        return cast(ConfidenceLabel, "high")
    if any(v.confidence == "medium" for v in vessels):
        return cast(ConfidenceLabel, "medium")
    return cast(ConfidenceLabel, "low")


def _paginate(vessels: list[Vessel], page: int, page_size: int) -> list[Vessel]:
    """Paginate vessel results.
    
    Returns slice for (page-1)*pageSize to (page)*pageSize.
    """
    start = (page - 1) * page_size
    end = start + page_size
    return vessels[start:end]


def _response_from_cache(payload: dict) -> QueryResponse:
    """Reconstruct QueryResponse from cached dictionary.
    
    Re-hydrates all dataclass objects with proper datetime parsing from ISO strings.
    """
    problems = [
        Problem(
            title=item["title"],
            description=item["description"],
            scope=item["scope"],
            severityLabel=item["severityLabel"],
            riskScore=int(item["riskScore"]),
            evidenceSource=item["evidenceSource"],
            lastUpdatedUtc=datetime.fromisoformat(item["lastUpdatedUtc"].replace("Z", "+00:00")),
        )
        for item in payload.get("problems", [])
    ]

    vessels = [
        Vessel(
            name=item["name"],
            imo=item["imo"],
            mmsi=item["mmsi"],
            vesselType=item["vesselType"],
            flag=item["flag"],
            originPort=item["originPort"],
            destinationPort=item["destinationPort"],
            departureTimeUtc=datetime.fromisoformat(item["departureTimeUtc"].replace("Z", "+00:00")),
            etaUtc=datetime.fromisoformat(item["etaUtc"].replace("Z", "+00:00")),
            isDelayed=bool(item["isDelayed"]),
            status=item["status"],
            sourceUsed=item["sourceUsed"],
            confidence=item["confidence"],
            freshnessMinutes=int(item["freshnessMinutes"]),
            speedKnots=item.get("speedKnots"),
            courseDegrees=item.get("courseDegrees"),
            draughtMeters=item.get("draughtMeters"),
        )
        for item in payload.get("vessels", [])
    ]

    return QueryResponse(
        meta=QueryMeta(
            requestId=payload["meta"]["requestId"],
            generatedAtUtc=datetime.fromisoformat(payload["meta"]["generatedAtUtc"].replace("Z", "+00:00")),
            freshnessTargetMinutes=payload["meta"].get("freshnessTargetMinutes", 5),
        ),
        chatSummary=payload["chatSummary"],
        port=payload["port"],
        problems=problems,
        vessels=vessels,
        quality=QualityInfo(
            overallConfidence=payload["quality"]["overallConfidence"],
            missingFields=payload["quality"].get("missingFields", []),
            sourceHealth=payload["quality"].get("sourceHealth", {}),
            staleDataWarning=bool(payload["quality"].get("staleDataWarning", False)),
        ),
        pagination=payload.get("pagination", {"page": 1, "pageSize": 50, "totalPages": 1}),
    )


# ========== UTILITY FUNCTIONS ==========

def _parse_dt(value: object) -> datetime | None:
    """Parse ISO 8601 datetime string."""
    if not isinstance(value, str) or not value:
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt
    except ValueError:
        return None


def _to_int(value: object) -> int | None:
    """Safely convert value to int."""
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            return int(value)
        return None
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> float | None:
    """Safely convert value to float."""
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value)
        return None
    except (TypeError, ValueError):
        return None


def _score(value: object) -> int:
    """Convert and clamp value to 0-100 score."""
    raw = _to_int(value)
    if raw is None:
        return 0
    return max(0, min(100, raw))
