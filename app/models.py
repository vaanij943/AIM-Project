"""Data models for the US Port Intelligence MVP.

Defines all request/response structures, data classes for ports, vessels, problems,
and risk scoring enums. These dataclasses are the core data types passed between
CLI, core business logic, and cache layers.

MAIN COMPONENTS:
  1. Request Types: PortQueryRequest (user query), QueryFilters (filtering options)
  2. Response Types: QueryResponse (complete result), QueryMeta, QualityInfo
  3. Domain Models: Port, Vessel, Problem (risk flags)
  4. Reference Data: PORTS (25 US ports), EXCLUDED_STATES (AK, HI scope guard)

RISK SCORING SCALE:
  - Severity: none (0-9) → light (10-29) → moderate (30-54) → heavily (55-79) → extreme (80-100)
  - Confidence: low (less trustworthy) → medium → high (most trustworthy)
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Literal

# ========== ENUMS ==========

# 5-tier severity scale for port risks (congestion, weather, delays, etc.)
SeverityLabel = Literal["none", "light", "moderate", "heavily", "extreme"]
# 3-tier confidence scale for data trustworthiness (how confident are we in this data?)
ConfidenceLabel = Literal["low", "medium", "high"]


@dataclass(slots=True)
class QueryFilters:
    """Filter criteria applied to query results (strict filtering in risk engine).
    
    These filters narrow down which problems and vessels are shown:
    - onlyDelayed: If True, show only vessels with arrival delays
    - minSeverity: Only show problems at or above this severity threshold
    - minConfidence: Only show data meeting this confidence level
    """
    onlyDelayed: bool = False  # If True, return only delayed vessels
    minSeverity: SeverityLabel = "none"  # Only problems at/above this severity (none/light/moderate/heavily/extreme)
    minConfidence: ConfidenceLabel = "low"  # Only data at/above this confidence (low/medium/high)



@dataclass(slots=True)
class PortQueryRequest:
    """Main query request from user: what port to analyze and what filters to apply.
    
    This is created by the CLI and passed to PortIntelligenceService.query_port().
    Includes all query parameters: port name, time horizon, pagination, filters.
    
    VALIDATION:
    - portInput: Required, 2+ characters (port name or UNLOCODE)
    - timeHorizonHours: 1-168 hours (vessel look-ahead window)
    - page/pageSize: Pagination controls (standard limits)
    - filters: Additional strictness applied to results
    """
    portInput: str  # Port name or UNLOCODE (e.g., "Los Angeles" or "USLAX") - REQUIRED
    inputType: Literal["name_or_code"] = "name_or_code"  # Search strategy (currently only name_or_code supported)
    timeHorizonHours: int = 48  # Look-ahead window in hours (typically 24-168); vessels arriving within this window
    page: int = 1  # Pagination: which page of results (1-based indexing)
    pageSize: int = 50  # Results per page (1-200); controls vessel list length
    filters: QueryFilters = field(default_factory=QueryFilters)  # Apply strict filtering (severity, confidence, delays)

    @classmethod
    def from_payload(cls, payload: dict) -> "PortQueryRequest":
        """Build request from dictionary payload (for REST API or JSON input)."""
        filters = payload.get("filters", {})
        model = cls(
            portInput=str(payload.get("portInput", "")).strip(),
            inputType=payload.get("inputType", "name_or_code"),
            timeHorizonHours=int(payload.get("timeHorizonHours", 48)),
            page=int(payload.get("page", 1)),
            pageSize=int(payload.get("pageSize", 50)),
            filters=QueryFilters(
                onlyDelayed=bool(filters.get("onlyDelayed", False)),
                minSeverity=filters.get("minSeverity", "none"),
                minConfidence=filters.get("minConfidence", "low"),
            ),
        )
        model.validate()
        return model

    def validate(self) -> None:
        """Validate all query parameters; raise ValueError if any constraint violated."""
        allowed_severity = {"none", "light", "moderate", "heavily", "extreme"}
        allowed_confidence = {"low", "medium", "high"}
        if len(self.portInput) < 2:
            raise ValueError("portInput must have at least 2 characters")
        if not (1 <= self.timeHorizonHours <= 168):
            raise ValueError("timeHorizonHours must be between 1 and 168")
        if self.page < 1:
            raise ValueError("page must be >= 1")
        if not (1 <= self.pageSize <= 200):
            raise ValueError("pageSize must be between 1 and 200")
        if self.filters.minSeverity not in allowed_severity:
            raise ValueError("minSeverity must be one of: none, light, moderate, heavily, extreme")
        if self.filters.minConfidence not in allowed_confidence:
            raise ValueError("minConfidence must be one of: low, medium, high")


@dataclass(slots=True)
class Port:
    """US port metadata - static reference data with geographic bounding boxes.
    
    Represents a single port location with geographic bounds for vessel tracking,
    center point for weather lookups, and timezone info.
    """
    name: str  # Official port name (e.g., "Port of Houston")
    min_lon: float  # Western boundary (minimum longitude)
    min_lat: float  # Southern boundary (minimum latitude)
    max_lon: float  # Eastern boundary (maximum longitude)
    max_lat: float  # Northern boundary (maximum latitude)
    timezone: str  # IANA timezone for time rendering (e.g., "America/Chicago")
    center_latitude: float  # Center latitude for weather API lookups
    center_longitude: float  # Center longitude for weather API lookups
    country: str = "US"  # Country code (always "US")
    isContiguousUS: bool = True  # Scope guard: always True (all ports are contiguous US)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass(slots=True)
class Vessel:
    """Live vessel data from AIS tracking (MERGED from VesselFinder + MarineTraffic).
    
    Represents a single vessel currently bound for or operating near the query port.
    Data is merged from two sources with deduplication by IMO number.
    Includes vessel type, status, ETA, and data quality metrics.
    """
    name: str  # Vessel name (e.g., "MV EVER GIVEN")
    imo: str  # International Maritime Organization identifier (unique per vessel)
    mmsi: str  # Maritime Mobile Service Identity (AIS transponder ID)
    vesselType: str  # Cargo type (Container Ship, Tanker, Bulk Carrier, Ro-Ro, etc.)
    flag: str  # Country of registry (US, PA, LR, SG, MH, etc.)
    originPort: str  # Departure port (where vessel sailed from)
    destinationPort: str  # Destination (typically the query port)
    departureTimeUtc: datetime  # When vessel left origin (UTC)
    etaUtc: datetime  # Estimated time of arrival at destination (UTC)
    isDelayed: bool  # True if vessel is behind schedule
    status: str  # Current status (Sailing, At anchor, Approaching, Moored, etc.)
    sourceUsed: str  # Which adapter provided this data (VesselFinder, MarineTraffic, or mock)
    confidence: ConfidenceLabel  # Data trustworthiness (low/medium/high)
    freshnessMinutes: int  # How old the data is in minutes (0 = just updated, 30 = 30 min old)
    speedKnots: float | None = None  # Current speed through water (optional, in knots)
    courseDegrees: int | None = None  # Current heading (0-360 degrees, optional)
    draughtMeters: float | None = None  # Vessel draft depth in water (optional, meters)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization with ISO datetime formatting."""
        payload = asdict(self)
        payload["departureTimeUtc"] = self.departureTimeUtc.isoformat()
        payload["etaUtc"] = self.etaUtc.isoformat()
        return payload


@dataclass(slots=True)
class Problem:
    """Risk flag raised by the risk engine (e.g., high congestion, severe weather, delays).
    
    Each Problem represents a detected issue at the port or vessel level.
    Includes severity score, description, and source for audit trail.
    Problems are aggregated from multiple engines: congestion, weather, operations, delays.
    """
    title: str  # One-liner (e.g., "Port Congestion", "Severe Weather Warning", "Vessel Delays")
    description: str  # Detailed explanation with metrics for chat/UI display
    scope: Literal["port", "vessel"]  # Is this port-wide or vessel-specific?
    severityLabel: SeverityLabel  # Risk level (none/light/moderate/heavily/extreme)
    riskScore: int  # Numeric 0-100 score for this particular problem
    evidenceSource: str  # Where did this risk come from? (VesselFinder, NOAA, merged, etc.)
    lastUpdatedUtc: datetime  # When was this risk last recalculated/verified?

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization with ISO datetime formatting."""
        payload = asdict(self)
        payload["lastUpdatedUtc"] = self.lastUpdatedUtc.isoformat()
        return payload


@dataclass(slots=True)
class QueryMeta:
    """Metadata about the query response (timing, request tracking, data freshness targets).
    
    Used for auditing, debugging, and understanding response provenance.
    """
    requestId: str  # Unique UUID for this query (for tracking/debugging)
    generatedAtUtc: datetime  # When was this response generated?
    freshnessTargetMinutes: int = 5  # Target data freshness (typically 5 min; cache TTL overrides this)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization with ISO datetime formatting."""
        payload = asdict(self)
        payload["generatedAtUtc"] = self.generatedAtUtc.isoformat()
        return payload


@dataclass(slots=True)
class QualityInfo:
    """Data quality assessment: source health, confidence levels, missing fields, staleness.
    
    Helps users understand data reliability and decide if results are trustworthy.
    Generated by merge engine and risk engine as they process data.
    """
    overallConfidence: ConfidenceLabel  # Aggregate confidence across all results (low/medium/high)
    missingFields: list[str]  # Which fields are unavailable (e.g., ["speed", "draft"])
    sourceHealth: dict[str, str]  # Health per source (e.g., {"VesselFinder": "ok", "NOAA": "degraded", "MarineTraffic": "stale"})
    staleDataWarning: bool  # True if any vessel data is older than freshness target (>15 min)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass(slots=True)
class QueryResponse:
    """Complete response returned to CLI/frontend after port intelligence query.
    
    This is what users see after querying a port. It includes:
    - Port metadata (location, timezone)
    - All detected problems/risks (sorted by severity)
    - Vessel list (paginated, filtered by user criteria)
    - Data quality assessment
    - Pagination metadata
    
    The response is cached for 180 seconds to avoid repeated computations.
    """
    meta: QueryMeta  # Request ID, generation time, freshness targets
    chatSummary: str  # Human-readable 1-3 sentence summary (for chat window display)
    port: dict  # Port details (name, state, unlocode, timezone, vessel count, etc.)
    problems: list[Problem]  # All flagged risks (filtered by user severity threshold)
    vessels: list[Vessel]  # Vessels bound for port (filtered by delay/confidence, paginated)
    quality: QualityInfo  # Data quality metrics: source health, confidence, staleness
    pagination: dict[str, int]  # Pagination info: page, pageSize, totalPages

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization (used by cache layer)."""
        return {
            "meta": self.meta.to_dict(),
            "chatSummary": self.chatSummary,
            "port": self.port,
            "problems": [p.to_dict() for p in self.problems],
            "vessels": [v.to_dict() for v in self.vessels],
            "quality": self.quality.to_dict(),
            "pagination": self.pagination,
        }


# ========== STATIC PORT REFERENCE DATA ==========

# All major US ports with geographic bounding boxes for vessel tracking
# Each entry includes:
#   - name: Official port name
#   - min_lon/max_lon: Longitude bounds (west-east)
#   - min_lat/max_lat: Latitude bounds (south-north)
#   - timezone: IANA timezone for time display
#   - center_latitude/center_longitude: Computed center point for weather API
#
# Total: 44 ports covering Gulf, Atlantic, Pacific, Great Lakes, and River systems
PORTS = [
    # GULF COAST PORTS
    {"name": "Port of Houston", "min_lon": -95.35, "min_lat": 29.55, "max_lon": -94.85, "max_lat": 29.85, "timezone": "America/Chicago", "center_latitude": 29.70, "center_longitude": -95.10},
    {"name": "Port of Corpus Christi", "min_lon": -97.50, "min_lat": 27.75, "max_lon": -97.00, "max_lat": 28.00, "timezone": "America/Chicago", "center_latitude": 27.875, "center_longitude": -97.25},
    {"name": "Port of Brownsville", "min_lon": -97.50, "min_lat": 25.90, "max_lon": -97.10, "max_lat": 26.10, "timezone": "America/Chicago", "center_latitude": 26.00, "center_longitude": -97.30},
    {"name": "Port Arthur / Beaumont", "min_lon": -94.20, "min_lat": 29.85, "max_lon": -93.80, "max_lat": 30.10, "timezone": "America/Chicago", "center_latitude": 29.975, "center_longitude": -94.00},
    {"name": "Port of Galveston", "min_lon": -94.90, "min_lat": 29.25, "max_lon": -94.70, "max_lat": 29.40, "timezone": "America/Chicago", "center_latitude": 29.325, "center_longitude": -94.80},
    {"name": "Port of New Orleans", "min_lon": -90.10, "min_lat": 29.90, "max_lon": -89.90, "max_lat": 30.05, "timezone": "America/Chicago", "center_latitude": 29.975, "center_longitude": -90.00},
    {"name": "Port of Lake Charles", "min_lon": -93.30, "min_lat": 30.15, "max_lon": -93.10, "max_lat": 30.25, "timezone": "America/Chicago", "center_latitude": 30.20, "center_longitude": -93.20},
    {"name": "Port of Mobile", "min_lon": -88.10, "min_lat": 30.60, "max_lon": -87.90, "max_lat": 30.75, "timezone": "America/Chicago", "center_latitude": 30.675, "center_longitude": -88.00},
    {"name": "Port of Tampa Bay", "min_lon": -82.55, "min_lat": 27.85, "max_lon": -82.35, "max_lat": 28.00, "timezone": "America/New_York", "center_latitude": 27.925, "center_longitude": -82.45},
    {"name": "Port of Pascagoula", "min_lon": -88.65, "min_lat": 30.30, "max_lon": -88.45, "max_lat": 30.45, "timezone": "America/Chicago", "center_latitude": 30.375, "center_longitude": -88.55},
    {"name": "Port of Gulfport", "min_lon": -89.15, "min_lat": 30.30, "max_lon": -88.95, "max_lat": 30.45, "timezone": "America/Chicago", "center_latitude": 30.375, "center_longitude": -89.05},
    {"name": "Port of Freeport", "min_lon": -95.40, "min_lat": 28.90, "max_lon": -95.20, "max_lat": 29.05, "timezone": "America/Chicago", "center_latitude": 28.975, "center_longitude": -95.30},
    
    # ATLANTIC COAST - NORTHEAST
    {"name": "Port of New York / New Jersey", "min_lon": -74.15, "min_lat": 40.55, "max_lon": -73.85, "max_lat": 40.75, "timezone": "America/New_York", "center_latitude": 40.65, "center_longitude": -74.00},
    {"name": "Port of Baltimore", "min_lon": -76.65, "min_lat": 39.20, "max_lon": -76.45, "max_lat": 39.35, "timezone": "America/New_York", "center_latitude": 39.275, "center_longitude": -76.55},
    {"name": "Port of Philadelphia", "min_lon": -75.20, "min_lat": 39.85, "max_lon": -75.00, "max_lat": 40.05, "timezone": "America/New_York", "center_latitude": 39.95, "center_longitude": -75.10},
    {"name": "Port of Boston", "min_lon": -71.10, "min_lat": 42.30, "max_lon": -70.90, "max_lat": 42.45, "timezone": "America/New_York", "center_latitude": 42.375, "center_longitude": -71.00},
    {"name": "Port of Providence", "min_lon": -71.45, "min_lat": 41.75, "max_lon": -71.35, "max_lat": 41.85, "timezone": "America/New_York", "center_latitude": 41.80, "center_longitude": -71.40},
    
    # ATLANTIC COAST - MID
    {"name": "Port of Virginia (Norfolk)", "min_lon": -76.40, "min_lat": 36.85, "max_lon": -76.20, "max_lat": 37.05, "timezone": "America/New_York", "center_latitude": 36.95, "center_longitude": -76.30},
    {"name": "Port of Wilmington (NC)", "min_lon": -77.98, "min_lat": 34.20, "max_lon": -77.88, "max_lat": 34.30, "timezone": "America/New_York", "center_latitude": 34.25, "center_longitude": -77.93},
    
    # ATLANTIC COAST - SOUTHEAST
    {"name": "Port of Savannah", "min_lon": -81.15, "min_lat": 31.95, "max_lon": -80.95, "max_lat": 32.15, "timezone": "America/New_York", "center_latitude": 32.05, "center_longitude": -81.05},
    {"name": "Port of Brunswick (GA)", "min_lon": -81.55, "min_lat": 31.10, "max_lon": -81.40, "max_lat": 31.25, "timezone": "America/New_York", "center_latitude": 31.175, "center_longitude": -81.475},
    {"name": "Port of Charleston", "min_lon": -79.97, "min_lat": 32.70, "max_lon": -79.87, "max_lat": 32.85, "timezone": "America/New_York", "center_latitude": 32.775, "center_longitude": -79.92},
    {"name": "Port of Jacksonville", "min_lon": -81.65, "min_lat": 30.30, "max_lon": -81.50, "max_lat": 30.45, "timezone": "America/New_York", "center_latitude": 30.375, "center_longitude": -81.575},
    
    # FLORIDA PORTS
    {"name": "Port Everglades (Fort Lauderdale)", "min_lon": -80.15, "min_lat": 26.05, "max_lon": -80.05, "max_lat": 26.15, "timezone": "America/New_York", "center_latitude": 26.10, "center_longitude": -80.10},
    {"name": "Port of Miami", "min_lon": -80.20, "min_lat": 25.75, "max_lon": -80.10, "max_lat": 25.85, "timezone": "America/New_York", "center_latitude": 25.80, "center_longitude": -80.15},
    {"name": "Port of Port Canaveral", "min_lon": -80.65, "min_lat": 28.38, "max_lon": -80.55, "max_lat": 28.48, "timezone": "America/New_York", "center_latitude": 28.43, "center_longitude": -80.60},
    
    # PACIFIC COAST - CALIFORNIA
    {"name": "Port of Los Angeles", "min_lon": -118.30, "min_lat": 33.65, "max_lon": -118.10, "max_lat": 33.80, "timezone": "America/Los_Angeles", "center_latitude": 33.725, "center_longitude": -118.20},
    {"name": "Port of Long Beach", "min_lon": -118.25, "min_lat": 33.70, "max_lon": -118.10, "max_lat": 33.85, "timezone": "America/Los_Angeles", "center_latitude": 33.775, "center_longitude": -118.175},
    {"name": "Port of San Diego", "min_lon": -117.20, "min_lat": 32.65, "max_lon": -117.05, "max_lat": 32.80, "timezone": "America/Los_Angeles", "center_latitude": 32.725, "center_longitude": -117.125},
    {"name": "Port of San Francisco", "min_lon": -122.45, "min_lat": 37.75, "max_lon": -122.30, "max_lat": 37.85, "timezone": "America/Los_Angeles", "center_latitude": 37.80, "center_longitude": -122.375},
    {"name": "Port of Oakland", "min_lon": -122.35, "min_lat": 37.75, "max_lon": -122.20, "max_lat": 37.85, "timezone": "America/Los_Angeles", "center_latitude": 37.80, "center_longitude": -122.275},
    {"name": "Port Hueneme", "min_lon": -119.25, "min_lat": 34.10, "max_lon": -119.15, "max_lat": 34.20, "timezone": "America/Los_Angeles", "center_latitude": 34.15, "center_longitude": -119.20},
    
    # PACIFIC COAST - PACIFIC NORTHWEST
    {"name": "Port of Seattle", "min_lon": -122.45, "min_lat": 47.55, "max_lon": -122.30, "max_lat": 47.70, "timezone": "America/Los_Angeles", "center_latitude": 47.625, "center_longitude": -122.375},
    {"name": "Port of Tacoma", "min_lon": -122.50, "min_lat": 47.20, "max_lon": -122.35, "max_lat": 47.35, "timezone": "America/Los_Angeles", "center_latitude": 47.275, "center_longitude": -122.425},
    {"name": "Port of Portland (OR)", "min_lon": -122.80, "min_lat": 45.55, "max_lon": -122.60, "max_lat": 45.70, "timezone": "America/Los_Angeles", "center_latitude": 45.625, "center_longitude": -122.70},
    
    # GREAT LAKES PORTS
    {"name": "Port of Chicago", "min_lon": -87.75, "min_lat": 41.70, "max_lon": -87.55, "max_lat": 41.90, "timezone": "America/Chicago", "center_latitude": 41.80, "center_longitude": -87.65},
    {"name": "Port of Detroit", "min_lon": -83.15, "min_lat": 42.25, "max_lon": -82.95, "max_lat": 42.45, "timezone": "America/New_York", "center_latitude": 42.35, "center_longitude": -83.05},
    {"name": "Port of Cleveland", "min_lon": -81.75, "min_lat": 41.45, "max_lon": -81.55, "max_lat": 41.65, "timezone": "America/New_York", "center_latitude": 41.55, "center_longitude": -81.65},
    {"name": "Port of Milwaukee", "min_lon": -87.95, "min_lat": 43.00, "max_lon": -87.85, "max_lat": 43.10, "timezone": "America/Chicago", "center_latitude": 43.05, "center_longitude": -87.90},
    {"name": "Port of Duluth / Superior", "min_lon": -92.25, "min_lat": 46.70, "max_lon": -91.95, "max_lat": 46.90, "timezone": "America/Chicago", "center_latitude": 46.80, "center_longitude": -92.10},
    {"name": "Port of Toledo", "min_lon": -83.60, "min_lat": 41.60, "max_lon": -83.45, "max_lat": 41.75, "timezone": "America/New_York", "center_latitude": 41.675, "center_longitude": -83.525},
    {"name": "Port of Buffalo", "min_lon": -79.05, "min_lat": 42.85, "max_lon": -78.85, "max_lat": 42.95, "timezone": "America/New_York", "center_latitude": 42.90, "center_longitude": -78.95},
    
    # RIVER PORTS
    {"name": "Port of St. Louis", "min_lon": -90.25, "min_lat": 38.55, "max_lon": -90.10, "max_lat": 38.70, "timezone": "America/Chicago", "center_latitude": 38.625, "center_longitude": -90.175},
    {"name": "Port of Memphis", "min_lon": -90.15, "min_lat": 35.10, "max_lon": -89.95, "max_lat": 35.25, "timezone": "America/Chicago", "center_latitude": 35.175, "center_longitude": -90.05},
    {"name": "Port of Baton Rouge", "min_lon": -91.25, "min_lat": 30.40, "max_lon": -91.05, "max_lat": 30.55, "timezone": "America/Chicago", "center_latitude": 30.475, "center_longitude": -91.15},
]

# Scope guard: exclude Alaska and Hawaii from port selection and vessel queries
# MVP Phase 1 is contiguous US only; can expand to include AK/HI in future phases
EXCLUDED_STATES = {"AK", "HI"}
