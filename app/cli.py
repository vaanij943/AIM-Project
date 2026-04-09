"""CLI interface for AIM Port Intelligence system.

Pure Python command-line tool for querying US port intelligence data.
Replaces Flask web server with direct function calls.

USAGE:
  python -m app.cli        Run CLI, enter port name, get full intelligence report
  
INPUT:
  - Port name (e.g., "Houston", "Los Angeles")
  - Port code (e.g., "USLAX", "USHOU")
  - State code/name (e.g., "CA", "California")

OUTPUT:
  - One-page comprehensive intelligence report with:
    1. Executive summary
    2. Port details and linked vessel count
    3. Active problems/risks (severity-ranked)
    4. Sample vessels and ETAs
    5. Data quality and source health
  
All queries cached for 180 seconds (app/data/query_cache.json).
"""
from __future__ import annotations

import json
import sys
from app.core import PortIntelligenceService
from app.models import PortQueryRequest, QueryFilters


def main():
    """Main CLI entry point - minimal single query flow.
    
    Ultra-simplified workflow:
    1. Ask for port name
    2. Query with default parameters
    3. Display full intelligence report
    4. Exit
    """
    service = PortIntelligenceService()
    
    # Single input: port name only
    port_input = input("Port (city or code): ").strip()
    if not port_input:
        print("[ERROR] No port entered.")
        sys.exit(1)
    
    try:
        # Build request with defaults - no extra prompts
        request = PortQueryRequest(
            portInput=port_input,
            timeHorizonHours=48,
            page=1,
            pageSize=50,
            filters=QueryFilters(
                onlyDelayed=False,
                minSeverity="none",
                minConfidence="low",
            ),
        )
        
        print()
        response = service.query_port(request)
        print()
        
        # Display comprehensive result
        _print_response(response, service)
        
    except ValueError as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)


def _print_response(response, service: PortIntelligenceService) -> None:
    """Display comprehensive one-page intelligence report.
    
    Includes:
    - Executive summary
    - Port metadata
    - Active problems ranked by severity
    - Sample vessels and ETAs
    - Data quality metrics
    - Source health status
    """
    print("=" * 80)
    print(f"PORT INTELLIGENCE REPORT")
    print("=" * 80)
    
    # Executive summary
    print(f"\n[SUMMARY]")
    print(f"   {response.chatSummary}")
    
    # Port info
    port = response.port
    print(f"\n[PORT INFO]")
    print(f"   Name: {port['name']}")
    print(f"   Timezone: {port['timezone']}")
    print(f"   Geographic Range: ({port['boundingBox']['min_lon']}, {port['boundingBox']['min_lat']}) to ({port['boundingBox']['max_lon']}, {port['boundingBox']['max_lat']})")
    print(f"   Linked Vessels (48h): {port['linkedVesselCount']}")
    
    # Problems/Risks ranked by severity
    if response.problems:
        print(f"\n[ACTIVE PROBLEMS] ({len(response.problems)} total)")
        for i, problem in enumerate(response.problems, 1):
            severity_marker = {
                "none": "[OK]",
                "light": "[!]",
                "moderate": "[!!]",
                "heavily": "[!!!]",
                "extreme": "[CRITICAL]",
            }
            marker = severity_marker.get(problem.severityLabel, "[?]")
            print(f"   {i}. {marker} {problem.title}")
            print(f"      Risk Score: {problem.riskScore}/100 ({problem.severityLabel.upper()})")
            print(f"      {problem.description}")
            print(f"      Source: {problem.evidenceSource}")
    else:
        print(f"\n[ACTIVE PROBLEMS]")
        print(f"   None detected - port operating normally")
    
    # Vessels (show first 10)
    if response.vessels:
        print(f"\n[INBOUND VESSELS] ({len(response.vessels)} bound for port, showing first 10):")
        for i, vessel in enumerate(response.vessels[:10], 1):
            delayed_flag = "[DELAYED]" if vessel.isDelayed else "[OK]"
            print(f"   {i}. {vessel.name} {delayed_flag}")
            print(f"      Type: {vessel.vesselType} | Flag: {vessel.flag}")
            print(f"      Route: {vessel.originPort} -> {vessel.destinationPort}")
            print(f"      ETA: {vessel.etaUtc.strftime('%Y-%m-%d %H:%M UTC')}")
            print(f"      Status: {vessel.status} | Confidence: {vessel.confidence}")
        if len(response.vessels) > 10:
            print(f"   ... and {len(response.vessels)-10} more vessels")
    else:
        print(f"\n[INBOUND VESSELS]")
        print(f"   None detected in 48-hour window")
    
    # Data quality & source health combined
    print(f"\n[DATA QUALITY & SOURCE STATUS]")
    print(f"   Overall Confidence: {response.quality.overallConfidence}")
    print(f"   Stale Data Warning: {'YES - data may be outdated' if response.quality.staleDataWarning else 'NO - data is current'}")
    
    health = service.health_sources()
    print(f"\n   Source Status:")
    print(f"      VesselFinder: {health['vesselfinder']} ({health['mode']['vesselfinder']})")
    print(f"      MarineTraffic: {health['marinetraffic']} ({health['mode']['marinetraffic']})")
    print(f"      NOAA Weather: {health['mode']['noaa']}")
    print(f"      Cache: {health['cache']} (TTL: {health['cacheTtlSeconds']}s)")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
