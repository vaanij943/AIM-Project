"""
Simple Flask web interface for MarineTraffic ports data extraction.
Paste raw HTML, CSV, or JSON—get structured port records back.
"""

from flask import Flask, render_template, request, jsonify
import json
import csv
import io
from marine_traffic_source import MarineTrafficDataSource

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/parse", methods=["POST"])
def parse_data():
    """
    Accept pasted data (HTML, CSV, or JSON) and return parsed ports.
    """
    data = request.get_json(silent=True)

    # Accept JSON body, form body, or raw text so bad request formats do not crash.
    if isinstance(data, dict):
        pasted_content = str(data.get("content", "")).strip()
    elif isinstance(data, str):
        pasted_content = data.strip()
    else:
        form_content = request.form.get("content", "") if request.form else ""
        raw_content = request.get_data(as_text=True) or ""
        pasted_content = (form_content or raw_content).strip()
    
    if not pasted_content:
        return jsonify({"error": "No content provided"}), 400
    
    try:
        # Try JSON
        try:
            json_data = json.loads(pasted_content)
            if isinstance(json_data, list):
                records = [normalize_record(r) for r in json_data]
                return jsonify({
                    "success": True,
                    "format": "JSON",
                    "count": len(records),
                    "records": records[:50]
                })
        except json.JSONDecodeError:
            pass
        
        # Try HTML table
        if "<table" in pasted_content.lower() or "<tr" in pasted_content.lower():
            source = MarineTrafficDataSource()
            records = source._parse_html_table(pasted_content)
            if records:
                normalized = [source._normalize_record(r) for r in records]
                return jsonify({
                    "success": True,
                    "format": "HTML",
                    "count": len(normalized),
                    "records": normalized[:50]
                })

        # Try CSV
        if "," in pasted_content and "\n" in pasted_content:
            records = parse_csv(pasted_content)
            if records:
                return jsonify({
                    "success": True,
                    "format": "CSV",
                    "count": len(records),
                    "records": records[:50]  # Return first 50
                })
        
        return jsonify({"error": "Could not parse content. Try CSV, JSON, or HTML table format."}), 400
    
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

def parse_csv(csv_content):
    """Parse CSV string and return list of dicts."""
    try:
        reader = csv.DictReader(io.StringIO(csv_content))
        if not reader.fieldnames:
            return []

        rows = [dict(row) for row in reader]
        # Ignore fully empty rows from trailing newlines/pasted blanks.
        return [
            row for row in rows
            if any((value or "").strip() for value in row.values())
        ]
    except Exception:
        return []

def normalize_record(record):
    """Normalize a port record to standard schema."""
    if isinstance(record, dict):
        return {
            "flag": record.get("flag"),
            "portname": record.get("portname") or record.get("Port Name") or record.get("port"),
            "unlocode": record.get("unlocode") or record.get("UNLOCODE"),
            "photo": record.get("photo"),
            "vessels_in_port": record.get("vessels_in_port"),
            "vessels_departures": record.get("vessels_departures"),
            "vessels_arrivals": record.get("vessels_arrivals"),
            "vessels_expected_arrivals": record.get("vessels_expected_arrivals"),
            "local_time": record.get("local_time"),
            "anchorage": record.get("anchorage"),
            "geographical_area_one": record.get("geographical_area_one"),
            "geographical_area_two": record.get("geographical_area_two"),
            "coverage": record.get("coverage"),
        }
    return record

if __name__ == "__main__":
    app.run(debug=True, port=5000)
