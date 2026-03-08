#!/usr/bin/env python3 -u
"""
NEXRAD Level 2 Radar Processor for LancasterWX.com
Uses NOAA THREDDS Data Server for real-time Level 2 data
Outputs GeoJSON to public directory for web access
"""

import os
import sys
import json
import tempfile
from datetime import datetime, timedelta
import time
import threading
import requests
from xml.etree import ElementTree
import pyart
import numpy as np
from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS

# Configuration
RADAR_SITES = ['KLWX', 'KDIX']  # Sterling VA, Mt Holly NJ
OUTPUT_DIR = '/app/public'
THREDDS_BASE = 'https://thredds.ucar.edu/thredds'
LOOP_INTERVAL = 300  # 5 minutes

# Reflectivity color scale (dBZ to color)
def get_color_from_dbz(dbz):
    """Convert radar reflectivity (dBZ) to hex color"""
    if dbz < 5:
        return None  # Transparent - no precipitation
    elif dbz < 10:
        return '#00ecec'  # Very light blue
    elif dbz < 15:
        return '#00d0ff'  # Light blue
    elif dbz < 20:
        return '#0099ff'  # Blue
    elif dbz < 25:
        return '#00ff00'  # Green
    elif dbz < 30:
        return '#00cc00'  # Dark green
    elif dbz < 35:
        return '#ffff00'  # Yellow
    elif dbz < 40:
        return '#ffcc00'  # Orange
    elif dbz < 45:
        return '#ff9900'  # Dark orange
    elif dbz < 50:
        return '#ff0000'  # Red
    elif dbz < 55:
        return '#cc0000'  # Dark red
    elif dbz < 60:
        return '#ff00ff'  # Magenta
    else:
        return '#9900cc'  # Purple - very heavy

def get_latest_radar_file(site):
    """Get the most recent radar file from THREDDS for a given site"""
    try:
        # Get current UTC time
        now = datetime.utcnow()
        
        # Try the last 2 hours (THREDDS organizes by date/time)
        for hours_ago in range(0, 3):
            check_time = now - timedelta(hours=hours_ago)
            date_str = check_time.strftime('%Y%m%d')
            
            # Build THREDDS catalog URL
            catalog_url = f"{THREDDS_BASE}/catalog/nexrad/level2/{site}/{date_str}/catalog.xml"
            
            print(f"Checking THREDDS for {site} on {date_str}...")
            
            try:
                response = requests.get(catalog_url, timeout=10)
                if response.status_code != 200:
                    continue
                
                # Parse XML catalog
                root = ElementTree.fromstring(response.content)
                
                # Find all dataset entries
                ns = {'cat': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
                datasets = root.findall('.//cat:dataset[@urlPath]', ns)
                
                if not datasets:
                    continue
                
                # Get the most recent file (last in list)
                latest = datasets[-1]
                url_path = latest.get('urlPath')
                
                if url_path:
                    # Build download URL
                    download_url = f"{THREDDS_BASE}/fileServer/{url_path}"
                    print(f"Found latest file: {url_path}")
                    return download_url
                    
            except Exception as e:
                print(f"Error checking {date_str}: {e}")
                continue
        
        print(f"No recent radar files found for {site}")
        return None
        
    except Exception as e:
        print(f"Error finding radar file for {site}: {e}")
        return None

def download_radar_file(download_url):
    """Download radar file from THREDDS to temp location"""
    try:
        # Create temp file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.ar2v')
        
        print(f"Downloading from THREDDS...")
        
        # Stream download to handle large files
        response = requests.get(download_url, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(temp_file.name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded to {temp_file.name}")
        return temp_file.name
        
    except Exception as e:
        print(f"Error downloading radar file: {e}")
        return None

def process_radar_file(file_path, site):
    """Process radar file with Py-ART and create GeoJSON"""
    try:
        print(f"Processing {site} radar data...")
        
        # Read the radar file
        radar = pyart.io.read_nexrad_archive(file_path)
        
        # Get the first sweep (lowest elevation angle - best for precipitation)
        sweep = 0
        
        # Extract reflectivity data
        reflectivity = radar.get_field(sweep, 'reflectivity')
        
        # Get gate coordinates (range and azimuth)
        gate_range = radar.range['data']
        gate_azimuth = radar.azimuth['data'][radar.sweep_start_ray_index['data'][sweep]:
                                             radar.sweep_end_ray_index['data'][sweep] + 1]
        
        # Radar location
        radar_lat = radar.latitude['data'][0]
        radar_lon = radar.longitude['data'][0]
        
        print(f"Radar location: {radar_lat}, {radar_lon}")
        print(f"Reflectivity shape: {reflectivity.shape}")
        
        # Create GeoJSON features
        features = []
        
        # Sample every Nth gate and ray to reduce data size
        gate_step = 5  # Every 5th gate (~1.25km spacing)
        ray_step = 2   # Every 2nd ray (~1 degree spacing)
        
        for ray_idx in range(0, len(gate_azimuth), ray_step):
            azimuth = gate_azimuth[ray_idx]
            
            for gate_idx in range(0, len(gate_range), gate_step):
                # Get reflectivity value
                dbz_value = reflectivity[ray_idx, gate_idx]
                
                # Skip masked/invalid values
                if np.ma.is_masked(dbz_value) or dbz_value < 5:
                    continue
                
                # Calculate lat/lon for this gate
                range_km = gate_range[gate_idx] / 1000.0
                
                # Simple projection (good enough for <230km range)
                lat_offset = range_km * np.cos(np.radians(azimuth)) / 111.0
                lon_offset = range_km * np.sin(np.radians(azimuth)) / (111.0 * np.cos(np.radians(radar_lat)))
                
                gate_lat = radar_lat + lat_offset
                gate_lon = radar_lon + lon_offset
                
                # Get color for this reflectivity value
                color = get_color_from_dbz(dbz_value)
                
                if color:
                    feature = {
                        "type": "Feature",
                        "properties": {
                            "dbz": float(dbz_value),
                            "color": color
                        },
                        "geometry": {
                            "type": "Point",
                            "coordinates": [gate_lon, gate_lat]
                        }
                    }
                    features.append(feature)
        
        print(f"Created {len(features)} features for {site}")
        
        # Create GeoJSON
        geojson = {
            "type": "FeatureCollection",
            "properties": {
                "site": site,
                "timestamp": datetime.utcnow().isoformat() + 'Z',
                "radar_lat": float(radar_lat),
                "radar_lon": float(radar_lon)
            },
            "features": features
        }
        
        return geojson
        
    except Exception as e:
        print(f"Error processing radar file: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_geojson(geojson, site):
    """Save GeoJSON to output directory"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        output_file = os.path.join(OUTPUT_DIR, f'radar-{site.lower()}.geojson')
        
        with open(output_file, 'w') as f:
            json.dump(geojson, f)
        
        print(f"Saved {output_file}")
        
        # Also save a status file with timestamp
        status_file = os.path.join(OUTPUT_DIR, 'status.json')
        status = {
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'sites': {}
        }
        
        # Load existing status if it exists
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status = json.load(f)
        
        status['sites'][site] = {
            'timestamp': geojson['properties']['timestamp'],
            'features': len(geojson['features'])
        }
        status['last_updated'] = datetime.utcnow().isoformat() + 'Z'
        
        with open(status_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        return True
        
    except Exception as e:
        print(f"Error saving GeoJSON: {e}")
        return False

def process_all_radars():
    """Process all radar sites"""
    print(f"=== NEXRAD Radar Processing Started at {datetime.utcnow().isoformat()}Z ===")
    
    for site in RADAR_SITES:
        print(f"\n--- Processing {site} ---")
        
        # Find latest radar file
        download_url = get_latest_radar_file(site)
        if not download_url:
            print(f"Skipping {site} - no recent data found")
            continue
        
        # Download the file
        local_file = download_radar_file(download_url)
        if not local_file:
            print(f"Skipping {site} - download failed")
            continue
        
        # Process the radar data
        geojson = process_radar_file(local_file, site)
        
        # Clean up downloaded file
        try:
            os.unlink(local_file)
        except:
            pass
        
        if geojson:
            # Save the GeoJSON
            save_geojson(geojson, site)
        else:
            print(f"Failed to process {site}")
    
    print(f"\n=== Processing Complete ===")

def main():
    """Main function - starts Flask server and radar processing thread"""
    print("NEXRAD Radar Processor Starting...")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Processing interval: {LOOP_INTERVAL} seconds")
    print(f"Radar sites: {', '.join(RADAR_SITES)}")
    print(f"Data source: NOAA THREDDS Server")
    
    # Create Flask app
    app = Flask(__name__)
    CORS(app)  # Enable CORS for cross-origin requests
    
    @app.route('/')
    def index():
        """Health check endpoint"""
        status_file = os.path.join(OUTPUT_DIR, 'status.json')
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status = json.load(f)
            return jsonify({
                'service': 'LancasterWX Radar Processor',
                'status': 'running',
                'source': 'NOAA THREDDS',
                'last_updated': status.get('last_updated'),
                'sites': status.get('sites'),
                'files': [
                    '/radar-klwx.geojson',
                    '/radar-kdix.geojson',
                    '/status.json'
                ]
            })
        return jsonify({
            'service': 'LancasterWX Radar Processor',
            'status': 'starting',
            'source': 'NOAA THREDDS',
            'message': 'Waiting for first radar update...'
        })
    
    @app.route('/<path:filename>')
    def serve_file(filename):
        """Serve GeoJSON and status files"""
        try:
            return send_from_directory(OUTPUT_DIR, filename)
        except FileNotFoundError:
            return jsonify({
                'error': 'File not found',
                'message': f'{filename} does not exist yet. Radar data is still processing.'
            }), 404
    
    # Start radar processing in background thread
    def radar_loop():
        """Background thread for radar processing"""
        while True:
            try:
                process_all_radars()
            except Exception as e:
                print(f"Error in processing loop: {e}")
                import traceback
                traceback.print_exc()
            
            print(f"\nWaiting {LOOP_INTERVAL} seconds until next update...")
            time.sleep(LOOP_INTERVAL)
    
    # Start background thread
    radar_thread = threading.Thread(target=radar_loop, daemon=True)
    radar_thread.start()
    print("Radar processing thread started")
    
    # Get port from environment (Render sets this)
    port = int(os.environ.get('PORT', 8080))
    print(f"Starting web server on port {port}...")
    
    # Run Flask app
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()
