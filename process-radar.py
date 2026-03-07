#!/usr/bin/env python3 -u
"""
NEXRAD Level 2 Radar Processor for LancasterWX.com
Runs on Railway.app - Downloads and processes radar data from KLWX and KDIX
Outputs GeoJSON to public directory for web access
"""

import os
import sys
import json
import tempfile
from datetime import datetime, timedelta
import time
import threading
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pyart
import numpy as np
from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS

# Configuration
RADAR_SITES = ['KLWX', 'KDIX']  # Sterling VA, Mt Holly NJ
OUTPUT_DIR = '/app/public'  # Railway serves files from /app/public
BUCKET_NAME = 'noaa-nexrad-level2'
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
    """Get the most recent radar file from AWS S3 for a given site"""
    try:
        # Get AWS credentials from environment
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        if not aws_access_key or not aws_secret_key:
            print(f"ERROR: AWS credentials not found in environment!")
            print(f"AWS_ACCESS_KEY_ID present: {bool(aws_access_key)}")
            print(f"AWS_SECRET_ACCESS_KEY present: {bool(aws_secret_key)}")
            return None
        
        # Create S3 client with explicit credentials
        s3 = boto3.client('s3', 
                         region_name='us-east-1',
                         aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_key)
        
        # Get current UTC time
        now = datetime.utcnow()
        
        # Try the last 30 minutes (radar updates every ~5 minutes)
        for minutes_ago in range(0, 30, 5):
            check_time = now - timedelta(minutes=minutes_ago)
            
            # Build S3 prefix: YYYY/MM/DD/SITE/
            prefix = f"{check_time.strftime('%Y/%m/%d')}/{site}/"
            
            print(f"Checking S3 for {site} at {check_time.strftime('%Y-%m-%d %H:%M')}...")
            
            # List objects in the bucket
            response = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=prefix,
                MaxKeys=100
            )
            
            if 'Contents' not in response:
                continue
            
            # Get the most recent file
            files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            
            if files:
                latest_file = files[0]['Key']
                print(f"Found latest file: {latest_file}")
                return latest_file
        
        print(f"No recent radar files found for {site}")
        return None
        
    except Exception as e:
        print(f"Error finding radar file for {site}: {e}")
        return None

def download_radar_file(s3_key):
    """Download radar file from S3 to temp location"""
    try:
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        s3 = boto3.client('s3', 
                         region_name='us-east-1',
                         aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_key)
        
        # Create temp file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.ar2v')
        
        print(f"Downloading {s3_key}...")
        s3.download_file(BUCKET_NAME, s3_key, temp_file.name)
        
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
        s3_key = get_latest_radar_file(site)
        if not s3_key:
            print(f"Skipping {site} - no recent data found")
            continue
        
        # Download the file
        local_file = download_radar_file(s3_key)
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
    print("Radar processing thread started", flush=True)
    
    # Get port from environment (Railway sets this)
    port = int(os.environ.get('PORT', 8080))
    print(f"Starting web server on port {port}...", flush=True)
    
    # Run Flask app
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()
