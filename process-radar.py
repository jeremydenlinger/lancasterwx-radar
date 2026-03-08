#!/usr/bin/env python3 -u
"""
NEXRAD Level 3 Radar Processor for LancasterWX.com
Uses Iowa Environmental Mesonet for pre-processed radar composites
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
from PIL import Image
import numpy as np
from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS

# Configuration
RADAR_SITES = ['KLWX']  # Sterling VA - covers Lancaster area
OUTPUT_DIR = '/tmp/radar-output'  # Use /tmp which is always writable
IEM_BASE = 'https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-900913'
LOOP_INTERVAL = 300  # 5 minutes

# Map bounds for Lancaster area (will fetch tiles covering this region)
LANCASTER_LAT = 40.0379
LANCASTER_LON = -76.3055
TILE_RADIUS = 2  # Fetch tiles in 2-tile radius around Lancaster

# Reflectivity color scale (dBZ to color) - matching Level 2 colors
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

def latlon_to_tile(lat, lon, zoom):
    """Convert lat/lon to tile coordinates at given zoom level"""
    lat_rad = np.radians(lat)
    n = 2.0 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - np.log(np.tan(lat_rad) + (1 / np.cos(lat_rad))) / np.pi) / 2.0 * n)
    return (xtile, ytile)

def tile_to_latlon(xtile, ytile, zoom):
    """Convert tile coordinates to lat/lon (NW corner)"""
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = np.arctan(np.sinh(np.pi * (1 - 2 * ytile / n)))
    lat_deg = np.degrees(lat_rad)
    return (lat_deg, lon_deg)

def download_radar_tile(zoom, x, y):
    """Download a single radar tile from Iowa State Mesonet"""
    try:
        # Iowa State Mesonet tile URL
        url = f"{IEM_BASE}/{zoom}/{x}/{y}.png"
        
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # Save to temp file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            temp_file.write(response.content)
            temp_file.close()
            return temp_file.name
        else:
            return None
            
    except Exception as e:
        print(f"Error downloading tile {zoom}/{x}/{y}: {e}")
        return None

def rgb_to_dbz(r, g, b, a):
    """Convert Iowa State Mesonet color to approximate dBZ value"""
    # If transparent, no data
    if a < 128:
        return None
    
    # Iowa State Mesonet uses standard NWS color scale
    # Approximate dBZ based on color
    if r < 50 and g > 200 and b > 200:
        return 10  # Light blue
    elif r < 50 and g > 150 and b > 200:
        return 15  # Blue
    elif r < 50 and g > 100 and b > 150:
        return 20  # Dark blue
    elif r < 100 and g > 200 and b < 100:
        return 25  # Green
    elif r < 100 and g > 150 and b < 50:
        return 30  # Dark green
    elif r > 200 and g > 200 and b < 100:
        return 35  # Yellow
    elif r > 200 and g > 150 and b < 100:
        return 40  # Orange
    elif r > 200 and g > 100 and b < 50:
        return 45  # Dark orange
    elif r > 200 and g < 100 and b < 50:
        return 50  # Red
    elif r > 150 and g < 50 and b < 50:
        return 55  # Dark red
    elif r > 200 and g < 100 and b > 200:
        return 60  # Magenta
    elif r > 100 and g < 50 and b > 150:
        return 65  # Purple
    else:
        return 20  # Default

def process_radar_composite():
    """Download and process radar tiles into GeoJSON"""
    try:
        print("Processing radar composite...")
        
        # Use zoom level 8 for good detail without too much data
        zoom = 8
        
        # Get center tile for Lancaster
        center_x, center_y = latlon_to_tile(LANCASTER_LAT, LANCASTER_LON, zoom)
        
        print(f"Center tile: {zoom}/{center_x}/{center_y}")
        
        # Download tiles in a grid around Lancaster
        tiles = []
        for dy in range(-TILE_RADIUS, TILE_RADIUS + 1):
            for dx in range(-TILE_RADIUS, TILE_RADIUS + 1):
                tile_x = center_x + dx
                tile_y = center_y + dy
                
                tile_file = download_radar_tile(zoom, tile_x, tile_y)
                if tile_file:
                    tiles.append({
                        'file': tile_file,
                        'x': tile_x,
                        'y': tile_y,
                        'zoom': zoom
                    })
        
        if not tiles:
            print("No radar tiles downloaded")
            return None
        
        print(f"Downloaded {len(tiles)} tiles")
        
        # Process tiles into GeoJSON features
        features = []
        
        for tile in tiles:
            try:
                # Load tile image
                img = Image.open(tile['file'])
                img_array = np.array(img)
                
                # Tile covers lat/lon bounds
                lat_nw, lon_nw = tile_to_latlon(tile['x'], tile['y'], zoom)
                lat_se, lon_se = tile_to_latlon(tile['x'] + 1, tile['y'] + 1, zoom)
                
                # Sample every Nth pixel to reduce data size
                step = 8  # Sample every 8th pixel
                
                height, width = img_array.shape[:2]
                
                for y in range(0, height, step):
                    for x in range(0, width, step):
                        # Get pixel color
                        if img_array.ndim == 3:
                            r, g, b = img_array[y, x, :3]
                            a = img_array[y, x, 3] if img_array.shape[2] == 4 else 255
                        else:
                            continue
                        
                        # Convert color to dBZ
                        dbz = rgb_to_dbz(r, g, b, a)
                        
                        if dbz is None or dbz < 5:
                            continue
                        
                        # Calculate lat/lon for this pixel
                        pixel_lon = lon_nw + (x / width) * (lon_se - lon_nw)
                        pixel_lat = lat_nw + (y / height) * (lat_se - lat_nw)
                        
                        # Get color for this reflectivity value
                        color = get_color_from_dbz(dbz)
                        
                        if color:
                            feature = {
                                "type": "Feature",
                                "properties": {
                                    "dbz": dbz,
                                    "color": color
                                },
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [pixel_lon, pixel_lat]
                                }
                            }
                            features.append(feature)
                
                # Clean up tile file
                os.unlink(tile['file'])
                
            except Exception as e:
                print(f"Error processing tile: {e}")
                continue
        
        print(f"Created {len(features)} features")
        
        # Create GeoJSON
        geojson = {
            "type": "FeatureCollection",
            "properties": {
                "source": "Iowa Environmental Mesonet",
                "product": "NEXRAD Base Reflectivity",
                "timestamp": datetime.utcnow().isoformat() + 'Z',
                "center_lat": LANCASTER_LAT,
                "center_lon": LANCASTER_LON
            },
            "features": features
        }
        
        return geojson
        
    except Exception as e:
        print(f"Error processing radar composite: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_geojson(geojson, filename='radar-composite.geojson'):
    """Save GeoJSON to output directory"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        output_file = os.path.join(OUTPUT_DIR, filename)
        
        with open(output_file, 'w') as f:
            json.dump(geojson, f)
        
        print(f"Saved {output_file}")
        
        # Also save a status file with timestamp
        status_file = os.path.join(OUTPUT_DIR, 'status.json')
        status = {
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'source': 'Iowa Environmental Mesonet',
            'product': 'NEXRAD Level 3 Composite',
            'features': len(geojson['features']),
            'files': [f'/{filename}', '/status.json']
        }
        
        with open(status_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        return True
        
    except Exception as e:
        print(f"Error saving GeoJSON: {e}")
        return False

def process_radar():
    """Main radar processing function"""
    print(f"=== NEXRAD Radar Processing Started at {datetime.utcnow().isoformat()}Z ===")
    
    # Process radar composite
    geojson = process_radar_composite()
    
    if geojson:
        # Save the GeoJSON
        save_geojson(geojson)
    else:
        print("Failed to process radar data")
    
    print(f"=== Processing Complete ===")

def main():
    """Main function - starts Flask server and radar processing thread"""
    print("NEXRAD Level 3 Radar Processor Starting...")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Processing interval: {LOOP_INTERVAL} seconds")
    print(f"Data source: Iowa Environmental Mesonet")
    print(f"Center: Lancaster, PA ({LANCASTER_LAT}, {LANCASTER_LON})")
    
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
                **status
            })
        return jsonify({
            'service': 'LancasterWX Radar Processor',
            'status': 'starting',
            'source': 'Iowa Environmental Mesonet',
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
                process_radar()
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
