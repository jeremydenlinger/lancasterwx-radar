# LancasterWX NEXRAD Radar Processor

Level 2 radar processing service for LancasterWX.com weather map.

## What This Does

- Downloads latest Level 2 NEXRAD radar data from AWS S3 (KLWX and KDIX)
- Processes with Py-ART for high-quality reflectivity data
- Outputs color-coded GeoJSON files every 5 minutes
- Serves files publicly via Railway.app

## Deployment on Railway.app

### Step 1: Create GitHub Repository

1. Go to https://github.com/new
2. Repository name: `lancasterwx-radar`
3. Make it **Public** (so Railway can access it)
4. Click "Create repository"

### Step 2: Upload Files to GitHub

Upload these files to your new repo:
- `process-radar.py` - Main processing script
- `requirements.txt` - Python dependencies
- `railway.json` - Railway configuration
- `README.md` - This file

You can do this via:
- **GitHub web interface:** Click "Add file" → "Upload files"
- **Git command line:**
  ```bash
  git clone https://github.com/YOUR-USERNAME/lancasterwx-radar.git
  cd lancasterwx-radar
  # Copy the 4 files here
  git add .
  git commit -m "Initial commit"
  git push
  ```

### Step 3: Deploy to Railway

1. Go to https://railway.app/new
2. Click "Deploy from GitHub repo"
3. Select your `lancasterwx-radar` repository
4. Click "Deploy Now"

Railway will automatically:
- Detect it's a Python project
- Install dependencies from `requirements.txt`
- Start running `process-radar.py`

### Step 4: Enable Public Networking

1. In Railway dashboard, click your deployed service
2. Go to **Settings** tab
3. Scroll to **Networking** section
4. Click **Generate Domain**
5. You'll get a URL like: `your-app.up.railway.app`

### Step 5: Access Your Radar Data

Your GeoJSON files will be available at:
- `https://your-app.up.railway.app/radar-klwx.geojson`
- `https://your-app.up.railway.app/radar-kdix.geojson`
- `https://your-app.up.railway.app/status.json` (check last update time)

### Step 6: Update Your WordPress Map

In your `lancaster-weather-map-maplibre.html` file, update the `radarDataPath`:

```javascript
const CONFIG = {
    // ...
    radarDataPath: 'https://your-app.up.railway.app/',  // <-- Change this
    // ...
};
```

## Railway Free Tier

- **500 execution hours/month** free
- This service uses ~1 hour per day = **30 hours/month**
- Well within free limits!
- No credit card required for free tier

## Monitoring

Check Railway dashboard to see:
- Live logs from the Python script
- Memory/CPU usage
- Service uptime
- Build/deploy status

## Troubleshooting

**Service keeps restarting:**
- Check logs in Railway dashboard
- Might need to adjust memory limits in Settings

**No radar data appearing:**
- Check logs for errors
- Verify files are in `/app/public/` directory
- Try accessing status.json directly

**Files not publicly accessible:**
- Make sure you generated a domain in Networking settings
- Check that files are being saved to `/app/public/`

## Local Testing (Optional)

To test locally before deploying:

```bash
# Install dependencies
pip install -r requirements.txt

# Set output directory
mkdir -p public

# Edit process-radar.py and change:
OUTPUT_DIR = './public'  # instead of '/app/public'

# Run once (not in loop)
python3 process-radar.py
```

## Cost Estimate

**Railway.app:**
- Free tier: 500 hours/month
- This uses: ~30 hours/month
- Cost: **$0**

**AWS S3 bandwidth:**
- Downloading ~20MB every 5 minutes
- ~5.7GB/month
- AWS doesn't charge for NOAA data downloads
- Cost: **$0**

**Total monthly cost: $0**
