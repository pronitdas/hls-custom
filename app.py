from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
import json
import httpx
from typing import Dict, Any, List, Optional
import os
from datetime import datetime, timedelta
import mercantile
import hashlib
import asyncio
from pathlib import Path
import subprocess
import tempfile
import atexit
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="GeoSense Spectral Intelligence", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Load indices configuration
with open("indices.json", "r") as f:
    config = json.load(f)

TITILER_ENDPOINT = os.getenv("TITILER_ENDPOINT", "http://localhost:8001")

# Multiple STAC endpoints for redundancy
STAC_ENDPOINTS = {
    "earth-search": "https://earth-search.aws.element84.com/v1",
    "microsoft-pc": "https://planetarycomputer.microsoft.com/api/stac/v1", 
    "usgs-stac": "https://landsatlook.usgs.gov/stac-server",
    "nasa-cmr": "https://cmr.earthdata.nasa.gov/stac"
}

# Primary STAC endpoint
STAC_CATALOG_URL = os.getenv("STAC_CATALOG_URL", STAC_ENDPOINTS["earth-search"])

# NASA Earthdata credentials (optional)
NASA_USERNAME = os.getenv("NASA_USERNAME")
NASA_PASSWORD = os.getenv("NASA_PASSWORD")

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

# Embedded Redis instance
redis_process = None
redis_port = 6379
redis_cache = {}

# Optimized for multi-sensor harmonization
SENSOR_COLLECTIONS = {
    "sentinel-2-l2a": {
        "name": "Sentinel-2 Level-2A",
        "band_mapping": {
            # Sentinel-2 has both broad NIR (B8) and narrow NIR (B8A)
            # Using B8A (nir08) for better cross-sensor compatibility
            "blue": "blue",      # B2 - 490nm
            "green": "green",    # B3 - 560nm  
            "red": "red",        # B4 - 665nm
            "nir": "nir08",      # B8A (narrow NIR, more compatible with Landsat)
            "nir08": "nir08",    # B8A - 865nm
            "swir16": "swir16",  # B11 - 1610nm
            "swir22": "swir22",  # B12 - 2190nm
            "rededge1": "rededge1",  # B5 - 705nm
            "rededge2": "rededge2"   # B6 - 740nm
        },
        "priority": 1
    },
    "landsat-c2-l2": {
        "name": "Landsat Collection 2 Level-2",
        "band_mapping": {
            "blue": "blue",      # B2 - 480nm
            "green": "green",    # B3 - 560nm
            "red": "red",        # B4 - 655nm 
            "nir": "nir08",      # B5 (Landsat only has one NIR band)
            "nir08": "nir08",    # B5 - 865nm
            "swir16": "swir16",  # B6 - 1610nm
            "swir22": "swir22"   # B7 - 2200nm
        },
        "priority": 2
    }
}

def get_least_cloud_asset(features: List[Dict]) -> Optional[Dict]:
    """Select the asset with the lowest cloud cover from a list of STAC features"""
    if not features:
        return None

    # Sort by cloud cover (ascending) and return the best one
    sorted_features = sorted(
        features,
        key=lambda f: f['properties'].get('eo:cloud_cover', 100)
    )
    return sorted_features[0]

def get_default_datetime_range(days_back: int = 180) -> str:
    """Generate default datetime range for the last N days (default 6 months)"""
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)
    return f"{start_date.strftime('%Y-%m-%d')}T00:00:00Z/{end_date.strftime('%Y-%m-%d')}T23:59:59Z"

def harmonize_band_expression(expression: str, collection: str) -> str:
    """Convert generic band names to collection-specific band names in expressions"""
    if collection not in SENSOR_COLLECTIONS:
        return expression

    band_mapping = SENSOR_COLLECTIONS[collection]["band_mapping"]
    harmonized_expression = expression

    # Use regex for word boundary replacement to avoid partial matches
    # Sort by length descending to replace longer band names first (e.g., nir08 before nir)
    sorted_bands = sorted(band_mapping.items(), key=lambda x: len(x[0]), reverse=True)
    
    for generic_name, specific_name in sorted_bands:
        # Only replace if generic and specific names are different
        if generic_name != specific_name:
            # Use word boundaries to ensure we don't replace parts of other band names
            pattern = r'\b' + re.escape(generic_name) + r'\b'
            harmonized_expression = re.sub(pattern, specific_name, harmonized_expression)

    return harmonized_expression

def get_best_assets(features: List[Dict]) -> List[Dict]:
    """Get the absolute best assets for vivid, seamless imagery from multiple sensors"""
    if not features:
        return []

    # Group by sensor collection
    sensor_groups = {}
    for feature in features:
        collection = feature.get('collection', 'unknown')
        if collection not in sensor_groups:
            sensor_groups[collection] = []
        sensor_groups[collection].append(feature)

    # Get best from each sensor
    best_assets = []
    
    # Sort each sensor group by cloud cover and date
    for collection, assets in sensor_groups.items():
        sorted_assets = sorted(
            assets,
            key=lambda f: (
                f['properties'].get('eo:cloud_cover', 100),
                -int(f['properties'].get('datetime', '2000-01-01T00:00:00Z')[:4])  # Recent year first
            )
        )
        # Take best asset from each sensor
        if sorted_assets:
            best_assets.append(sorted_assets[0])
    
    # Final sort by priority and cloud cover
    best_assets = sorted(
        best_assets,
        key=lambda f: (
            SENSOR_COLLECTIONS.get(f.get('collection', ''), {}).get('priority', 999),
            f['properties'].get('eo:cloud_cover', 100)
        )
    )

    # Return up to 10 assets for gap-filling, ensuring multi-sensor coverage
    return best_assets[:10]

def start_embedded_redis():
    """Start embedded Redis server"""
    global redis_process
    if redis_process is None:
        try:
            redis_process = subprocess.Popen(
                ['redis-server', '--port', str(redis_port), '--save', '', '--appendonly', 'no'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            # Wait a moment for Redis to start
            import time
            time.sleep(1)
        except FileNotFoundError:
            # Redis not installed, use in-memory cache
            print("Redis not found, using in-memory cache")
            redis_process = None

def stop_embedded_redis():
    """Stop embedded Redis server"""
    global redis_process
    if redis_process:
        redis_process.terminate()
        redis_process.wait()
        redis_process = None

def get_from_cache(key: str) -> str:
    """Get value from cache (Redis or in-memory)"""
    if redis_process:
        try:
            import redis
            r = redis.Redis(host='localhost', port=redis_port, decode_responses=True)
            return r.get(key)
        except Exception:
            pass
    return redis_cache.get(key)

def set_in_cache(key: str, value: str, expire: int = 3600):
    """Set value in cache (Redis or in-memory)"""
    if redis_process:
        try:
            import redis
            r = redis.Redis(host='localhost', port=redis_port, decode_responses=True)
            r.setex(key, expire, value)
            return
        except Exception:
            pass
    redis_cache[key] = value

def clear_cache():
    """Clear all cache"""
    if redis_process:
        try:
            import redis
            r = redis.Redis(host='localhost', port=redis_port, decode_responses=True)
            r.flushdb()
        except Exception:
            pass
    redis_cache.clear()

def tile_to_bbox(z: int, x: int, y: int) -> List[float]:
    """Convert tile coordinates to bounding box"""
    bbox = mercantile.bounds(x, y, z)
    return [bbox.west, bbox.south, bbox.east, bbox.north]

def generate_cache_key(prefix: str, *args) -> str:
    """Generate a cache key from arguments"""
    key_data = f"{prefix}:{':'.join(map(str, args))}"
    return hashlib.md5(key_data.encode()).hexdigest()

async def discover_tile_assets(z: int, x: int, y: int, max_cloud_cover: float = 10.0) -> List[Dict]:
    """Discover best available assets for a specific tile from multiple sensors with zoom-aware selection"""
    cache_key = generate_cache_key("tile_assets", z, x, y, max_cloud_cover)

    # Try cache first
    cached = get_from_cache(cache_key)
    if cached:
        return json.loads(cached)

    # Convert tile to bbox
    bbox = tile_to_bbox(z, x, y)
    bbox_str = ",".join(map(str, bbox))

    # Zoom-aware asset discovery - adjust search strategy based on zoom level
    if z <= 8:  # Lower zoom levels need more coverage
        datetime_range = get_default_datetime_range(1095)  # Last 3 years
        limit = 300  # More assets for overview levels
        cloud_threshold = min(max_cloud_cover * 2, 50)  # More lenient for overviews
    else:  # Higher zoom levels can be more selective
        datetime_range = get_default_datetime_range(730)  # Last 2 years
        limit = 200
        cloud_threshold = max_cloud_cover
    
    logger.info(f"Zoom {z}: searching {limit} assets over {datetime_range.split('/')[0][:4]}-{datetime_range.split('/')[1][:4]} with <{cloud_threshold}% clouds")

    # Search BOTH Sentinel-2 and Landsat for comprehensive coverage
    params = {
        "collections": "sentinel-2-l2a,landsat-c2-l2",  # Multi-sensor search
        "bbox": bbox_str,
        "datetime": datetime_range,
        "limit": limit,
        "query": json.dumps({"eo:cloud_cover": {"lt": cloud_threshold}})
    }

    try:
        # Use fallback STAC search
        data = await search_stac_with_fallback(params)

        features = data.get("features", [])

        # Add sensor info and prioritize
        for feature in features:
            collection = feature.get("collection", "")
            if collection in SENSOR_COLLECTIONS:
                feature["properties"]["sensor_info"] = SENSOR_COLLECTIONS[collection]

        # Get best coverage assets from BOTH sensors
        best_assets = get_best_assets(features)
        
        # Debug: Log sensor distribution
        sensor_count = {}
        for asset in best_assets:
            collection = asset.get('collection', 'unknown')
            sensor_count[collection] = sensor_count.get(collection, 0) + 1
        print(f"Tile {z}/{x}/{y} - Found {len(features)} total features, selected {len(best_assets)} best assets")
        print(f"Sensor distribution in best assets: {sensor_count}")

        # Cache for 1 hour
        set_in_cache(cache_key, json.dumps(best_assets), 3600)
        return best_assets

    except Exception as e:
        print(f"Asset discovery error for tile {z}/{x}/{y}: {e}")
        return []

async def create_mosaic_json(assets: List[Dict]) -> Dict:
    """Create a MosaicJSON from multiple STAC assets for seamless coverage"""
    if not assets:
        return {}

    # Sort assets by priority and cloud cover for optimal ordering
    sorted_assets = sorted(assets, key=lambda a: (
        SENSOR_COLLECTIONS.get(a.get("collection", ""), {}).get("priority", 999),
        a["properties"].get("eo:cloud_cover", 100)
    ))

    # Create MosaicJSON structure
    mosaic_json = {
        "mosaicjson": "1.0.0",
        "name": "Global Harmonized Mosaic",
        "description": "Multi-sensor harmonized Earth observation mosaic",
        "version": "1.0.0",
        "attribution": "Sentinel-2, Landsat via Earth Search",
        "minzoom": 0,
        "maxzoom": 18,
        "quadkey_zoom": 12,
        "bounds": [-180, -85, 180, 85],
        "center": [0, 0, 2],
        "tiles": {}
    }

    # Add each asset as a tile source
    for asset in sorted_assets:
        stac_url = f"{STAC_CATALOG_URL}/collections/{asset['collection']}/items/{asset['id']}"

        # Get the geometry bounds for this asset
        if "bbox" in asset:
            bbox = asset["bbox"]
            # Convert bbox to quadkeys for the mosaic
            # For now, use a simplified approach - add to all relevant tiles
            mosaic_json["tiles"]["default"] = mosaic_json["tiles"].get("default", [])
            mosaic_json["tiles"]["default"].append(stac_url)

    return mosaic_json

async def get_harmonized_stac_urls(assets: List[Dict]) -> List[str]:
    """Get STAC URLs for seamless mosaic rendering"""
    if not assets:
        return []

    # Return STAC URLs for best assets (already sorted)
    return [f"{STAC_CATALOG_URL}/collections/{asset['collection']}/items/{asset['id']}"
            for asset in assets]

async def search_stac_with_fallback(params: dict, max_retries: int = 3) -> dict:
    """Search STAC with multiple endpoint fallback"""
    
    # Try primary endpoint first
    endpoints_to_try = [STAC_CATALOG_URL] + [url for url in STAC_ENDPOINTS.values() if url != STAC_CATALOG_URL]
    
    for i, endpoint in enumerate(endpoints_to_try):
        try:
            # Setup authentication if needed
            auth = None
            if "earthdata.nasa.gov" in endpoint and NASA_USERNAME and NASA_PASSWORD:
                auth = (NASA_USERNAME, NASA_PASSWORD)
            
            timeout = httpx.Timeout(15.0 + i * 5)  # Increase timeout for each retry
            
            async with httpx.AsyncClient(timeout=timeout, auth=auth) as client:
                logger.info(f"Trying STAC endpoint {i+1}/{len(endpoints_to_try)}: {endpoint.split('/')[-2] if '/' in endpoint else endpoint}")
                
                response = await client.get(f"{endpoint}/search", params=params)
                
                if response.status_code == 200:
                    logger.info(f"STAC success with endpoint: {endpoint.split('/')[-2] if '/' in endpoint else endpoint}")
                    return response.json()
                else:
                    logger.warning(f"STAC endpoint failed with {response.status_code}: {endpoint}")
                    
        except Exception as e:
            logger.error(f"STAC endpoint error: {endpoint} - {str(e)[:100]}")
            continue
    
    # If all endpoints fail, raise an exception
    raise HTTPException(status_code=503, detail="All STAC endpoints unavailable")

async def test_titiler_connection() -> bool:
    """Test if TiTiler is accessible"""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
            response = await client.get(f"{TITILER_ENDPOINT}/health")
            return response.status_code == 200
    except Exception:
        return False

@app.get("/")
async def root():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/indices")
async def get_indices():
    """Get all available spectral indices"""
    return config["indices"]

@app.get("/api/rgb")
async def get_rgb_composites():
    """Get all RGB composite definitions"""
    return config["rgb_composites"]

@app.get("/api/sensors")
async def get_sensors():
    """Get all available sensor collections and their band mappings"""
    return SENSOR_COLLECTIONS

@app.get("/api/stac/health")
async def get_stac_health():
    """Check health of all STAC endpoints"""
    health_status = {}
    
    for name, endpoint in STAC_ENDPOINTS.items():
        try:
            auth = None
            if "earthdata.nasa.gov" in endpoint and NASA_USERNAME and NASA_PASSWORD:
                auth = (NASA_USERNAME, NASA_PASSWORD)
                
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0), auth=auth) as client:
                # Simple health check
                response = await client.get(f"{endpoint}/collections", params={"limit": 1})
                health_status[name] = {
                    "url": endpoint,
                    "status": "healthy" if response.status_code == 200 else f"error_{response.status_code}",
                    "response_time": "<5s" if response.status_code == 200 else "timeout"
                }
        except Exception as e:
            health_status[name] = {
                "url": endpoint,
                "status": "error",
                "error": str(e)[:100]
            }
    
    return {
        "primary_endpoint": STAC_CATALOG_URL,
        "endpoints": health_status,
        "nasa_auth_configured": bool(NASA_USERNAME and NASA_PASSWORD)
    }

@app.get("/api/collections")
async def get_collections():
    """Get available STAC collections with metadata"""
    collections_info = {}
    for collection_id, info in SENSOR_COLLECTIONS.items():
        collections_info[collection_id] = {
            "name": info["name"],
            "bands": list(info["band_mapping"].keys()),
            "native_bands": list(info["band_mapping"].values()),
            "priority": info.get("priority", 999)
        }
    return collections_info

@app.get("/api/tile-info/{z}/{x}/{y}")
async def get_tile_info(z: int, x: int, y: int, max_cloud_cover: float = 10.0):
    """Get information about available imagery for a specific tile"""
    assets = await discover_tile_assets(z, x, y, max_cloud_cover)

    if not assets:
        return {
            "tile": f"{z}/{x}/{y}",
            "bbox": tile_to_bbox(z, x, y),
            "available_assets": 0,
            "sensors": [],
            "coverage": "none"
        }

    sensors_used = list(set(asset.get("collection", "") for asset in assets))

    return {
        "tile": f"{z}/{x}/{y}",
        "bbox": tile_to_bbox(z, x, y),
        "available_assets": len(assets),
        "sensors": sensors_used,
        "coverage": "full" if len(assets) >= 2 else "partial",
        "best_asset": {
            "collection": assets[0].get("collection"),
            "datetime": assets[0]["properties"].get("datetime"),
            "cloud_cover": assets[0]["properties"].get("eo:cloud_cover", 0)
        } if assets else None
    }

@app.post("/api/cache/clear")
async def clear_cache_endpoint():
    """Clear all caches"""
    try:
        # Clear Redis/memory cache
        clear_cache()
        
        # Clear file cache
        import shutil
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(exist_ok=True)

        return {"status": "success", "message": "All caches cleared"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/cache/stats")
async def get_cache_stats():
    """Get cache statistics"""
    try:
        # Count cached tiles
        tile_count = len(list(CACHE_DIR.glob("*.png"))) if CACHE_DIR.exists() else 0
        
        cache_info = {
            "memory_cache_keys": len(redis_cache),
            "redis_running": redis_process is not None
        }
        
        if redis_process:
            try:
                import redis
                r = redis.Redis(host='localhost', port=redis_port, decode_responses=True)
                info = r.info()
                cache_info.update({
                    "used_memory_human": info.get("used_memory_human", "0B"),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0)
                })
            except Exception:
                pass

        return {
            "cache": cache_info,
            "file_cache": {
                "cached_tiles": tile_count,
                "cache_dir": str(CACHE_DIR)
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stac/search")
async def stac_search(
    bbox: str = None,
    datetime: str = None,
    collections: str = "sentinel-2-l2a,landsat-c2-l2",
    limit: int = 10,
    max_cloud_cover: float = 1.0,
    auto_datetime: bool = False
):
    """Search STAC catalog for imagery with bbox (legacy support)"""
    """Search STAC catalog for imagery with cloud filtering and multi-sensor harmonization"""

    # Use default 2-year lookback if auto_datetime is enabled
    if auto_datetime and not datetime:
        datetime = get_default_datetime_range()

    params = {
        "collections": collections,
        "limit": limit
    }

    if bbox:
        params["bbox"] = bbox
    if datetime:
        params["datetime"] = datetime

    # Add cloud cover filtering
    query_filter = {
        "eo:cloud_cover": {"lt": max_cloud_cover}
    }
    params["query"] = json.dumps(query_filter)

    # Use fallback STAC search for reliability
    data = await search_stac_with_fallback(params)

    # Add sensor information to each feature
    for feature in data.get("features", []):
        collection = feature.get("collection", "")
        if collection in SENSOR_COLLECTIONS:
            feature["properties"]["sensor_info"] = SENSOR_COLLECTIONS[collection]

        # For full coverage, get the best assets from multiple sensors
        if bbox and data.get("features"):
            full_coverage_assets = get_best_assets(data["features"])
            data["full_coverage"] = full_coverage_assets
            data["coverage_summary"] = {
                "total_available": len(data["features"]),
                "full_coverage_count": len(full_coverage_assets),
                "sensors_used": list(set(asset.get("collection", "") for asset in full_coverage_assets))
            }

        return data

@app.post("/api/stac/polygon-search")
async def polygon_stac_search(
    request_data: dict
):
    """Search STAC catalog using polygon geometry for PDD reports"""
    geometry = request_data.get("geometry")
    datetime = request_data.get("datetime")
    collections = request_data.get("collections", "sentinel-2-l2a,landsat-c2-l2")
    limit = request_data.get("limit", 50)
    max_cloud_cover = request_data.get("max_cloud_cover", 10.0)
    
    if not geometry:
        raise HTTPException(status_code=400, detail="Geometry is required")
    
    # Use default datetime if not provided
    if not datetime:
        datetime = get_default_datetime_range(365)
    
    params = {
        "collections": collections,
        "limit": limit,
        "intersects": json.dumps(geometry),
        "datetime": datetime,
        "query": json.dumps({"eo:cloud_cover": {"lt": max_cloud_cover}})
    }
    
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(20.0),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    ) as client:
        response = await client.get(f"{STAC_CATALOG_URL}/search", params=params)
        response.raise_for_status()
        data = response.json()
        
        # Add sensor information and harmonization
        for feature in data.get("features", []):
            collection = feature.get("collection", "")
            if collection in SENSOR_COLLECTIONS:
                feature["properties"]["sensor_info"] = SENSOR_COLLECTIONS[collection]
        
        # Get best assets for analysis
        if data.get("features"):
            best_assets = get_best_assets(data["features"])
            data["analysis_ready"] = {
                "best_assets": best_assets[:5],  # Top 5 for analysis
                "total_found": len(data["features"]),
                "sensors_available": list(set(f.get("collection", "") for f in best_assets)),
                "avg_cloud_cover": sum(f["properties"].get("eo:cloud_cover", 0) for f in best_assets) / len(best_assets) if best_assets else 0
            }
        
        return data

@app.get("/api/best-cloudfree")
async def best_cloudfree_tile(
    bbox: str,
    index_id: str = "NDVI",
    datetime_range: str = None,
    collections: str = "sentinel-2-l2a,landsat-c2-l2",
    max_cloud_cover: float = 1.0
):
    """Get the best cloud-free imagery for a given AOI and index"""

    # Use default 2-year lookback if no datetime range provided
    if not datetime_range:
        datetime_range = get_default_datetime_range()

    # Search for imagery with strict cloud filtering
    search_params = {
        "bbox": bbox,
        "datetime": datetime_range,
        "collections": collections,
        "limit": 50,  # Get more options to choose from
        "max_cloud_cover": max_cloud_cover
    }

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    ) as client:
        response = await client.get(f"{STAC_CATALOG_URL}/search", params={
            "collections": search_params["collections"],
            "bbox": search_params["bbox"],
            "datetime": search_params["datetime"],
            "limit": search_params["limit"],
            "query": json.dumps({"eo:cloud_cover": {"lt": search_params["max_cloud_cover"]}})
        })
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="STAC search failed")

        data = response.json()
        features = data.get("features", [])

        if not features:
            raise HTTPException(status_code=404, detail="No cloud-free imagery found for the specified criteria")

        # Select the best asset (lowest cloud cover)
        best_asset = get_least_cloud_asset(features)

        if not best_asset:
            raise HTTPException(status_code=404, detail="No suitable imagery found")

        # Get collection info for band harmonization
        collection = best_asset.get("collection", "")

        return {
            "stac_item": best_asset,
            "collection": collection,
            "sensor_info": SENSOR_COLLECTIONS.get(collection, {}),
            "cloud_cover": best_asset["properties"].get("eo:cloud_cover", 0),
            "datetime": best_asset["properties"].get("datetime"),
            "recommended_url": best_asset["assets"]["visual"]["href"] if "visual" in best_asset.get("assets", {}) else None
        }

@app.get("/api/timelapse/{index_id}")
async def get_timelapse(
    index_id: str,
    bbox: str,
    start_date: str,
    end_date: str,
    collections: str = "sentinel-2-l2a,landsat-c2-l2",
    max_cloud_cover: float = 5.0,
    limit: int = 100
):
    """Get time-series imagery for timelapse generation"""

    if index_id not in config["indices"]:
        raise HTTPException(status_code=404, detail="Index not found")

    datetime_range = f"{start_date}/{end_date}"

    # Search for time-series imagery
    search_params = {
        "bbox": bbox,
        "datetime": datetime_range,
        "collections": collections,
        "limit": limit,
        "max_cloud_cover": max_cloud_cover
    }

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    ) as client:
        response = await client.get(f"{STAC_CATALOG_URL}/search", params={
            "collections": search_params["collections"],
            "bbox": search_params["bbox"],
            "datetime": search_params["datetime"],
            "limit": search_params["limit"],
            "query": json.dumps({"eo:cloud_cover": {"lt": search_params["max_cloud_cover"]}})
        })
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="STAC search failed")

        data = response.json()
        features = data.get("features", [])

        if not features:
            raise HTTPException(status_code=404, detail="No imagery found for timelapse")

        # Sort by datetime for chronological order
        sorted_features = sorted(
            features,
            key=lambda f: f["properties"].get("datetime", "")
        )

        # Prepare timelapse data
        timelapse_items = []
        for feature in sorted_features:
            collection = feature.get("collection", "")
            timelapse_items.append({
                "datetime": feature["properties"].get("datetime"),
                "cloud_cover": feature["properties"].get("eo:cloud_cover", 0),
                "stac_url": feature["assets"]["visual"]["href"] if "visual" in feature.get("assets", {}) else None,
                "collection": collection,
                "sensor_info": SENSOR_COLLECTIONS.get(collection, {}),
                "feature": feature
            })

        return {
            "index_id": index_id,
            "total_items": len(timelapse_items),
            "date_range": f"{start_date} to {end_date}",
            "items": timelapse_items
        }

@app.get("/tiles/{index_id}/{z}/{x}/{y}.png")
async def get_global_tile(
    index_id: str,
    z: int,
    x: int,
    y: int,
    colormap: str = None,
    rescale: str = None,
    max_cloud_cover: float = 20.0
):
    """Global harmonized tile endpoint - works anywhere on Earth"""
    if index_id not in config["indices"]:
        raise HTTPException(status_code=404, detail="Index not found")

    # Generate cache key for the tile
    cache_key = generate_cache_key("tile", index_id, z, x, y, colormap or "default", rescale or "auto", max_cloud_cover)

    # Check file cache first
    tile_cache_path = CACHE_DIR / f"{cache_key}.png"
    if tile_cache_path.exists():
        with open(tile_cache_path, "rb") as f:
            return Response(content=f.read(), media_type="image/png")

    # Discover assets for this tile
    assets = await discover_tile_assets(z, x, y, max_cloud_cover)
    if not assets:
        raise HTTPException(status_code=404, detail="No imagery available for this tile")

    # Get multiple STAC URLs for mosaic
    stac_urls = await get_harmonized_stac_urls(assets)
    if not stac_urls:
        raise HTTPException(status_code=404, detail="No suitable imagery found")

    # Get index configuration
    index_config = config["indices"][index_id]
    expression = index_config["expression"]
    default_colormap = index_config.get("colormap", "viridis")

    # Use best STAC URL for seamless quality
    if not stac_urls:
        raise HTTPException(status_code=404, detail="No STAC URLs available")
    
    stac_url = stac_urls[0]  # Use absolute best asset
    best_asset = assets[0]
    collection = best_asset.get("collection", "")

    # Apply band harmonization
    harmonized_expression = expression
    if collection in SENSOR_COLLECTIONS:
        harmonized_expression = harmonize_band_expression(expression, collection)
        print(f"Index: {index_id}, Collection: {collection}")
        print(f"Original expression: {expression}")
        print(f"Harmonized expression: {harmonized_expression}")

    # Build TiTiler request
    params = {
        "url": stac_url,
        "expression": harmonized_expression,
        "colormap_name": colormap or default_colormap,
        "asset_as_band": "true",
        "resampling": "bilinear"
    }

    if rescale:
        params["rescale"] = rescale
    elif "rescale" in index_config:
        params["rescale"] = index_config["rescale"]

    # Request tile from TiTiler
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        titiler_url = f"{TITILER_ENDPOINT}/stac/tiles/WebMercatorQuad/{z}/{x}/{y}.png"
        response = await client.get(titiler_url, params=params)
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=f"TiTiler error: {response.text}")
        
        tile_data = response.content

    # Cache the successful tile
    try:
        with open(tile_cache_path, "wb") as f:
            f.write(tile_data)
    except Exception:
        pass  # Cache error, continue

    return Response(content=tile_data, media_type="image/png")

@app.get("/api/tiles/{index_id}/{z}/{x}/{y}.png")
async def get_index_tile(
    index_id: str,
    z: int,
    x: int,
    y: int,
    stac_url: str,
    collection: str = None,
    colormap: str = None,
    rescale: str = None
):
    """Get spectral index tile using STAC with band harmonization"""
    if index_id not in config["indices"]:
        raise HTTPException(status_code=404, detail="Index not found")

    index_config = config["indices"][index_id]
    expression = index_config["expression"]
    default_colormap = index_config.get("colormap", "viridis")

    # Apply band harmonization if collection is specified
    if collection and collection in SENSOR_COLLECTIONS:
        expression = harmonize_band_expression(expression, collection)

    params = {
        "url": stac_url,
        "expression": expression,
        "colormap_name": colormap or default_colormap,
        "asset_as_band": "true"
    }

    if rescale:
        params["rescale"] = rescale
    else:
        # Default rescaling for spectral indices (typically -1 to 1 range)
        if index_id in ["NDVI", "EVI", "SAVI", "GNDVI", "ARVI", "NDWI", "MNDWI", "NBR", "NDBI"]:
            params["rescale"] = "-1,1"
        else:
            # For other indices, use a broader range
            params["rescale"] = "-2,2"

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
    ) as client:
        response = await client.get(
            f"{TITILER_ENDPOINT}/stac/tiles/WebMercatorQuad/{z}/{x}/{y}.png",
            params=params
        )
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        return Response(
            content=response.content,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"}
        )

@app.get("/tiles/rgb/{composite}/{z}/{x}/{y}.png")
async def get_global_rgb_tile(
    composite: str,
    z: int,
    x: int,
    y: int,
    rescale: str = None,
    max_cloud_cover: float = 5.0  # Stricter cloud filtering for RGB
):
    """Global harmonized RGB tile endpoint with zoom-aware asset selection"""
    # Adjust cloud cover tolerance for different zoom levels
    if z <= 6:  # Very low zoom - be more permissive
        effective_cloud_cover = min(max_cloud_cover * 4, 80)
    elif z <= 8:  # Low zoom - somewhat permissive  
        effective_cloud_cover = min(max_cloud_cover * 2, 40)
    else:  # High zoom - use requested threshold
        effective_cloud_cover = max_cloud_cover
        
    logger.info(f"RGB tile z={z}: using {effective_cloud_cover}% cloud threshold for overviews")
    
    if composite not in config["rgb_composites"]:
        raise HTTPException(status_code=404, detail="RGB composite not found")

    # Generate cache key
    cache_key = generate_cache_key("rgb_tile", composite, z, x, y, rescale or "auto", max_cloud_cover)

    # Check file cache
    tile_cache_path = CACHE_DIR / f"{cache_key}.png"
    if tile_cache_path.exists():
        with open(tile_cache_path, "rb") as f:
            return Response(content=f.read(), media_type="image/png")

    # Discover assets with zoom-aware strategy
    assets = await discover_tile_assets(z, x, y, effective_cloud_cover)
    if not assets and effective_cloud_cover < 80:
        # Fallback: try with even higher cloud tolerance for overviews
        fallback_threshold = 80.0
        logger.info(f"No assets found with {effective_cloud_cover}% cloud cover, trying with {fallback_threshold}%")
        assets = await discover_tile_assets(z, x, y, fallback_threshold)
    
    # Special handling for low zoom overviews
    if not assets and z <= 8:
        logger.info(f"No assets at zoom {z}, trying global overview search")
        # Convert tile to larger bbox for overview search
        bbox = tile_to_bbox(z, x, y)
        bbox_str = ",".join(map(str, bbox))
        
        # Very permissive global search for overviews
        global_params = {
            "collections": "sentinel-2-l2a,landsat-c2-l2",
            "bbox": bbox_str,
            "datetime": get_default_datetime_range(1460),  # 4 years
            "limit": 500,
            "query": json.dumps({"eo:cloud_cover": {"lt": 95}})  # Very permissive
        }
        
        try:
            # Use fallback STAC search for global overview
            data = await search_stac_with_fallback(global_params)
            features = data.get("features", [])
            if features:
                for feature in features:
                    collection = feature.get("collection", "")
                    if collection in SENSOR_COLLECTIONS:
                        feature["properties"]["sensor_info"] = SENSOR_COLLECTIONS[collection]
                assets = get_best_assets(features)
                logger.info(f"Global overview search found {len(assets)} assets")
        except Exception as e:
            logger.error(f"Global overview search failed: {e}")
    
    if not assets:
        raise HTTPException(status_code=404, detail=f"No imagery available for tile z={z} even with global search")

    # Get multiple STAC URLs for mosaic
    stac_urls = await get_harmonized_stac_urls(assets)
    if not stac_urls:
        raise HTTPException(status_code=404, detail="No suitable imagery found")

    # Get RGB configuration
    rgb_config = config["rgb_composites"][composite]
    base_bands = rgb_config["bands"]

    # Use multiple STAC URLs for gap-filling
    if not stac_urls:
        raise HTTPException(status_code=404, detail="No STAC URLs available")
    
    # Implement gap-filling with multiple assets
    if len(stac_urls) > 1:
        # Use multiple assets for gap-filling
        stac_urls_for_mosaic = stac_urls[:5]  # Top 5 assets for gap-filling
        logger.info(f"Gap-filling with {len(stac_urls_for_mosaic)} assets from sensors: {[assets[i].get('collection') for i in range(len(stac_urls_for_mosaic))]}")
    else:
        stac_urls_for_mosaic = stac_urls[:1]
        logger.info(f"Single asset: {assets[0].get('collection')}")
    
    # Use primary asset for harmonization reference
    best_asset = assets[0]
    collection = best_asset.get("collection", "")

    # Apply band harmonization - CRITICAL FIX
    harmonized_bands = base_bands.copy()
    if collection in SENSOR_COLLECTIONS:
        band_mapping = SENSOR_COLLECTIONS[collection]["band_mapping"]
        harmonized_bands = [band_mapping.get(band, band) for band in base_bands]
        # Harmonization working correctly
        logger.debug(f"Bands harmonized for {collection}: {base_bands} -> {harmonized_bands}")

    # Build TiTiler request with primary asset (fallback implemented in request logic)
    primary_stac_url = stac_urls_for_mosaic[0]
    params = {
        "url": primary_stac_url,
        "asset_as_band": "true",
        "resampling": "bilinear", 
        "return_mask": "false",
        "nodata": "0"  # Handle no-data areas
    }
    
    # Add each band as a separate 'assets' parameter
    assets_params = [("assets", band) for band in harmonized_bands]

    if rescale:
        params["rescale"] = rescale
    elif "rescale" in rgb_config:
        params["rescale"] = rgb_config["rescale"]
    else:
        params["rescale"] = "0,3500"  # Vivid rescaling

    # Request tile from TiTiler
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        titiler_url = f"{TITILER_ENDPOINT}/stac/tiles/WebMercatorQuad/{z}/{x}/{y}.png"
        
        # Build URL with gap-filling fallback logic
        from urllib.parse import urlencode
        query_params = [(k, v) for k, v in params.items()]
        query_params.extend(assets_params)
        query_string = urlencode(query_params)
        full_url = f"{titiler_url}?{query_string}"
        
        logger.info(f"Gap-filling: trying {len(stac_urls_for_mosaic)} assets, {len(harmonized_bands)} bands")
        
        # Try assets in order until we get a successful response
        response = None
        for i, stac_url in enumerate(stac_urls_for_mosaic):
            try:
                # Update URL for this asset
                params["url"] = stac_url
                query_params = [(k, v) for k, v in params.items()]
                query_params.extend(assets_params)
                query_string = urlencode(query_params)
                current_url = f"{titiler_url}?{query_string}"
                
                logger.info(f"Trying asset {i+1}/{len(stac_urls_for_mosaic)}: {assets[i].get('collection')}")
                response = await client.get(current_url)
                
                if response.status_code == 200:
                    logger.info(f"Success with asset {i+1}: {assets[i].get('collection')}")
                    break
                else:
                    logger.warning(f"Asset {i+1} failed with status {response.status_code}")
                    if i == len(stac_urls_for_mosaic) - 1:  # Last asset
                        break
            except Exception as e:
                logger.error(f"Asset {i+1} error: {e}")
                if i == len(stac_urls_for_mosaic) - 1:  # Last asset
                    break
        
        if not response or response.status_code != 200:
            error_detail = response.text if response else "No response from any asset"
            logger.error(f"All assets failed. Last error: {error_detail}")
            raise HTTPException(status_code=404, detail=f"Gap-filling failed: {error_detail}")
        
        tile_data = response.content

    # Cache the successful tile
    try:
        with open(tile_cache_path, "wb") as f:
            f.write(tile_data)
    except Exception:
        pass

    return Response(content=tile_data, media_type="image/png")

@app.get("/api/tiles/rgb/{composite}/{z}/{x}/{y}.png")
async def get_rgb_tile(
    composite: str,
    z: int,
    x: int,
    y: int,
    stac_url: str,
    collection: str = None,
    rescale: str = None
):
    """Get RGB composite tile using STAC with band harmonization"""
    if composite not in config["rgb_composites"]:
        raise HTTPException(status_code=404, detail="RGB composite not found")

    rgb_config = config["rgb_composites"][composite]
    bands = rgb_config["bands"]

    # Apply band harmonization if collection is specified or extract from STAC URL
    if not collection:
        # Extract collection from STAC URL
        import re
        match = re.search(r'/collections/([^/]+)/', stac_url)
        if match:
            collection = match.group(1)
    
    print(f"RGB collection detected: {collection}")
    print(f"RGB original bands: {bands}")
    
    if collection and collection in SENSOR_COLLECTIONS:
        band_mapping = SENSOR_COLLECTIONS[collection]["band_mapping"]
        harmonized_bands = []
        for band in bands:
            harmonized_bands.append(band_mapping.get(band, band))
        bands = harmonized_bands
        print(f"RGB harmonized bands: {bands}")

    # Build params with separate assets parameters
    params = {
        "url": stac_url,
        "asset_as_band": "true"
    }
    # Add each band as a separate 'assets' parameter
    assets_params = [("assets", band) for band in bands]

    if rescale:
        params["rescale"] = rescale
    else:
        # Vivid rescaling for maximum color saturation
        params["rescale"] = "0,3500"

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
    ) as client:
        # Build URL with multiple assets parameters
        from urllib.parse import urlencode
        query_params = [(k, v) for k, v in params.items()]
        query_params.extend(assets_params)
        query_string = urlencode(query_params)
        titiler_url = f"{TITILER_ENDPOINT}/stac/tiles/WebMercatorQuad/{z}/{x}/{y}.png"
        full_url = f"{titiler_url}?{query_string}"
        
        response = await client.get(full_url)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        return Response(
            content=response.content,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"}
        )

@app.get("/api/info")
async def get_cog_info(url: str):
    """Get COG information"""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{TITILER_ENDPOINT}/cog/info", params={"url": url})
        return response.json()

@app.get("/api/statistics/{index_id}")
async def get_index_statistics(
    index_id: str,
    url: str,
    bbox: str = None
):
    """Get statistics for spectral index"""
    if index_id not in config["indices"]:
        raise HTTPException(status_code=404, detail="Index not found")
    
    index_config = config["indices"][index_id]
    expression = index_config["expression"]
    
    params = {
        "url": url,
        "expression": expression
    }
    
    if bbox:
        params["bbox"] = bbox
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{TITILER_ENDPOINT}/cog/statistics",
            params=params
        )
        return response.json()

@app.post("/api/export/{format}")
async def export_analysis(
    format: str,
    request_data: dict
):
    """Export spectral index analysis as PNG/JPG for PDD reports"""
    
    # Validate format
    if format not in ["png", "jpg", "jpeg"]:
        raise HTTPException(status_code=400, detail="Format must be png, jpg, or jpeg")
    
    # Extract parameters
    index_id = request_data.get("index_id")
    geometry = request_data.get("geometry")
    width = request_data.get("width", 1024)
    height = request_data.get("height", 1024)
    colormap = request_data.get("colormap")
    rescale = request_data.get("rescale")
    max_cloud_cover = request_data.get("max_cloud_cover", 5.0)
    
    if not index_id or not geometry:
        raise HTTPException(status_code=400, detail="index_id and geometry are required")
    
    if index_id not in config["indices"]:
        raise HTTPException(status_code=404, detail="Index not found")
    
    # Search for best imagery using polygon
    search_params = {
        "collections": "sentinel-2-l2a,landsat-c2-l2",
        "limit": 20,
        "intersects": json.dumps(geometry),
        "datetime": get_default_datetime_range(180),
        "query": json.dumps({"eo:cloud_cover": {"lt": max_cloud_cover}})
    }
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        # Search for imagery
        search_response = await client.get(f"{STAC_CATALOG_URL}/search", params=search_params)
        search_response.raise_for_status()
        search_data = search_response.json()
        
        features = search_data.get("features", [])
        if not features:
            raise HTTPException(status_code=404, detail="No suitable imagery found for the specified area")
        
        # Get the best asset
        best_asset = get_least_cloud_asset(features)
        collection = best_asset.get("collection", "")
        stac_url = f"{STAC_CATALOG_URL}/collections/{collection}/items/{best_asset['id']}"
        
        # Get index configuration
        index_config = config["indices"][index_id]
        expression = index_config["expression"]
        default_colormap = index_config.get("colormap", "viridis")
        
        # Apply band harmonization
        if collection in SENSOR_COLLECTIONS:
            expression = harmonize_band_expression(expression, collection)
        
        # Build TiTiler export request
        export_format = "png" if format == "png" else "jpeg"
        titiler_params = {
            "url": stac_url,
            "expression": expression,
            "colormap_name": colormap or default_colormap,
            "asset_as_band": "true",
            "width": width,
            "height": height,
            "resampling": "bilinear"
        }
        
        # Add rescaling
        if rescale:
            titiler_params["rescale"] = rescale
        elif "rescale" in index_config:
            titiler_params["rescale"] = index_config["rescale"]
        else:
            # Default rescaling for spectral indices
            if index_id in ["NDVI", "EVI", "SAVI", "GNDVI", "ARVI", "NDWI", "MNDWI", "NBR", "NDBI"]:
                titiler_params["rescale"] = "-1,1"
            else:
                titiler_params["rescale"] = "-2,2"
        
        # Create export using TiTiler crop endpoint with geometry
        crop_url = f"{TITILER_ENDPOINT}/stac/crop/{export_format}"
        crop_response = await client.post(
            crop_url,
            params=titiler_params,
            json={"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": geometry}]}
        )
        
        if crop_response.status_code != 200:
            # Fallback: try bbox-based export
            if "bbox" in best_asset:
                bbox = best_asset["bbox"]
                bbox_str = ",".join(map(str, bbox))
                titiler_params["bbox"] = bbox_str
                
                fallback_url = f"{TITILER_ENDPOINT}/stac/crop/{export_format}"
                crop_response = await client.get(fallback_url, params=titiler_params)
        
        if crop_response.status_code != 200:
            raise HTTPException(
                status_code=crop_response.status_code, 
                detail=f"Export failed: {crop_response.text}"
            )
        
        # Return the exported image
        media_type = "image/png" if format == "png" else "image/jpeg"
        filename = f"{index_id}_{collection}_export.{format}"
        
        return Response(
            content=crop_response.content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Cache-Control": "no-cache"
            }
        )

@app.get("/api/export-bbox/{format}")
async def export_bbox_analysis(
    format: str,
    index_id: str,
    bbox: str,
    width: int = 1024,
    height: int = 1024,
    colormap: str = None,
    rescale: str = None,
    max_cloud_cover: float = 5.0
):
    """Export spectral index analysis using bbox (simpler alternative)"""
    
    if format not in ["png", "jpg", "jpeg"]:
        raise HTTPException(status_code=400, detail="Format must be png, jpg, or jpeg")
    
    if index_id not in config["indices"]:
        raise HTTPException(status_code=404, detail="Index not found")
    
    # Search for best imagery using bbox
    search_params = {
        "collections": "sentinel-2-l2a,landsat-c2-l2",
        "bbox": bbox,
        "datetime": get_default_datetime_range(180),
        "limit": 20,
        "query": json.dumps({"eo:cloud_cover": {"lt": max_cloud_cover}})
    }
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        search_response = await client.get(f"{STAC_CATALOG_URL}/search", params=search_params)
        search_response.raise_for_status()
        search_data = search_response.json()
        
        features = search_data.get("features", [])
        if not features:
            raise HTTPException(status_code=404, detail="No suitable imagery found for the specified area")
        
        best_asset = get_least_cloud_asset(features)
        collection = best_asset.get("collection", "")
        stac_url = f"{STAC_CATALOG_URL}/collections/{collection}/items/{best_asset['id']}"
        
        # Get index configuration and apply harmonization
        index_config = config["indices"][index_id]
        expression = index_config["expression"]
        default_colormap = index_config.get("colormap", "viridis")
        
        if collection in SENSOR_COLLECTIONS:
            expression = harmonize_band_expression(expression, collection)
        
        # Build export parameters
        export_format = "png" if format == "png" else "jpeg"
        titiler_params = {
            "url": stac_url,
            "expression": expression,
            "colormap_name": colormap or default_colormap,
            "asset_as_band": "true",
            "bbox": bbox,
            "width": width,
            "height": height,
            "resampling": "bilinear"
        }
        
        # Add rescaling
        if rescale:
            titiler_params["rescale"] = rescale
        elif "rescale" in index_config:
            titiler_params["rescale"] = index_config["rescale"]
        else:
            if index_id in ["NDVI", "EVI", "SAVI", "GNDVI", "ARVI", "NDWI", "MNDWI", "NBR", "NDBI"]:
                titiler_params["rescale"] = "-1,1"
            else:
                titiler_params["rescale"] = "-2,2"
        
        # Export using TiTiler
        export_url = f"{TITILER_ENDPOINT}/stac/crop/{export_format}"
        export_response = await client.get(export_url, params=titiler_params)
        
        if export_response.status_code != 200:
            raise HTTPException(
                status_code=export_response.status_code,
                detail=f"Export failed: {export_response.text}"
            )
        
        media_type = "image/png" if format == "png" else "image/jpeg"
        filename = f"{index_id}_{collection}_bbox_export.{format}"
        
        return Response(
            content=export_response.content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Cache-Control": "no-cache"
            }
        )

# Startup and cleanup
@app.on_event("startup")
async def startup_event():
    """Initialize embedded Redis on startup"""
    start_embedded_redis()
    atexit.register(stop_embedded_redis)

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    stop_embedded_redis()

if __name__ == "__main__":
    import uvicorn
    start_embedded_redis()
    atexit.register(stop_embedded_redis)
    uvicorn.run(app, host="0.0.0.0", port=8080)
