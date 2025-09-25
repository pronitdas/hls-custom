## Project Overview

hls-custom is a spectral intelligence web application that provides dynamic Earth observation capabilities using STAC (SpatioTemporal Asset Catalog), TiTiler, and runtime band math. The application offers 25+ spectral indices and 5 RGB composites for analyzing satellite imagery from multiple sensors (Sentinel-2, Landsat).

## Common Commands

### Development & Running
```bash
# Start the entire application stack
docker-compose up -d

# Start in development mode (rebuild on changes)
docker-compose up --build

# Stop services
docker-compose down

# View logs
docker-compose logs -f geosense
docker-compose logs -f titiler

# Access the application
open http://localhost:8080
```

### Python Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the FastAPI server directly (development)
python app.py

# Run with uvicorn (production-like)
uvicorn app:app --host 0.0.0.0 --port 8080 --reload
```

### Cache Management
```bash
# Clear all caches via API
curl -X POST http://localhost:8080/api/cache/clear

# View cache statistics
curl http://localhost:8080/api/cache/stats
```

## Architecture

### Core Components

1. **FastAPI Backend** (`app.py`): Main API server with multi-sensor harmonization
   - STAC catalog integration (Earth Search AWS)
   - TiTiler proxy for dynamic tile generation
   - Redis caching for performance
   - Multi-sensor support (Sentinel-2, Landsat)

2. **Frontend** (`index.html`): Single-page MapLibre GL application
   - Interactive map with layer controls
   - Spectral index and RGB composite buttons
   - Real-time imagery search and visualization

3. **Configuration** (`indices.json`): Spectral indices and RGB composite definitions
   - 25 spectral indices across 7 categories
   - Band math expressions using generic band names
   - Colormap and rescaling configurations

4. **Services** (`docker-compose.yml`):
   - **TiTiler**: Dynamic tile generation service
   - **Redis**: Caching layer for tiles and metadata
   - **GeoSense**: Main application container

### Key Features

- **Multi-sensor harmonization**: Automatic band mapping between Sentinel-2 and Landsat
- **Dynamic processing**: Runtime band math using TiTiler expressions
- **Global coverage**: Automatic asset discovery for any tile coordinate
- **Caching strategy**: Redis + file-based caching for performance
- **STAC integration**: Earth Search AWS catalog for imagery discovery

## Development Guidelines

### Adding New Spectral Indices
1. Edit `indices.json` with new index definition
2. Use generic band names (red, green, blue, nir, swir16, swir22, etc.)
3. Include colormap, category, and description
4. Band harmonization is handled automatically

### Multi-sensor Support
- Band expressions use generic names (red, green, blue, nir, swir16, swir22)
- `SENSOR_COLLECTIONS` in `app.py` defines band mapping for each sensor
- Harmonization occurs at runtime based on detected collection

### API Endpoints Structure
- `/api/indices` - List all spectral indices
- `/api/rgb` - List RGB composites
- `/api/stac/search` - Search STAC catalog
- `/tiles/{index_id}/{z}/{x}/{y}.png` - Global tile endpoint
- `/api/tiles/{index_id}/{z}/{x}/{y}.png` - Specific STAC item tiles

### Environment Variables
- `TITILER_ENDPOINT`: TiTiler service URL (default: http://titiler:8000)
- `STAC_CATALOG_URL`: STAC catalog endpoint
- `REDIS_URL`: Redis connection string

## File Structure
```
├── app.py                 # FastAPI backend with multi-sensor support
├── index.html             # MapLibre GL frontend
├── indices.json           # Spectral indices and RGB definitions
├── requirements.txt       # Python dependencies
├── docker-compose.yml     # Multi-service stack
├── Dockerfile            # Application container
└── cache/                # File-based tile cache
```

## Testing & Validation

### Manual Testing
1. Search for imagery in different regions
2. Test various spectral indices and RGB composites
3. Verify multi-sensor harmonization (Sentinel-2 vs Landsat)
4. Check cache performance and clear cache functionality

### API Testing
```bash
# Test index listing
curl http://localhost:8080/api/indices

# Test STAC search
curl "http://localhost:8080/api/stac/search?bbox=-122.4,37.7,-122.3,37.8&collections=sentinel-2-l2a"

# Test tile generation
curl "http://localhost:8080/tiles/NDVI/12/656/1582.png" -o test_tile.png
```

## Performance Considerations

- Redis caching reduces STAC search latency
- File-based tile caching improves repeated access
- Multi-sensor asset discovery optimizes coverage
- TiTiler expressions enable dynamic processing without pre-computation
- Connection pooling for external API calls
