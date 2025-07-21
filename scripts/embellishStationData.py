#!/usr/bin/env python3
"""
Chicago Community Area Station Enrichment Script
Maps Divvy bike stations to Chicago's 77 community areas using geographic boundaries.
"""

import json
import requests
import psycopg2
from shapely.geometry import Point, shape
from typing import Dict, List, Optional, Tuple
import argparse
from dotenv import load_dotenv
import os
from urllib.parse import urlparse

load_dotenv() 

class ChicagoAreaMapper:
    def __init__(self, db_connection_string: str):
        """Initialize with database connection."""
        self.db_connection_string = db_connection_string
        self.community_areas = {}
        self.conn = None
        
    def connect_to_database(self):
        """Connect to PostgreSQL database."""
        try:
            parsed = urlparse(self.db_connection_string)
            self.conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path.lstrip('/'),
                user=parsed.username,
                password=parsed.password
            )
            print("✅ Connected to database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def download_community_areas(self) -> Dict:
        """Download Chicago community area boundaries from the city's open data portal."""
        print("🗺️  Downloading Chicago community area boundaries...")
        
        # Chicago Data Portal - Community Areas (GeoJSON)
        url = "https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            geojson_data = response.json()
            print(f"✅ Downloaded {len(geojson_data['features'])} community areas")
            
            return geojson_data
            
        except requests.RequestException as e:
            print(f"❌ Error downloading community areas: {e}")
            print("📋 Alternative: You can manually download from:")
            print("   https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6")
            raise
    
    def parse_community_areas(self, geojson_data: Dict):
        """Parse community areas into searchable format."""
        print("📍 Parsing community area boundaries...")
        
        for feature in geojson_data['features']:
            properties = feature['properties']
            
            # Extract community area info
            area_num = int(properties.get('area_num_1', properties.get('area_numbe', 0)))
            area_name = properties.get('community', 'Unknown')
            
            # Create shapely geometry for point-in-polygon testing
            geometry = shape(feature['geometry'])
            
            self.community_areas[area_num] = {
                'name': area_name,
                'number': area_num,
                'geometry': geometry
            }
        
        print(f"✅ Parsed {len(self.community_areas)} community areas")
    
    def find_community_area(self, latitude: float, longitude: float) -> Optional[int]:
        """Find which community area a point falls into."""
        point = Point(longitude, latitude)  # Note: Point takes (x, y) = (lon, lat)
        
        for area_num, area_data in self.community_areas.items():
            if area_data['geometry'].contains(point):
                return area_num
        
        return None
    
    def get_stations_from_db(self) -> List[Tuple[int, str, float, float]]:
        """Get all stations from the database."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT id, name, latitude::float, longitude::float 
            FROM "Station" 
            WHERE "communityArea" IS NULL
            ORDER BY id
        """)
        
        stations = cursor.fetchall()
        cursor.close()
        
        print(f"📊 Found {len(stations)} stations to process")
        return stations
    
    def update_station_community_area(self, station_id: int, community_area: int):
        """Update a station's community area in the database."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            UPDATE "Station" 
            SET "communityArea" = %s 
            WHERE id = %s
        """, (community_area, station_id))
        
        cursor.close()
    
    def process_stations(self):
        """Process all stations and assign community areas."""
        stations = self.get_stations_from_db()
        
        if not stations:
            print("ℹ️  No stations found that need community area assignment")
            return
        
        updated_count = 0
        not_found_count = 0
        
        for station_id, name, latitude, longitude in stations:
            try:
                community_area = self.find_community_area(latitude, longitude)
                
                if community_area:
                    self.update_station_community_area(station_id, community_area)
                    area_name = self.community_areas[community_area]['name']
                    print(f"✅ {name} → {area_name} (#{community_area})")
                    updated_count += 1
                else:
                    print(f"❓ {name} → No community area found (lat: {latitude}, lon: {longitude})")
                    not_found_count += 1
                    
            except Exception as e:
                print(f"❌ Error processing {name}: {e}")
                not_found_count += 1
        
        # Commit all changes
        self.conn.commit()
        
        print(f"\n📊 Processing Summary:")
        print(f"✅ Updated: {updated_count} stations")
        print(f"❓ Not found: {not_found_count} stations")
        
        if not_found_count > 0:
            print(f"\n💡 Stations not found might be:")
            print(f"   - Outside Chicago city limits")
            print(f"   - Have incorrect coordinates")
            print(f"   - Located in areas not covered by community area boundaries")
    
    def show_community_area_summary(self):
        """Show summary of stations per community area."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT s."communityArea", COUNT(*) as station_count
            FROM "Station" s
            WHERE s."communityArea" IS NOT NULL
            GROUP BY s."communityArea"
            ORDER BY s."communityArea"
        """)
        
        results = cursor.fetchall()
        cursor.close()
        
        print(f"\n📊 Stations by Community Area:")
        total_stations = 0
        
        for area_num, count in results:
            area_name = self.community_areas.get(area_num, {}).get('name', 'Unknown')
            print(f"   {area_num:2d}. {area_name:<25} {count:3d} stations")
            total_stations += count
        
        print(f"\n   Total: {total_stations} stations assigned to community areas")
    
    def cleanup(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed")

def main():
    parser = argparse.ArgumentParser(description="Enrich Divvy station data with Chicago community areas")
    parser.add_argument("--db-url", help="Database connection URL (defaults to DATABASE_URL env var)")
    parser.add_argument("--summary", action="store_true", help="Show community area summary after processing")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    
    args = parser.parse_args()
    
    # Use provided URL or fall back to environment variable
    db_url = args.db_url or os.getenv('DATABASE_URL')
    
    if not db_url:
        print("❌ Error: Database URL not provided")
        print("   Use --db-url argument or set DATABASE_URL environment variable")
        return 1
    
    mapper = ChicagoAreaMapper(db_url)
    
    try:
        # Connect to database
        mapper.connect_to_database()
        
        # Download and parse community area data
        geojson_data = mapper.download_community_areas()
        mapper.parse_community_areas(geojson_data)
        
        # Process stations
        if args.dry_run:
            print("🔍 DRY RUN MODE - No changes will be made to the database")
            # You could implement dry run logic here
        else:
            mapper.process_stations()
        
        # Show summary if requested
        if args.summary:
            mapper.show_community_area_summary()
        
        print("\n🎉 Station enrichment completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    finally:
        mapper.cleanup()
    
    return 0

if __name__ == "__main__":
    exit(main())