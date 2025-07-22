import psycopg2
import pandas as pd
import os
from glob import glob
import requests
import json
from datetime import datetime
import numpy as np

# Chicago Community Area boundaries (approximate center points for quick lookup)
COMMUNITY_AREAS = {
    # This is a simplified mapping - you'd want to use actual GIS data
    # Format: (lat_min, lat_max, lng_min, lng_max): (area_number, area_name)
    (41.8, 41.85, -87.65, -87.60): (32, "Loop"),
    (41.85, 41.9, -87.65, -87.60): (8, "Near North Side"),
    (41.75, 41.8, -87.65, -87.60): (35, "Douglas"),
    # Add more mappings as needed...
}

def get_community_area_from_coords(lat, lng):
    """
    Simple bounding box lookup for community area
    In production, you'd use PostGIS or a proper GIS library
    """
    if lat is None or lng is None:
        return None, None
    
    for (lat_min, lat_max, lng_min, lng_max), (area_num, area_name) in COMMUNITY_AREAS.items():
        if lat_min <= lat <= lat_max and lng_min <= lng <= lng_max:
            return area_num, area_name
    
    # Default mapping for common Chicago areas
    if 41.7 <= lat <= 42.0 and -87.8 <= lng <= -87.5:
        return 1, "Rogers Park"  # Default fallback
    
    return None, None

def process_station(cursor, station_id, station_name, lat, lng):
    """
    Insert or update station with community area info
    """
    if not station_id or not station_name:
        return None
    
    # Get community area
    community_area, community_area_name = get_community_area_from_coords(lat, lng)
    
    # Check if station exists
    cursor.execute(
        "SELECT id FROM stations WHERE station_id = %s",
        (station_id,)
    )
    
    result = cursor.fetchone()
    if result:
        station_pk = result[0]
        # Update existing station
        cursor.execute("""
            UPDATE stations 
            SET name = %s, latitude = %s, longitude = %s, 
                community_area = %s, community_area_name = %s, 
                updated_at = NOW()
            WHERE station_id = %s
        """, (station_name, lat, lng, community_area, community_area_name, station_id))
    else:
        # Insert new station
        cursor.execute("""
            INSERT INTO stations (station_id, name, latitude, longitude, 
                                community_area, community_area_name)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (station_id, station_name, lat, lng, community_area, community_area_name))
        
        station_pk = cursor.fetchone()[0]
    
    return station_pk

def aggregate_daily_data(cursor):
    """
    Aggregate raw trip data into daily station statistics
    """
    print("Aggregating daily data...")
    
    cursor.execute("""
        INSERT INTO station_days (station_id, day, month, year, acoustic_depart, electric_depart)
        SELECT 
            s.id as station_id,
            EXTRACT(day FROM t.started_at) as day,
            EXTRACT(month FROM t.started_at) as month,
            EXTRACT(year FROM t.started_at) as year,
            COUNT(CASE WHEN t.rideable_type IN ('classic_bike', 'docked_bike') THEN 1 END) as acoustic_depart,
            COUNT(CASE WHEN t.rideable_type = 'electric_bike' THEN 1 END) as electric_depart
        FROM trips_raw t
        JOIN stations s ON t.start_station_id = s.station_id
        WHERE t.started_at IS NOT NULL AND t.start_station_id IS NOT NULL
        GROUP BY s.id, EXTRACT(day FROM t.started_at), EXTRACT(month FROM t.started_at), EXTRACT(year FROM t.started_at)
        ON CONFLICT (day, month, year, station_id) 
        DO UPDATE SET 
            acoustic_depart = station_days.acoustic_depart + EXCLUDED.acoustic_depart,
            electric_depart = station_days.electric_depart + EXCLUDED.electric_depart
    """)
    
    cursor.execute("""
        INSERT INTO station_days (station_id, day, month, year, acoustic_arrive, electric_arrive)
        SELECT 
            s.id as station_id,
            EXTRACT(day FROM t.ended_at) as day,
            EXTRACT(month FROM t.ended_at) as month,
            EXTRACT(year FROM t.ended_at) as year,
            COUNT(CASE WHEN t.rideable_type IN ('classic_bike', 'docked_bike') THEN 1 END) as acoustic_arrive,
            COUNT(CASE WHEN t.rideable_type = 'electric_bike' THEN 1 END) as electric_arrive
        FROM trips_raw t
        JOIN stations s ON t.end_station_id = s.station_id
        WHERE t.ended_at IS NOT NULL AND t.end_station_id IS NOT NULL
        GROUP BY s.id, EXTRACT(day FROM t.ended_at), EXTRACT(month FROM t.ended_at), EXTRACT(year FROM t.ended_at)
        ON CONFLICT (day, month, year, station_id) 
        DO UPDATE SET 
            acoustic_arrive = station_days.acoustic_arrive + EXCLUDED.acoustic_arrive,
            electric_arrive = station_days.electric_arrive + EXCLUDED.electric_arrive
    """)
    
    print("Daily aggregation complete!")

def import_csvs_to_postgres():
    # Database connection
    conn = psycopg2.connect(
        host="localhost",
        database="divvy_db",
        user="divvy_user",
        password="your_secure_password"  # Replace with your password
    )
    cur = conn.cursor()
    
    # Get all CSV files
    csv_files = glob('divvy_data/*.csv')
    csv_files.sort()
    
    total_rows = 0
    stations_processed = set()
    
    for csv_file in csv_files:
        print(f"Processing {csv_file}...")
        
        try:
            # Read CSV in chunks
            chunk_size = 5000
            for chunk_df in pd.read_csv(csv_file, chunksize=chunk_size):
                # Clean column names
                chunk_df.columns = chunk_df.columns.str.lower().str.replace(' ', '_')
                
                # Handle column variations
                column_mapping = {
                    'trip_id': 'ride_id',
                    'bikeid': 'rideable_type',
                    'starttime': 'started_at',
                    'stoptime': 'ended_at',
                    'from_station_name': 'start_station_name',
                    'from_station_id': 'start_station_id',
                    'to_station_name': 'end_station_name',
                    'to_station_id': 'end_station_id',
                    'usertype': 'member_casual'
                }
                
                for old_name, new_name in column_mapping.items():
                    if old_name in chunk_df.columns and new_name:
                        chunk_df.rename(columns={old_name: new_name}, inplace=True)
                
                # Process stations first
                for _, row in chunk_df.iterrows():
                    # Start station
                    if row.get('start_station_id') and row.get('start_station_id') not in stations_processed:
                        process_station(
                            cur, 
                            str(row['start_station_id']), 
                            row.get('start_station_name'),
                            row.get('start_lat'), 
                            row.get('start_lng')
                        )
                        stations_processed.add(row['start_station_id'])
                    
                    # End station
                    if row.get('end_station_id') and row.get('end_station_id') not in stations_processed:
                        process_station(
                            cur, 
                            str(row['end_station_id']), 
                            row.get('end_station_name'),
                            row.get('end_lat'), 
                            row.get('end_lng')
                        )
                        stations_processed.add(row['end_station_id'])
                
                # Insert raw trip data
                required_columns = [
                    'ride_id', 'rideable_type', 'started_at', 'ended_at',
                    'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id',
                    'start_lat', 'start_lng', 'end_lat', 'end_lng', 'member_casual'
                ]
                
                available_columns = [col for col in required_columns if col in chunk_df.columns]
                chunk_df = chunk_df[available_columns].dropna(subset=['ride_id'])
                
                # Convert to list of tuples
                data_tuples = [tuple(row) for row in chunk_df.values]
                
                if data_tuples:
                    placeholders = ','.join(['%s'] * len(available_columns))
                    columns_str = ','.join(available_columns)
                    
                    insert_query = f"""
                        INSERT INTO trips_raw ({columns_str}) 
                        VALUES ({placeholders})
                        ON CONFLICT (ride_id) DO NOTHING
                    """
                    
                    cur.executemany(insert_query, data_tuples)
                    conn.commit()
                    
                    total_rows += len(data_tuples)
                    print(f"  Inserted {len(data_tuples)} rows")
        
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
            conn.rollback()
            continue
    
    print(f"Total raw trips imported: {total_rows}")
    print(f"Total stations processed: {len(stations_processed)}")
    
    # Aggregate the data
    aggregate_daily_data(cur)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    import_csvs_to_postgres()