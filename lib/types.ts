export interface Station {
  id: number;
  station_id: string;
  name: string;
  latitude: number | null;
  longitude: number | null;
  community_area: number | null;
  community_area_name: string | null;
  created_at: Date;
  updated_at: Date;
}

export interface StationDay {
  id: number;
  station_id: number;
  day: number;
  month: number;
  year: number;
  acoustic_arrive: number;
  acoustic_depart: number;
  electric_arrive: number;
  electric_depart: number;
  created_at: Date;
}

export interface TripRaw {
  ride_id: string;
  rideable_type: string | null;
  started_at: Date | null;
  ended_at: Date | null;
  start_station_name: string | null;
  start_station_id: string | null;
  end_station_name: string | null;
  end_station_id: string | null;
  start_lat: number | null;
  start_lng: number | null;
  end_lat: number | null;
  end_lng: number | null;
  member_casual: string | null;
}

export interface CommunityAreaStats {
  community_area: number;
  community_area_name: string;
  station_count: number;
  total_departures: number;
  total_arrivals: number;
}

export interface StationWithStats extends Station {
  total_departures: number;
  total_arrivals: number;
  electric_total: number;
  acoustic_total: number;
}

export interface DatabaseStats {
  total_raw_trips: number;
  total_stations: number;
  total_station_days: number;
  unique_community_areas: number;
  earliest_day: number;
  latest_day: number;
}
