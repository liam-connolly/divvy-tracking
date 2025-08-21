export interface Station {
  id: number;
  name: string;
  community_area_name: string | null;
  community_area: number | null;
}

export interface CommunityArea {
  community_area: number;
  community_area_name: string;
}

export interface StationWithTrips extends Station {
  total_trips: number;
}

export interface CommunityAreaWithTrips extends CommunityArea {
  station_count: number;
  total_trips: number;
}

export interface FilterState {
  selectedStation: Station | null;
  selectedCommunityArea: CommunityArea | null;
}

export type FilterType = 'station' | 'community-area' | 'none';