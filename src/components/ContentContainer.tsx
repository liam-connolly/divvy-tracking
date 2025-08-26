'use client';

import { useEffect, useState } from 'react';
import ViewSelector, { ViewType } from '@/components/ViewSelector';
import TripDataDisplay from '@/components/TripDataDisplay';
import FAQ from '@/components/FAQ';
import { Box } from '@mui/material';
import { useStations } from '@/contexts/StationsProvider';
import { useCommunityAreas } from '@/contexts/CommunityAreasProvider';
import { useQueryState } from 'nuqs';

interface CityData {
  total_trips: number;
  total_stations: number;
  unique_community_areas: number;
}

interface CommunityAreaData {
  community_area: number;
  community_area_name: string;
  station_count: number;
  total_trips: number;
}

interface StationData {
  id: number;
  name: string;
  community_area_name: string | null;
  total_trips: number;
}

interface DatabaseStats {
  total_raw_trips: number;
  total_stations: number;
  total_station_days: number;
  unique_community_areas: number;
  earliest_day: number;
  latest_day: number;
}

export default function ContentContainer() {
  // Get context data
  const stations = useStations();
  const communityAreas = useCommunityAreas();
  const [faqOpen, _] = useQueryState('faq');

  const [selectedView, setSelectedView] = useState<ViewType>('city');
  const [tripData, setTripData] = useState<
    CityData | CommunityAreaData[] | StationData[] | null
  >(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchTripData = async () => {
      try {
        setLoading(true);
        setError(null);

        let endpoint = '';
        switch (selectedView) {
          case 'city':
            endpoint = '/api/trips/city';
            break;
          case 'community':
            endpoint = '/api/trips/community';
            break;
          case 'stations':
            endpoint = '/api/trips/stations';
            break;
        }

        const response = await fetch(endpoint);
        if (!response.ok)
          throw new Error(`Failed to fetch ${selectedView} data`);
        const result = await response.json();

        setTripData(result.data);
        setLoading(false);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
        setLoading(false);
      }
    };

    fetchTripData();
  }, [selectedView]);

  // Debug log to ensure context data is available
  useEffect(() => {
    console.log('Context data loaded:', {
      stationsCount: stations.length,
      communityAreasCount: communityAreas.length,
    });
  }, [stations.length, communityAreas.length]);

  return (
    <>
      {faqOpen && <FAQ />}

      {/* Don't lose state when FAQ is opened */}
      <Box sx={{ display: faqOpen ? 'none' : 'block', height: '100%' }}>
        <div className=' py-8'>
          <div className='max-w-7xl mx-auto px-4 sm:px-6 lg:px-8'>
            {/* View Selector */}
            <ViewSelector
              selectedView={selectedView}
              onViewChange={setSelectedView}
            />

            {/* Trip Data Display */}
            <TripDataDisplay
              viewType={selectedView}
              data={tripData}
              loading={loading}
              error={error}
            />
          </div>
        </div>
      </Box>
    </>
  );
}
