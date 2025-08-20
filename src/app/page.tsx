'use client';

import { useEffect, useState } from 'react';
import ViewSelector, { ViewType } from '@/components/ViewSelector';
import TripDataDisplay from '@/components/TripDataDisplay';

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

export default function Home() {
  const [selectedView, setSelectedView] = useState<ViewType>('city');
  const [tripData, setTripData] = useState<
    CityData | CommunityAreaData[] | StationData[] | null
  >(null);
  const [stats, setStats] = useState<DatabaseStats | null>(null);
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

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const statsResponse = await fetch('/api/stats');
        if (statsResponse.ok) {
          const statsData = await statsResponse.json();
          setStats(statsData.stats);
        }
      } catch (err) {
        console.error('Failed to fetch database stats:', err);
      }
    };

    fetchStats();
  }, []);

  if (loading) {
    return (
      <div className='min-h-screen flex items-center justify-center'>
        <div className='text-center'>
          <div className='animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-4'></div>
          <p className='text-gray-600'>Loading Divvy data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className='min-h-screen flex items-center justify-center'>
        <div className='text-center text-red-600'>
          <h2 className='text-xl font-bold mb-2'>Error</h2>
          <p>{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className='min-h-screen bg-gray-50 py-8'>
      <div className='max-w-7xl mx-auto px-4 sm:px-6 lg:px-8'>
        {/* Header */}
        <div className='text-center mb-12'>
          <h1 className='text-4xl font-bold text-gray-900 mb-4'>
            🚲 Divvy Analytics Dashboard
          </h1>
          <p className='text-lg text-gray-600'>
            Chicago Bike Share Data Analysis
          </p>
        </div>

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
  );
}
