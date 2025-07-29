'use client';

import Header from '@/components/Header/Header';
import { Container, Grid, Stack } from '@mui/material';
import { useEffect, useState } from 'react';

interface CommunityArea {
  community_area: number;
  community_area_name: string;
  station_count: number;
  total_departures: number;
  total_arrivals: number;
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
  const [stats, setStats] = useState<DatabaseStats | null>(null);
  const [communityAreas, setCommunityAreas] = useState<CommunityArea[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);

        // Fetch database stats
        const statsResponse = await fetch('/api/stats');
        if (!statsResponse.ok) throw new Error('Failed to fetch stats');
        const statsData = await statsResponse.json();
        setStats(statsData.stats);

        // Fetch community areas
        const areasResponse = await fetch('/api/community_areas');
        if (!areasResponse.ok)
          throw new Error('Failed to fetch community areas');
        const areasData = await areasResponse.json();
        setCommunityAreas(areasData.communityAreas.slice(0, 10)); // Top 10

        setLoading(false);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
        setLoading(false);
      }
    };

    fetchData();
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
    <main>
      <Grid
        container
        justifyContent='center'
        columns={{ xs: 6, sm: 8, md: 8, lg: 12 }}
        height='90vh'
        width='97vw'
      >
        {/* Heading */}
        <Header />

        {/* See Data by... */}
      </Grid>
    </main>
  );
}
