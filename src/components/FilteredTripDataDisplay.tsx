'use client';

import React from 'react';
import { Box, Typography, Grid, Card, CardContent, Chip } from '@mui/material';
import { FilterType } from '@/types';

interface StationFilterData {
  total_trips: number;
  acoustic_trips: number;
  electric_trips: number;
  total_departures: number;
  total_arrivals: number;
  station_info: {
    name: string;
    community_area_name: string | null;
    community_area: number | null;
  } | null;
}

interface CommunityAreaFilterData {
  total_trips: number;
  acoustic_trips: number;
  electric_trips: number;
  total_departures: number;
  total_arrivals: number;
  station_count: number;
  community_area_info: {
    community_area: number;
    community_area_name: string;
  } | null;
}

interface CityData {
  total_trips: number;
  total_stations: number;
  unique_community_areas: number;
}

interface FilteredTripDataDisplayProps {
  data: StationFilterData | CommunityAreaFilterData | CityData | null;
  filterType: FilterType;
  loading: boolean;
  error: string | null;
}

export default function FilteredTripDataDisplay({
  data,
  filterType,
  loading,
  error,
}: FilteredTripDataDisplayProps) {
  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/3 mb-4"></div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {[1, 2, 3].map((i) => (
              <div key={i} className="h-24 bg-gray-200 rounded"></div>
            ))}
          </div>
        </div>
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Card sx={{ backgroundColor: '#ffebee' }}>
          <CardContent>
            <Typography color="error" variant="h6">
              Error
            </Typography>
            <Typography color="error">{error}</Typography>
          </CardContent>
        </Card>
      </Box>
    );
  }

  if (!data) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography color="text.secondary">No data available</Typography>
      </Box>
    );
  }

  if (filterType === 'station') {
    const stationData = data as StationFilterData;
    return (
      <Box sx={{ p: 3 }}>
        <Box sx={{ mb: 3, display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
          <Typography variant="h5" sx={{ fontWeight: 600 }}>
            🚲 Station Details
          </Typography>
          {stationData.station_info?.community_area_name && (
            <Chip 
              label={`${stationData.station_info.community_area_name}`}
              color="primary"
              variant="outlined"
              size="small"
            />
          )}
        </Box>
        
        {stationData.station_info && (
          <Typography variant="h6" sx={{ mb: 3, color: 'text.secondary' }}>
            {stationData.station_info.name}
          </Typography>
        )}

        <Grid container spacing={3}>
          <Grid size={{ xs: 12, md: 4 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h4" sx={{ fontWeight: 700, color: 'primary.main', mb: 1 }}>
                  {stationData.total_trips.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Total Trips
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 4 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h4" sx={{ fontWeight: 700, color: 'success.main', mb: 1 }}>
                  {stationData.total_departures.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Departures
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 4 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h4" sx={{ fontWeight: 700, color: 'info.main', mb: 1 }}>
                  {stationData.total_arrivals.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Arrivals
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 6 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h5" sx={{ fontWeight: 600, color: 'warning.main', mb: 1 }}>
                  {stationData.acoustic_trips.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Classic Bike Trips
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 6 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h5" sx={{ fontWeight: 600, color: 'secondary.main', mb: 1 }}>
                  {stationData.electric_trips.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Electric Bike Trips
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      </Box>
    );
  }

  if (filterType === 'community-area') {
    const communityData = data as CommunityAreaFilterData;
    return (
      <Box sx={{ p: 3 }}>
        <Box sx={{ mb: 3, display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
          <Typography variant="h5" sx={{ fontWeight: 600 }}>
            🏘️ Community Area Details
          </Typography>
          {communityData.community_area_info && (
            <Chip 
              label={`Area #${communityData.community_area_info.community_area}`}
              color="primary"
              variant="outlined"
              size="small"
            />
          )}
        </Box>
        
        {communityData.community_area_info && (
          <Typography variant="h6" sx={{ mb: 3, color: 'text.secondary' }}>
            {communityData.community_area_info.community_area_name}
          </Typography>
        )}

        <Grid container spacing={3}>
          <Grid size={{ xs: 12, md: 4 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h4" sx={{ fontWeight: 700, color: 'primary.main', mb: 1 }}>
                  {communityData.total_trips.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Total Trips
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 4 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h4" sx={{ fontWeight: 700, color: 'success.main', mb: 1 }}>
                  {communityData.station_count}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Stations
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 4 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h4" sx={{ fontWeight: 700, color: 'info.main', mb: 1 }}>
                  {Math.round(communityData.total_trips / communityData.station_count).toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Avg Trips/Station
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 6 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h5" sx={{ fontWeight: 600, color: 'warning.main', mb: 1 }}>
                  {communityData.acoustic_trips.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Classic Bike Trips
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid size={{ xs: 12, md: 6 }}>
            <Card>
              <CardContent sx={{ textAlign: 'center' }}>
                <Typography variant="h5" sx={{ fontWeight: 600, color: 'secondary.main', mb: 1 }}>
                  {communityData.electric_trips.toLocaleString()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Electric Bike Trips
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      </Box>
    );
  }

  // City-wide data (fallback)
  const cityData = data as CityData;
  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        🏙️ City-wide Summary
      </Typography>

      <Grid container spacing={3}>
        <Grid size={{ xs: 12, md: 4 }}>
          <Card>
            <CardContent sx={{ textAlign: 'center' }}>
              <Typography variant="h4" sx={{ fontWeight: 700, color: 'primary.main', mb: 1 }}>
                {cityData.total_trips?.toLocaleString() || 'N/A'}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Total Trips
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid size={{ xs: 12, md: 4 }}>
          <Card>
            <CardContent sx={{ textAlign: 'center' }}>
              <Typography variant="h4" sx={{ fontWeight: 700, color: 'success.main', mb: 1 }}>
                {cityData.total_stations?.toLocaleString() || 'N/A'}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Active Stations
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid size={{ xs: 12, md: 4 }}>
          <Card>
            <CardContent sx={{ textAlign: 'center' }}>
              <Typography variant="h4" sx={{ fontWeight: 700, color: 'info.main', mb: 1 }}>
                {cityData.unique_community_areas || 'N/A'}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Community Areas
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
}