'use client';

import React from 'react';
import { Box, Grid, Button, Typography } from '@mui/material';
import { Station, CommunityArea, FilterType } from '@/types';
import StationFilter from './StationFilter';
import CommunityAreaFilter from './CommunityAreaFilter';

interface FilterContainerProps {
  filterType: FilterType;
  selectedStation: Station | null;
  selectedCommunityArea: CommunityArea | null;
  onFilterTypeChange: (type: FilterType) => void;
  onStationSelect: (station: Station | null) => void;
  onCommunityAreaSelect: (communityArea: CommunityArea | null) => void;
  onClearFilters: () => void;
}

export default function FilterContainer({
  filterType,
  selectedStation,
  selectedCommunityArea,
  onFilterTypeChange,
  onStationSelect,
  onCommunityAreaSelect,
  onClearFilters,
}: FilterContainerProps) {
  const hasActiveFilters = selectedStation || selectedCommunityArea;

  return (
    <Box sx={{ mb: 4, p: 3, backgroundColor: '#f5f5f5', borderRadius: 2 }}>
      <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
        Filter Data
      </Typography>
      
      {/* Filter Type Selector */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="body2" sx={{ mb: 1, color: 'text.secondary' }}>
          Filter by:
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
          <Button
            variant={filterType === 'none' ? 'contained' : 'outlined'}
            onClick={() => onFilterTypeChange('none')}
            size="small"
          >
            🏙️ All Data
          </Button>
          <Button
            variant={filterType === 'station' ? 'contained' : 'outlined'}
            onClick={() => onFilterTypeChange('station')}
            size="small"
          >
            🚲 Station
          </Button>
          <Button
            variant={filterType === 'community-area' ? 'contained' : 'outlined'}
            onClick={() => onFilterTypeChange('community-area')}
            size="small"
          >
            🏘️ Community Area
          </Button>
        </Box>
      </Box>

      {/* Filter Controls */}
      <Grid container spacing={2} alignItems="center">
        <Grid size={{ xs: 12, md: 5 }}>
          <StationFilter
            onStationSelect={onStationSelect}
            selectedStation={selectedStation}
            disabled={filterType !== 'station'}
          />
        </Grid>
        
        <Grid size={{ xs: 12, md: 5 }}>
          <CommunityAreaFilter
            onCommunityAreaSelect={onCommunityAreaSelect}
            selectedCommunityArea={selectedCommunityArea}
            disabled={filterType !== 'community-area'}
          />
        </Grid>
        
        <Grid size={{ xs: 12, md: 2 }}>
          <Button
            onClick={onClearFilters}
            variant="outlined"
            color="secondary"
            fullWidth
            disabled={!hasActiveFilters}
          >
            Clear All
          </Button>
        </Grid>
      </Grid>

      {/* Active Filter Display */}
      {hasActiveFilters && (
        <Box sx={{ mt: 2, p: 2, backgroundColor: 'white', borderRadius: 1 }}>
          <Typography variant="body2" sx={{ fontWeight: 500, mb: 1 }}>
            Active Filters:
          </Typography>
          {selectedStation && (
            <Typography variant="body2" color="primary">
              🚲 Station: {selectedStation.name}
              {selectedStation.community_area_name && 
                ` (${selectedStation.community_area_name})`
              }
            </Typography>
          )}
          {selectedCommunityArea && (
            <Typography variant="body2" color="primary">
              🏘️ Community Area: {selectedCommunityArea.community_area_name} 
              (#{selectedCommunityArea.community_area})
            </Typography>
          )}
        </Box>
      )}
    </Box>
  );
}