'use client';

import React from 'react';
import { FormControl, InputLabel, Select, MenuItem } from '@mui/material';
import { CommunityArea } from '@/types';
import { useCommunityAreas } from '@/contexts/CommunityAreasProvider';

interface CommunityAreaFilterProps {
  onCommunityAreaSelect: (communityArea: CommunityArea | null) => void;
  selectedCommunityArea: CommunityArea | null;
  disabled?: boolean;
}

export default function CommunityAreaFilter({
  onCommunityAreaSelect,
  selectedCommunityArea,
  disabled = false,
}: CommunityAreaFilterProps) {
  const communityAreas = useCommunityAreas();

  const handleCommunityAreaChange = (event: any) => {
    const value = event.target.value;
    if (value === '') {
      onCommunityAreaSelect(null);
    } else {
      const selectedArea = communityAreas.find(
        (area) => area.community_area === parseInt(value)
      );
      onCommunityAreaSelect(selectedArea || null);
    }
  };

  return (
    <FormControl fullWidth disabled={disabled}>
      <InputLabel id="community-area-select-label">Community Area</InputLabel>
      <Select
        labelId="community-area-select-label"
        value={selectedCommunityArea?.community_area || ''}
        label="Community Area"
        onChange={handleCommunityAreaChange}
        MenuProps={{
          PaperProps: {
            style: {
              maxHeight: 400,
            },
          },
        }}
      >
        <MenuItem value="">
          <em>All Community Areas</em>
        </MenuItem>
        {communityAreas
          .sort((a, b) => a.community_area_name.localeCompare(b.community_area_name))
          .map((area) => (
            <MenuItem key={area.community_area} value={area.community_area}>
              {area.community_area_name} (#{area.community_area})
            </MenuItem>
          ))}
      </Select>
    </FormControl>
  );
}