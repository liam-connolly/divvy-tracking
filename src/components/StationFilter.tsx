'use client';

import React, { useState } from 'react';
import { Autocomplete, TextField, CircularProgress } from '@mui/material';
import { Station } from '@/types';
import { useStations } from '@/contexts/StationsProvider';

interface StationFilterProps {
  onStationSelect: (station: Station | null) => void;
  selectedStation: Station | null;
  disabled?: boolean;
}

export default function StationFilter({
  onStationSelect,
  selectedStation,
  disabled = false,
}: StationFilterProps) {
  const stations = useStations();
  const [inputValue, setInputValue] = useState('');

  const handleStationChange = (
    _event: React.SyntheticEvent,
    newValue: Station | null
  ) => {
    onStationSelect(newValue);
  };

  const filterOptions = (options: Station[], { inputValue }: { inputValue: string }) => {
    const inputTokens = inputValue
      .split(' ')
      .map((v) => v.trim().toLowerCase())
      .filter((t) => t !== '&' && t !== '');

    if (inputTokens.length === 0) return options.slice(0, 100); // Limit initial results

    return options.filter((station) => {
      const lowercaseName = station.name.toLowerCase();
      const lowercaseCommunity = station.community_area_name?.toLowerCase() || '';

      return inputTokens.every(
        (token) => 
          lowercaseName.includes(token) || 
          lowercaseCommunity.includes(token)
      );
    }).slice(0, 50); // Limit filtered results
  };

  return (
    <Autocomplete
      value={selectedStation}
      onChange={handleStationChange}
      inputValue={inputValue}
      onInputChange={(_, newInputValue) => setInputValue(newInputValue)}
      options={stations}
      getOptionLabel={(option) => option.name}
      filterOptions={filterOptions}
      disabled={disabled}
      renderInput={(params) => (
        <TextField
          {...params}
          label="Search Stations"
          placeholder="Enter station name..."
          variant="outlined"
          fullWidth
          InputProps={{
            ...params.InputProps,
            endAdornment: (
              <>
                {params.InputProps.endAdornment}
              </>
            ),
          }}
        />
      )}
      renderOption={(props, option) => (
        <li {...props} key={option.id}>
          <div>
            <div style={{ fontWeight: 500 }}>{option.name}</div>
            {option.community_area_name && (
              <div style={{ fontSize: '0.875em', color: '#666' }}>
                {option.community_area_name}
              </div>
            )}
          </div>
        </li>
      )}
      noOptionsText="No stations found"
      clearOnBlur={false}
      selectOnFocus
    />
  );
}