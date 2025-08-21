'use client';

import React, { createContext, useContext } from 'react';
import { Station } from '@/types';

const StationsContext = createContext<Station[]>([]);

export const useStations = () => {
  const context = useContext(StationsContext);
  if (!context) {
    throw new Error('useStations must be used within a StationsProvider');
  }
  return context;
};

interface StationsProviderProps {
  stations: Station[];
  children: React.ReactNode;
}

export default function StationsProvider({ stations, children }: StationsProviderProps) {
  return (
    <StationsContext.Provider value={stations}>
      {children}
    </StationsContext.Provider>
  );
}