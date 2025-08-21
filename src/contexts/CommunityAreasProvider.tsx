'use client';

import React, { createContext, useContext } from 'react';
import { CommunityArea } from '@/types';

const CommunityAreasContext = createContext<CommunityArea[]>([]);

export const useCommunityAreas = () => {
  const context = useContext(CommunityAreasContext);
  if (!context) {
    throw new Error('useCommunityAreas must be used within a CommunityAreasProvider');
  }
  return context;
};

interface CommunityAreasProviderProps {
  communityAreas: CommunityArea[];
  children: React.ReactNode;
}

export default function CommunityAreasProvider({ communityAreas, children }: CommunityAreasProviderProps) {
  return (
    <CommunityAreasContext.Provider value={communityAreas}>
      {children}
    </CommunityAreasContext.Provider>
  );
}