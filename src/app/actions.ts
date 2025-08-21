'use server';

import { getCommunityAreasForFilter, getStationsForFilter } from '@/lib/db';
import { Station, CommunityArea } from '@/types';

export async function getStations(): Promise<Station[]> {
  try {
    const stations = await getStationsForFilter();
    return stations;
  } catch (error) {
    console.error('Failed to fetch stations in server action:', error);
    return [];
  }
}

export async function getCommunityAreas(): Promise<CommunityArea[]> {
  try {
    const communityAreas = await getCommunityAreasForFilter();
    return communityAreas;
  } catch (error) {
    console.error('Failed to fetch community areas in server action:', error);
    return [];
  }
}