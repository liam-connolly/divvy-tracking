'use client';

import { ViewType } from './ViewSelector';

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

interface TripDataDisplayProps {
  viewType: ViewType;
  data: CityData | CommunityAreaData[] | StationData[] | null;
  loading: boolean;
  error: string | null;
}

export default function TripDataDisplay({
  viewType,
  data,
  loading,
  error,
}: TripDataDisplayProps) {
  // Debug logging
  console.log('TripDataDisplay:', { viewType, dataType: typeof data, isArray: Array.isArray(data), data });
  if (loading) {
    return (
      <div className='bg-white rounded-lg shadow-md p-6'>
        <div className='animate-pulse'>
          <div className='h-6 bg-gray-200 rounded w-1/3 mb-4'></div>
          <div className='space-y-3'>
            <div className='h-4 bg-gray-200 rounded'></div>
            <div className='h-4 bg-gray-200 rounded w-5/6'></div>
            <div className='h-4 bg-gray-200 rounded w-4/6'></div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className='bg-white rounded-lg shadow-md p-6'>
        <div className='text-red-600 text-center'>
          <h3 className='text-lg font-semibold mb-2'>Error</h3>
          <p>{error}</p>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className='bg-white rounded-lg shadow-md p-6'>
        <div className='text-gray-500 text-center'>No data available</div>
      </div>
    );
  }

  if (viewType === 'city') {
    const cityData = data as CityData;
    return (
      <div className='bg-white rounded-lg shadow-md p-6'>
        <h2 className='text-2xl font-bold text-gray-900 mb-6 font-big-shoulders'>
          City-wide Trip Summary
        </h2>
        <div className='grid grid-cols-1 md:grid-cols-3 gap-6'>
          <div className='text-center'>
            <div className='text-4xl font-bold text-blue-600'>
              {cityData.total_trips?.toLocaleString() || 'N/A'}
            </div>
            <div className='text-sm text-gray-500 mt-1'>Total Trips</div>
          </div>
          <div className='text-center'>
            <div className='text-4xl font-bold text-green-600'>
              {cityData.total_stations?.toLocaleString() || 'N/A'}
            </div>
            <div className='text-sm text-gray-500 mt-1'>Active Stations</div>
          </div>
          <div className='text-center'>
            <div className='text-4xl font-bold text-purple-600'>
              {cityData.unique_community_areas || 'N/A'}
            </div>
            <div className='text-sm text-gray-500 mt-1'>Community Areas</div>
          </div>
        </div>
      </div>
    );
  }

  if (viewType === 'community') {
    const communityData = Array.isArray(data) ? data as CommunityAreaData[] : [];
    
    if (communityData.length === 0) {
      return (
        <div className='bg-white rounded-lg shadow-md p-6'>
          <div className='text-gray-500 text-center'>No community area data available</div>
        </div>
      );
    }
    
    return (
      <div className='bg-white rounded-lg shadow-md p-6'>
        <h2 className='text-2xl font-bold text-gray-900 mb-6 font-big-shoulders'>
          Community Areas by Trip Volume
        </h2>
        <div className='overflow-x-auto'>
          <table className='min-w-full table-auto'>
            <thead>
              <tr className='bg-gray-50'>
                <th className='px-4 py-3 text-left text-sm font-medium text-gray-500'>
                  Rank
                </th>
                <th className='px-4 py-3 text-left text-sm font-medium text-gray-500'>
                  Area
                </th>
                <th className='px-4 py-3 text-left text-sm font-medium text-gray-500'>
                  Name
                </th>
                <th className='px-4 py-3 text-right text-sm font-medium text-gray-500'>
                  Stations
                </th>
                <th className='px-4 py-3 text-right text-sm font-medium text-gray-500'>
                  Total Trips
                </th>
              </tr>
            </thead>
            <tbody className='divide-y divide-gray-200'>
              {communityData.map((area, index) => (
                <tr key={area.community_area} className='hover:bg-gray-50'>
                  <td className='px-4 py-3 text-sm font-medium text-gray-900'>
                    #{index + 1}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-900'>
                    {area.community_area}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-900 font-medium'>
                    {area.community_area_name}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-900 text-right'>
                    {area.station_count}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-900 text-right font-semibold'>
                    {area.total_trips?.toLocaleString() || '0'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  if (viewType === 'stations') {
    const stationData = Array.isArray(data) ? data as StationData[] : [];
    
    if (stationData.length === 0) {
      return (
        <div className='bg-white rounded-lg shadow-md p-6'>
          <div className='text-gray-500 text-center'>No station data available</div>
        </div>
      );
    }
    
    const topStations = stationData.slice(0, 50); // Show top 50 stations

    return (
      <div className='bg-white rounded-lg shadow-md p-6'>
        <h2 className='text-2xl font-bold text-gray-900 mb-6 font-big-shoulders'>
          Top Stations by Trip Volume
          <span className='text-sm font-normal text-gray-500 ml-2'>
            (Showing top 50 of {stationData.length})
          </span>
        </h2>
        <div className='overflow-x-auto'>
          <table className='min-w-full table-auto'>
            <thead>
              <tr className='bg-gray-50'>
                <th className='px-4 py-3 text-left text-sm font-medium text-gray-500'>
                  Rank
                </th>
                <th className='px-4 py-3 text-left text-sm font-medium text-gray-500'>
                  Station Name
                </th>
                <th className='px-4 py-3 text-left text-sm font-medium text-gray-500'>
                  Community Area
                </th>
                <th className='px-4 py-3 text-right text-sm font-medium text-gray-500'>
                  Total Trips
                </th>
              </tr>
            </thead>
            <tbody className='divide-y divide-gray-200'>
              {topStations.map((station, index) => (
                <tr key={station.id} className='hover:bg-gray-50'>
                  <td className='px-4 py-3 text-sm font-medium text-gray-900'>
                    #{index + 1}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-900 font-medium'>
                    {station.name}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-500'>
                    {station.community_area_name || 'Unknown'}
                  </td>
                  <td className='px-4 py-3 text-sm text-gray-900 text-right font-semibold'>
                    {station.total_trips?.toLocaleString() || '0'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  return null;
}
