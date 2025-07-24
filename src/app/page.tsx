"use client";

import { useEffect, useState } from "react";

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
        const statsResponse = await fetch("/api/stats");
        if (!statsResponse.ok) throw new Error("Failed to fetch stats");
        const statsData = await statsResponse.json();
        setStats(statsData.stats);

        // Fetch community areas
        const areasResponse = await fetch("/api/community_areas");
        if (!areasResponse.ok)
          throw new Error("Failed to fetch community areas");
        const areasData = await areasResponse.json();
        setCommunityAreas(areasData.communityAreas.slice(0, 10)); // Top 10

        setLoading(false);
      } catch (err) {
        setError(err instanceof Error ? err.message : "An error occurred");
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading Divvy data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center text-red-600">
          <h2 className="text-xl font-bold mb-2">Error</h2>
          <p>{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            🚲 Divvy Analytics Dashboard
          </h1>
          <p className="text-lg text-gray-600">
            Chicago Bike Share Data Analysis
          </p>
        </div>

        {/* Database Stats */}
        {stats && (
          <div className="bg-white rounded-lg shadow-md p-6 mb-8">
            <h2 className="text-2xl font-bold text-gray-900 mb-6">
              Database Overview
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <div className="text-center">
                <div className="text-3xl font-bold text-blue-600">
                  {stats.total_raw_trips?.toLocaleString() || "N/A"}
                </div>
                <div className="text-sm text-gray-500">Total Trips</div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-green-600">
                  {stats.total_stations?.toLocaleString() || "N/A"}
                </div>
                <div className="text-sm text-gray-500">Stations</div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-purple-600">
                  {stats.unique_community_areas || "N/A"}
                </div>
                <div className="text-sm text-gray-500">Community Areas</div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-orange-600">
                  {stats.total_station_days?.toLocaleString() || "N/A"}
                </div>
                <div className="text-sm text-gray-500">Station Days</div>
              </div>
            </div>
          </div>
        )}

        {/* Top Community Areas */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <h2 className="text-2xl font-bold text-gray-900 mb-6">
            Top 10 Community Areas by Activity
          </h2>
          <div className="overflow-x-auto">
            <table className="min-w-full table-auto">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-500">
                    Area
                  </th>
                  <th className="px-4 py-2 text-left text-sm font-medium text-gray-500">
                    Name
                  </th>
                  <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">
                    Stations
                  </th>
                  <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">
                    Departures
                  </th>
                  <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">
                    Arrivals
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {communityAreas.map((area, index) => (
                  <tr key={area.community_area} className="hover:bg-gray-50">
                    <td className="px-4 py-2 text-sm text-gray-900">
                      {area.community_area}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-900 font-medium">
                      {area.community_area_name}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-900 text-right">
                      {area.station_count}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-900 text-right">
                      {area.total_departures?.toLocaleString() || "0"}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-900 text-right">
                      {area.total_arrivals?.toLocaleString() || "0"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* API Test Links */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-2xl font-bold text-gray-900 mb-6">
            API Endpoints
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <a
              href="/api/community-areas"
              target="_blank"
              className="block p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="font-medium text-blue-600">Community Areas</div>
              <div className="text-sm text-gray-500">/api/community-areas</div>
            </a>
            <a
              href="/api/stations"
              target="_blank"
              className="block p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="font-medium text-blue-600">All Stations</div>
              <div className="text-sm text-gray-500">/api/stations</div>
            </a>
            <a
              href="/api/stations/community/32"
              target="_blank"
              className="block p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="font-medium text-blue-600">Loop Stations</div>
              <div className="text-sm text-gray-500">
                /api/stations/community/32
              </div>
            </a>
            <a
              href="/api/stats"
              target="_blank"
              className="block p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div className="font-medium text-blue-600">Database Stats</div>
              <div className="text-sm text-gray-500">/api/stats</div>
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}
