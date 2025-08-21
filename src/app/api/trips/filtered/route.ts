import { NextRequest, NextResponse } from "next/server";
import { getTripDataByStation, getTripDataByCommunityArea, getCityTripSummary } from "@/lib/db";

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const filterType = searchParams.get("type");
    const stationName = searchParams.get("station");
    const communityArea = searchParams.get("communityArea");

    if (filterType === "station" && stationName) {
      const data = await getTripDataByStation(stationName);
      if (!data) {
        return NextResponse.json(
          { error: "Station not found" },
          { status: 404 }
        );
      }
      return NextResponse.json({ data, filterType: "station" });
    }

    if (filterType === "community-area" && communityArea) {
      const communityAreaNum = parseInt(communityArea);
      if (isNaN(communityAreaNum)) {
        return NextResponse.json(
          { error: "Invalid community area number" },
          { status: 400 }
        );
      }
      
      const data = await getTripDataByCommunityArea(communityAreaNum);
      if (!data) {
        return NextResponse.json(
          { error: "Community area not found" },
          { status: 404 }
        );
      }
      return NextResponse.json({ data, filterType: "community-area" });
    }

    // Default to city-wide data if no valid filter
    const data = await getCityTripSummary();
    return NextResponse.json({ data, filterType: "city" });

  } catch (error) {
    console.error("Filtered trips API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch filtered trip data" },
      { status: 500 }
    );
  }
}