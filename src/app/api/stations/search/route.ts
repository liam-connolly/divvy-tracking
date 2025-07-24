import { NextRequest, NextResponse } from "next/server";
import { searchStationsGrouped } from "@/lib/db";

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const stationName = searchParams.get("name");

    if (!stationName) {
      return NextResponse.json(
        { error: "Station name parameter required" },
        { status: 400 }
      );
    }

    const stations = await searchStationsGrouped(stationName);

    return NextResponse.json({
      stations,
      count: stations.length,
      searchTerm: stationName,
    });
  } catch (error) {
    console.error("Station search API error:", error);
    return NextResponse.json(
      { error: "Failed to search stations" },
      { status: 500 }
    );
  }
}
