import { NextRequest, NextResponse } from "next/server";
import { getStationActivityOverTime } from "@/lib/db";

interface Params {
  params: {
    stationName: string;
  };
}

export async function GET(request: NextRequest, { params }: Params) {
  try {
    const { searchParams } = new URL(request.url);
    const startYear = searchParams.get("startYear");
    const endYear = searchParams.get("endYear");

    const stationName = decodeURIComponent(params.stationName);

    const activity = await getStationActivityOverTime(
      stationName,
      startYear ? parseInt(startYear) : undefined,
      endYear ? parseInt(endYear) : undefined
    );

    return NextResponse.json({
      activity,
      stationName,
      count: activity.length,
    });
  } catch (error) {
    console.error("Station activity API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch station activity" },
      { status: 500 }
    );
  }
}
