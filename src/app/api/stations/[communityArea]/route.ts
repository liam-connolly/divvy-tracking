import { NextRequest, NextResponse } from "next/server";
import { getStationsByCommunityAreaGrouped } from "@/lib/db";

interface Params {
  params: {
    communityArea: string;
  };
}

export async function GET(request: NextRequest, { params }: Params) {
  try {
    const { searchParams } = new URL(request.url);
    const year = searchParams.get("year");
    const month = searchParams.get("month");

    const communityArea = parseInt(params.communityArea);

    if (isNaN(communityArea)) {
      return NextResponse.json(
        { error: "Invalid community area number" },
        { status: 400 }
      );
    }

    const stations = await getStationsByCommunityAreaGrouped(
      communityArea,
      year ? parseInt(year) : undefined,
      month ? parseInt(month) : undefined
    );

    return NextResponse.json({
      stations,
      communityArea,
      count: stations.length,
      filters: {
        year: year ? parseInt(year) : null,
        month: month ? parseInt(month) : null,
      },
    });
  } catch (error) {
    console.error("Stations by community area API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch stations" },
      { status: 500 }
    );
  }
}
