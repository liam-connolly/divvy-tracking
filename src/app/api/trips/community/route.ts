import { NextResponse } from "next/server";
import { getCommunityAreasWithTripCounts } from "@/lib/db";

export async function GET() {
  try {
    const data = await getCommunityAreasWithTripCounts();

    return NextResponse.json({
      data,
      count: data.length,
    });
  } catch (error) {
    console.error("Community trips API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch community area trip data" },
      { status: 500 }
    );
  }
}