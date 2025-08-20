import { NextResponse } from "next/server";
import { getStationsWithTripCounts } from "@/lib/db";

export async function GET() {
  try {
    const data = await getStationsWithTripCounts();

    return NextResponse.json({
      data,
      count: data.length,
    });
  } catch (error) {
    console.error("Station trips API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch station trip data" },
      { status: 500 }
    );
  }
}