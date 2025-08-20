import { NextResponse } from "next/server";
import { getCityTripSummary } from "@/lib/db";

export async function GET() {
  try {
    const data = await getCityTripSummary();

    if (!data) {
      return NextResponse.json(
        { error: "Unable to fetch city trip summary" },
        { status: 500 }
      );
    }

    return NextResponse.json({ data });
  } catch (error) {
    console.error("City trips API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch city trip data" },
      { status: 500 }
    );
  }
}