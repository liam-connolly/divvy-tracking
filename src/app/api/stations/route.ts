import { NextResponse } from "next/server";
import { getAllStationsGrouped } from "@/lib/db";

export async function GET() {
  try {
    const stations = await getAllStationsGrouped();

    return NextResponse.json({
      stations,
      count: stations.length,
    });
  } catch (error) {
    console.error("All stations API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch stations" },
      { status: 500 }
    );
  }
}
