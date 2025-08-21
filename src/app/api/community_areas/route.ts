import { NextResponse } from "next/server";
import { getCommunityAreasForFilter } from "@/lib/db";

export async function GET() {
  try {
    const communityAreas = await getCommunityAreasForFilter();

    return NextResponse.json({
      communityAreas,
      count: communityAreas.length,
    });
  } catch (error) {
    console.error("Community areas API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch community areas" },
      { status: 500 }
    );
  }
}
