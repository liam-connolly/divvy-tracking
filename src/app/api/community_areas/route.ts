import { NextResponse } from "next/server";
import { getCommunityAreas } from "@/lib/db";

export async function GET() {
  try {
    const communityAreas = await getCommunityAreas();

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
