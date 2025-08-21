import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import StationsProvider from "@/contexts/StationsProvider";
import CommunityAreasProvider from "@/contexts/CommunityAreasProvider";
import { getStations, getCommunityAreas } from "./actions";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Divvy Analytics Dashboard",
  description: "Explore Chicago Divvy bike share data by station and community area",
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  // Fetch data server-side for context providers
  const [stations, communityAreas] = await Promise.all([
    getStations(),
    getCommunityAreas(),
  ]);

  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <StationsProvider stations={stations}>
          <CommunityAreasProvider communityAreas={communityAreas}>
            {children}
          </CommunityAreasProvider>
        </StationsProvider>
      </body>
    </html>
  );
}
