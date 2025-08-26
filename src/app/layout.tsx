import type { Metadata } from "next";
import { Roboto } from "next/font/google";
import "./globals.css";
import StationsProvider from "@/contexts/StationsProvider";
import CommunityAreasProvider from "@/contexts/CommunityAreasProvider";
import { getStations, getCommunityAreas } from "./actions";
import { NuqsAdapter } from 'nuqs/adapters/next/app';

// Import Big Shoulders Display from Google Fonts via CSS
const bigShouldersDisplay = {
  variable: "--font-big-shoulders",
};

const roboto = Roboto({
  variable: "--font-roboto",
  subsets: ["latin"],
  weight: ["300", "400", "500", "700"],
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
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
      </head>
      <body
        className={`${bigShouldersDisplay.variable} ${roboto.variable} antialiased`}
        style={{ fontFamily: 'var(--font-roboto)' }}
      >
        <StationsProvider stations={stations}>
          <CommunityAreasProvider communityAreas={communityAreas}>
            <NuqsAdapter>
              {children}
            </NuqsAdapter>
          </CommunityAreasProvider>
        </StationsProvider>
      </body>
    </html>
  );
}
