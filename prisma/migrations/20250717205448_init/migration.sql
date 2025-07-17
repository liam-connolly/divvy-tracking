-- CreateTable
CREATE TABLE "Station" (
    "id" SERIAL NOT NULL,
    "communityArea" INTEGER,
    "latitude" TEXT NOT NULL,
    "longitude" TEXT NOT NULL,
    "name" TEXT NOT NULL,

    CONSTRAINT "Station_Pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StationDay" (
    "id" SERIAL NOT NULL,
    "acousticArrive" INTEGER NOT NULL,
    "acousticDepart" INTEGER NOT NULL,
    "day" INTEGER NOT NULL,
    "stationId" INTEGER NOT NULL,
    "electricArrive" INTEGER NOT NULL,
    "electricDepart" INTEGER NOT NULL,
    "month" INTEGER NOT NULL,
    "year" INTEGER NOT NULL,

    CONSTRAINT "StationDay_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Station_name_key" ON "Station"("name");

-- CreateIndex
CREATE INDEX "StationDay_stationId_idx" ON "StationDay"("stationId");

-- CreateIndex
CREATE UNIQUE INDEX "StationDay_day_month_year_stationId_key" ON "StationDay"("day", "month", "year", "stationId");

-- AddForeignKey
ALTER TABLE "StationDay" ADD CONSTRAINT "StationDay_stationId_fkey" FOREIGN KEY ("stationId") REFERENCES "Station"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
