#!/usr/bin/env python3
"""
Divvy Trip Data Downloader
Downloads and extracts monthly Divvy bike trip data from the public S3 bucket.
"""

import os
import sys
import zipfile
import requests
from pathlib import Path
from typing import List, Optional
import argparse
from datetime import datetime
import shutil

class DivvyDownloader:
    def __init__(self, data_dir: str = "data"):
        self.base_url = "https://divvy-tripdata.s3.amazonaws.com"
        self.data_dir = Path(data_dir)
        self.temp_dir = self.data_dir / "temp"
        self.csv_dir = self.data_dir / "csv"
        
        # Create directories if they don't exist
        self.data_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
    
    def get_filename(self, year: int, month: int) -> str:
        """Generate the filename for a given year and month."""
        return f"{year:04d}{month:02d}-divvy-tripdata.zip"
    
    def download_file(self, filename: str, force: bool = False) -> bool:
        """Download a single zip file from S3."""
        local_zip_path = self.temp_dir / filename
        
        # Skip if file already exists and not forcing
        if local_zip_path.exists() and not force:
            print(f"✓ {filename} already exists, skipping download")
            return True
        
        url = f"{self.base_url}/{filename}"
        print(f"⬇️  Downloading {filename}...")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get file size for progress tracking
            total_size = int(response.headers.get('content-length', 0))
            
            with open(local_zip_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Simple progress indicator
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(f"\r   Progress: {progress:.1f}%", end='', flush=True)
            
            print(f"\n✓ Downloaded {filename} ({downloaded:,} bytes)")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error downloading {filename}: {e}")
            # Clean up partial download
            if local_zip_path.exists():
                local_zip_path.unlink()
            return False
    
    def extract_file(self, filename: str, force: bool = False) -> bool:
        """Extract a zip file and move CSV to the csv directory."""
        zip_path = self.temp_dir / filename
        
        if not zip_path.exists():
            print(f"❌ {filename} not found in temp directory")
            return False
        
        # Expected CSV filename (without .zip extension)
        csv_filename = filename.replace('.zip', '.csv')
        csv_path = self.csv_dir / csv_filename
        
        # Skip if CSV already exists and not forcing
        if csv_path.exists() and not force:
            print(f"✓ {csv_filename} already extracted, skipping")
            return True
        
        print(f"📦 Extracting {filename}...")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List contents to find the CSV file
                file_list = zip_ref.namelist()
                csv_files = [f for f in file_list if f.endswith('.csv')]
                
                if not csv_files:
                    print(f"❌ No CSV file found in {filename}")
                    return False
                
                if len(csv_files) > 1:
                    print(f"⚠️  Multiple CSV files found in {filename}, using first one")
                
                # Extract the CSV file
                csv_in_zip = csv_files[0]
                zip_ref.extract(csv_in_zip, self.temp_dir)
                
                # Move to csv directory with consistent naming
                extracted_path = self.temp_dir / csv_in_zip
                extracted_path.rename(csv_path)
                
                print(f"✓ Extracted {csv_filename}")
                return True
                
        except zipfile.BadZipFile as e:
            print(f"❌ Error extracting {filename}: Invalid zip file")
            return False
        except Exception as e:
            print(f"❌ Error extracting {filename}: {e}")
            return False
    
    def cleanup_temp_files(self):
        """Remove temporary zip files."""
        print("🧹 Cleaning up temporary files...")
        for file in self.temp_dir.glob("*.zip"):
            file.unlink()
        print("✓ Temporary files cleaned up")
    
    def download_month(self, year: int, month: int, force: bool = False) -> bool:
        """Download and extract data for a specific month."""
        filename = self.get_filename(year, month)
        
        success = self.download_file(filename, force)
        if success:
            success = self.extract_file(filename, force)
        
        return success
    
    def download_year(self, year: int, force: bool = False) -> List[str]:
        """Download and extract data for an entire year."""
        print(f"📅 Downloading data for {year}")
        
        successful_downloads = []
        failed_downloads = []
        
        for month in range(1, 13):
            print(f"\n--- Processing {year}-{month:02d} ---")
            
            if self.download_month(year, month, force):
                successful_downloads.append(f"{year}-{month:02d}")
            else:
                failed_downloads.append(f"{year}-{month:02d}")
        
        print(f"\n📊 Download Summary for {year}:")
        print(f"✓ Successful: {len(successful_downloads)}")
        print(f"❌ Failed: {len(failed_downloads)}")
        
        if failed_downloads:
            print(f"Failed months: {', '.join(failed_downloads)}")
        
        return successful_downloads
    
    def list_downloaded_files(self):
        """List all downloaded CSV files."""
        csv_files = list(self.csv_dir.glob("*.csv"))
        
        if not csv_files:
            print("No CSV files found in data directory")
            return
        
        print(f"\n📁 Downloaded CSV files ({len(csv_files)}):")
        for file in sorted(csv_files):
            file_size = file.stat().st_size
            print(f"  {file.name} ({file_size:,} bytes)")
    
    def get_available_files(self) -> List[str]:
        """Get list of available CSV files for processing."""
        return [f.name for f in sorted(self.csv_dir.glob("*.csv"))]


def main():
    parser = argparse.ArgumentParser(description="Download Divvy trip data")
    parser.add_argument("--year", type=int, default=2025, help="Year to download (default: 2025)")
    parser.add_argument("--month", type=int, help="Specific month to download (1-12)")
    parser.add_argument("--force", action="store_true", help="Force re-download existing files")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't clean up zip files after extraction")
    parser.add_argument("--list", action="store_true", help="List downloaded files")
    parser.add_argument("--data-dir", default="data", help="Data directory (default: data)")
    
    args = parser.parse_args()
    
    downloader = DivvyDownloader(args.data_dir)
    
    if args.list:
        downloader.list_downloaded_files()
        return
    
    try:
        if args.month:
            # Download specific month
            print(f"Downloading data for {args.year}-{args.month:02d}")
            success = downloader.download_month(args.year, args.month, args.force)
            if not success:
                sys.exit(1)
        else:
            # Download entire year
            successful = downloader.download_year(args.year, args.force)
            if not successful:
                sys.exit(1)
        
        # Cleanup unless explicitly disabled
        if not args.no_cleanup:
            downloader.cleanup_temp_files()
        
        print(f"\n🎉 Download complete! CSV files are in: {downloader.csv_dir}")
        downloader.list_downloaded_files()
        
    except KeyboardInterrupt:
        print("\n⚠️  Download interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()