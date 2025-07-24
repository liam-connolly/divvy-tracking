import requests
from bs4 import BeautifulSoup
import os
import zipfile
from urllib.parse import urljoin
import re
from datetime import datetime
import tempfile

def download_divvy_data():
    base_url = "https://divvy-tripdata.s3.amazonaws.com/"
    
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Error accessing S3 bucket: {e}")
        return
    
    # Try XML parser first, fall back to html.parser
    try:
        soup = BeautifulSoup(response.content, 'xml')
    except Exception:
        print("⚠️  XML parser not available, using HTML parser...")
        soup = BeautifulSoup(response.content, 'html.parser')
    
    # Create data directory
    os.makedirs('divvy_data', exist_ok=True)
    
    # Find all ZIP files from Jan 2020 to present
    zip_files = []
    contents = soup.find_all('Contents')
    
    if not contents:
        # Try alternative parsing if Contents tag not found
        print("⚠️  Trying alternative parsing method...")
        contents = soup.find_all('key')
    
    print(f"Found {len(contents)} total files in S3 bucket")
    
    for content in contents:
        key_element = content.find('Key') or content
        if key_element:
            key = key_element.get_text()
            if key.endswith('.zip'):
                # Extract date from filename (format: YYYYMM-divvy-tripdata.zip)
                match = re.search(r'(\d{6})-divvy-tripdata\.zip', key)
                if match:
                    date_str = match.group(1)
                    year = int(date_str[:4])
                    month = int(date_str[4:])
                    
                    # Only download from Jan 2020 onwards
                    if year >= 2020 and (year > 2020 or month >= 1):
                        current_date = datetime.now()
                        if year < current_date.year or (year == current_date.year and month <= current_date.month):
                            zip_files.append(key)
    
    if not zip_files:
        print("❌ No matching ZIP files found. Check the S3 bucket structure.")
        return
    
    print(f"Found {len(zip_files)} ZIP files to download and extract")
    
    # Sort files chronologically
    zip_files.sort()
    
    downloaded_count = 0
    skipped_count = 0
    extracted_count = 0
    
    for zip_file in zip_files:
        # Check if CSV already exists with either naming convention
        expected_csv_old = zip_file.replace('.zip', '.csv')
        expected_csv_new = zip_file.replace('-divvy-tripdata.zip', '-divvy-publictripdata.csv')
        
        csv_path_old = os.path.join('divvy_data', os.path.basename(expected_csv_old))
        csv_path_new = os.path.join('divvy_data', os.path.basename(expected_csv_new))
        
        if os.path.exists(csv_path_old) or os.path.exists(csv_path_new):
            print(f"  ⏭️  Skipping {zip_file} (CSV already exists)")
            skipped_count += 1
            continue
        
        file_url = urljoin(base_url, zip_file)
        print(f"Downloading and extracting {zip_file}...")
        
        try:
            # Download ZIP file to temporary location
            response = requests.get(file_url, stream=True)
            response.raise_for_status()
            
            # Get file size for progress
            file_size = int(response.headers.get('content-length', 0))
            
            # Use temporary file for ZIP download
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    temp_zip.write(chunk)
                    downloaded += len(chunk)
                    
                    # Show progress for large files
                    if file_size > 0 and downloaded % (1024 * 1024) == 0:  # Every MB
                        progress = (downloaded / file_size) * 100
                        print(f"  Download progress: {progress:.1f}%", end='\r')
                
                temp_zip_path = temp_zip.name
            
            print(f"  ✅ Downloaded {zip_file} ({downloaded / (1024*1024):.1f} MB)")
            downloaded_count += 1
            
            # Extract ZIP file
            try:
                with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                    # List contents of ZIP
                    zip_contents = zip_ref.namelist()
                    print(f"  📦 ZIP contains: {zip_contents}")
                    
                    # Find actual CSV files (ignore macOS metadata)
                    csv_files_in_zip = [
                        f for f in zip_contents 
                        if f.endswith('.csv') and not f.startswith('__MACOSX') and not f.startswith('.')
                    ]
                    
                    print(f"  🔍 Filtered CSV files: {csv_files_in_zip}")
                    
                    if not csv_files_in_zip:
                        print(f"  ⚠️  No valid CSV files found in {zip_file}")
                        print(f"  🔍 All files in ZIP: {zip_contents}")
                        continue
                    
                    # Extract CSV files
                    for csv_file_in_zip in csv_files_in_zip:
                        print(f"  📤 Extracting {csv_file_in_zip}...")
                        
                        # Extract to a temporary location first
                        zip_ref.extract(csv_file_in_zip, 'temp_extract')
                        temp_csv_path = os.path.join('temp_extract', csv_file_in_zip)
                        
                        # Clean filename for final destination
                        clean_filename = os.path.basename(csv_file_in_zip)
                        final_path = os.path.join('divvy_data', clean_filename)
                        
                        # Read and examine the file content
                        try:
                            with open(temp_csv_path, 'rb') as temp_file:
                                # Read first 500 bytes as bytes to see raw content
                                raw_content = temp_file.read(500)
                                print(f"  🔍 First 200 chars (raw): {raw_content[:200]}")
                            
                            with open(temp_csv_path, 'r', encoding='utf-8', errors='ignore') as temp_file:
                                content = temp_file.read()
                                print(f"  🔍 First 200 chars (text): {repr(content[:200])}")
                                print(f"  🔍 File size: {len(content)} characters")
                            
                            # Check if file starts with proper CSV header
                            lines = content.split('\n')
                            print(f"  🔍 First 5 lines:")
                            for i, line in enumerate(lines[:5]):
                                print(f"    Line {i}: {repr(line[:100])}")
                            
                            csv_start = -1
                            for i, line in enumerate(lines):
                                if 'ride_id' in line or 'trip_id' in line:
                                    csv_start = i
                                    print(f"  ✅ Found CSV header at line {i}: {repr(line[:100])}")
                                    break
                            
                            if csv_start >= 0:
                                if csv_start > 0:
                                    content = '\n'.join(lines[csv_start:])
                                    print(f"  ✂️  Removed {csv_start} corrupted lines")
                                
                                # Write cleaned content
                                with open(final_path, 'w', encoding='utf-8') as final_file:
                                    final_file.write(content)
                                
                                # Verify the cleaned file
                                file_size_mb = os.path.getsize(final_path) / (1024 * 1024)
                                
                                # Quick validation - check first line
                                with open(final_path, 'r') as check_file:
                                    first_line = check_file.readline().strip()
                                    print(f"  ✅ Clean CSV: {clean_filename} ({file_size_mb:.1f} MB)")
                                    print(f"  ✅ Header: {first_line}")
                                    extracted_count += 1
                            else:
                                print(f"  ❌ Could not find valid CSV data in {csv_file_in_zip}")
                                print(f"  🔍 Searched through {len(lines)} lines")
                        
                        except Exception as read_error:
                            print(f"  ❌ Error processing {csv_file_in_zip}: {read_error}")
                        
                        finally:
                            # Clean up temp file
                            if os.path.exists(temp_csv_path):
                                os.remove(temp_csv_path)
                    
                    # Clean up temp directory
                    import shutil
                    if os.path.exists('temp_extract'):
                        shutil.rmtree('temp_extract')
            
            except zipfile.BadZipFile:
                print(f"  ❌ Error: {zip_file} is not a valid ZIP file")
            except Exception as e:
                print(f"  ❌ Error extracting {zip_file}: {e}")
            
            # Clean up temporary ZIP file
            try:
                os.unlink(temp_zip_path)
            except:
                pass
                
        except Exception as e:
            print(f"  ❌ Error downloading {zip_file}: {e}")
    
    print(f"\n📊 Summary:")
    print(f"  Downloaded: {downloaded_count} ZIP files")
    print(f"  Extracted: {extracted_count} CSV files")
    print(f"  Skipped: {skipped_count} files")
    print(f"  Total ZIP files processed: {len(zip_files)}")

if __name__ == "__main__":
    print("Starting Divvy data download and extraction...")
    print("This will download ZIP files from January 2020 to present and extract the CSV files")
    print("Note: This may take a while and download several GB of data")
    
    confirm = input("Continue? (y/N): ")
    if confirm.lower() in ['y', 'yes']:
        download_divvy_data()
        print("\n✅ Download and extraction complete!")
        print("CSV files are now available in the 'divvy_data/' directory")
    else:
        print("Download cancelled.")