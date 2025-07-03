#!/usr/bin/env python3
"""
Simple crypto data downloader for Bybit tick data.

Usage:
    python download.py BTCUSDT 2024-01-01 2024-01-31
    python download.py BTCUSDT 2024-01-01 2024-01-31 -t 5m
    python download.py BTCUSDT 2024-01-01 2024-01-31 --timeframe 1h
"""
import argparse
import gzip
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download and convert Bybit tick data to OHLCV format"
    )
    parser.add_argument("symbol", help="Trading symbol (e.g., BTCUSDT)")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "-t", "--timeframe",
        default="1m",
        choices=["1s", "1m", "5m", "15m", "1h", "4h", "all"],
        help="Target timeframe for OHLCV conversion (default: 1m). Use 'all' to generate all timeframes."
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory (default: data)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum download retries (default: 3)"
    )
    return parser.parse_args()


def validate_symbol(symbol: str) -> str:
    """Validate and normalize symbol."""
    symbol = symbol.upper().strip()
    
    # 基本的な検証のみ行う
    if not symbol:
        raise ValueError("Symbol cannot be empty")
    
    # 英数字のみ許可（ハイフンやアンダースコアは含まない）
    if not symbol.isalnum():
        raise ValueError(
            f"Invalid symbol: {symbol}. "
            "Symbol must contain only letters and numbers"
        )
    
    # 最低3文字は必要
    if len(symbol) < 3:
        raise ValueError(
            f"Invalid symbol: {symbol}. "
            "Symbol must be at least 3 characters long"
        )
    
    return symbol


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def generate_dates(start_date: datetime, end_date: datetime):
    """Generate dates between start and end (inclusive)."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_download_url(symbol: str, date: datetime) -> str:
    """Build Bybit download URL for given symbol and date."""
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{symbol}{date_str}.csv.gz"
    return f"https://public.bybit.com/trading/{symbol}/{filename}"


def download_file(url: str, output_path: Path, max_retries: int = 3) -> bool:
    """
    Download file from URL with retry logic.
    
    Returns:
        True if successful, False otherwise
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CryptoDataFetcher/1.0)"
    }
    
    for attempt in range(max_retries):
        try:
            print(f"    Downloading... ", end="", flush=True)
            
            req = Request(url, headers=headers)
            with urlopen(req, timeout=300) as response:
                with open(output_path, "wb") as f:
                    shutil.copyfileobj(response, f)
            
            print("Done!")
            return True
            
        except (URLError, HTTPError) as e:
            if attempt < max_retries - 1:
                print(f"Failed! Retrying ({attempt + 1}/{max_retries})... ", end="", flush=True)
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed after {max_retries} attempts!")
                print(f"    Error: {e}")
                return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False
    
    return False


def decompress_file(gz_path: Path, csv_path: Path) -> bool:
    """
    Decompress .gz file to .csv.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"    Decompressing... ", end="", flush=True)
        with gzip.open(gz_path, "rb") as f_in:
            with open(csv_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Done!")
        return True
    except Exception as e:
        print(f"Failed!")
        print(f"    Error: {e}")
        return False


def convert_to_ohlcv(csv_path: Path, output_path: Path, timeframe: str = "1m") -> bool:
    """
    Convert tick data to OHLCV format.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"    Converting to {timeframe} OHLCV... ", end="", flush=True)
        
        # Get path to conversion script
        script_path = Path(__file__).parent / "scripts" / "convert_to_ohlcv.py"
        
        if not script_path.exists():
            print(f"Failed! Conversion script not found: {script_path}")
            return False
        
        # Run conversion script
        cmd = [
            sys.executable,
            str(script_path),
            str(csv_path),
            "-t", timeframe,
            "-o", str(output_path),
            "--chunk-size", "100000"
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("Done!")
            return True
        else:
            print("Failed!")
            print(f"    Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Failed!")
        print(f"    Error: {e}")
        return False


def process_date(symbol: str, date: datetime, output_dir: Path, timeframe: str, max_retries: int) -> bool:
    """
    Process a single date: download and convert.
    
    Returns:
        True if successful or already exists, False if failed
    """
    date_str = date.strftime("%Y-%m-%d")
    
    # Setup paths
    symbol_dir = output_dir / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine timeframes to process
    if timeframe == "all":
        timeframes = ["1s", "1m", "5m", "15m", "1h", "4h"]
    else:
        timeframes = [timeframe]
    
    # Check if all required files exist
    all_exist = True
    for tf in timeframes:
        output_file = symbol_dir / f"{symbol}_{date_str}_{tf}.csv"
        if not output_file.exists():
            all_exist = False
            break
    
    if all_exist:
        print(f"    All required files already exist, skipping")
        return True
    
    # Temporary files
    temp_dir = symbol_dir / "temp"
    temp_dir.mkdir(exist_ok=True)
    
    gz_file = temp_dir / f"{symbol}{date_str}.csv.gz"
    csv_file = temp_dir / f"{symbol}{date_str}.csv"
    
    try:
        # Download only if CSV file doesn't exist
        if not csv_file.exists():
            # Download
            url = build_download_url(symbol, date)
            if not download_file(url, gz_file, max_retries):
                return False
            
            # Decompress
            if not decompress_file(gz_file, csv_file):
                return False
            
            # Clean up gz file after decompression
            gz_file.unlink(missing_ok=True)
        else:
            print(f"    Using existing CSV file")
        
        # Convert to all required timeframes
        success = True
        for tf in timeframes:
            output_file = symbol_dir / f"{symbol}_{date_str}_{tf}.csv"
            if not output_file.exists():
                if not convert_to_ohlcv(csv_file, output_file, tf):
                    success = False
                    break
        
        # Cleanup temporary CSV file
        csv_file.unlink(missing_ok=True)
        
        return success
        
    except Exception as e:
        print(f"    Unexpected error: {e}")
        # Cleanup on error
        gz_file.unlink(missing_ok=True)
        csv_file.unlink(missing_ok=True)
        for tf in timeframes:
            output_file = symbol_dir / f"{symbol}_{date_str}_{tf}.csv"
            output_file.unlink(missing_ok=True)
        return False


def main():
    """Main entry point."""
    args = parse_arguments()
    
    try:
        # Validate inputs
        symbol = validate_symbol(args.symbol)
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        if start_date > end_date:
            print("Error: Start date must be before or equal to end date")
            return 1
        
        # Check date ranges
        earliest_date = datetime(2020, 3, 25)
        if start_date < earliest_date:
            print(f"Warning: Bybit data starts from 2020-03-25. Adjusting start date.")
            start_date = earliest_date
        
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date > today:
            print(f"Warning: Cannot download future data. Adjusting end date to today.")
            end_date = today
        
        # Setup output directory
        output_dir = Path(args.output_dir)
        
        # Process each date
        dates = list(generate_dates(start_date, end_date))
        total_days = len(dates)
        
        print(f"\nProcessing {symbol} from {start_date.date()} to {end_date.date()}...")
        print(f"Total days: {total_days}\n")
        
        success_count = 0
        failed_dates = []
        
        for i, date in enumerate(dates, 1):
            date_str = date.strftime("%Y-%m-%d")
            print(f"[{i}/{total_days}] {date_str}:")
            
            if process_date(symbol, date, output_dir, args.timeframe, args.max_retries):
                success_count += 1
            else:
                failed_dates.append(date_str)
        
        # Summary
        print(f"\n{'='*50}")
        print(f"Summary:")
        print(f"  Total days: {total_days}")
        print(f"  Successful: {success_count}")
        print(f"  Failed: {len(failed_dates)}")
        
        if failed_dates:
            print(f"\nFailed dates:")
            for date_str in failed_dates:
                print(f"  - {date_str}")
            print(f"\nTo retry failed dates, run:")
            print(f"  python download.py {symbol} {failed_dates[0]} {failed_dates[-1]}")
        else:
            print(f"\nAll downloads completed successfully!")
        
        return 0 if not failed_dates else 1
        
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())