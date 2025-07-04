#!/usr/bin/env python3
"""
Simple crypto data downloader for Bybit tick data.

Usage:
    python download.py BTCUSDT --full -t all
    python download.py BTCUSDT --start 2024-01-01 --end 2024-01-31
    python download.py BTCUSDT --start 2024-01-01
    python download.py BTCUSDT --end 2024-01-31
"""
import argparse
import gzip
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple, List
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


class MarketType(Enum):
    FUTURES = "futures"
    SPOT = "spot"
    BOTH = "both"


def interactive_mode():
    """Interactive mode for easier usage."""
    print("=" * 50)
    print("  Bybit Historical Data Downloader")
    print("=" * 50)
    print()
    
    # Get symbol
    print("利用可能な取引ペア: https://public.bybit.com/trading/")
    print("※ USDTは自動的に追加されます（例: BTC → BTCUSDT）")
    while True:
        symbol = input("取引ペアを入力してください (例: BTC): ").strip().upper()
        if symbol:
            break
        print("取引ペアを入力してください。")
    
    try:
        symbol = validate_symbol(symbol)
    except ValueError as e:
        print(f"エラー: {e}")
        return 1
    
    # Ask for market type
    print("\n市場タイプを選択してください:")
    print("  [1] 先物 (Futures)")
    print("  [2] 現物 (Spot)")
    print("  [3] 両方 (Both)")
    
    while True:
        market_choice = input("選択 [1]: ").strip() or "1"
        if market_choice == "1":
            market_type = MarketType.FUTURES
            break
        elif market_choice == "2":
            market_type = MarketType.SPOT
            break
        elif market_choice == "3":
            market_type = MarketType.BOTH
            break
        else:
            print("1-3の数字を入力してください。")
    
    # Get date range option
    print("\nダウンロードする期間を選択してください:")
    print("  [1] 利用可能な全ての期間")
    print("  [2] 期間を指定する")
    print("  [3] 指定した日付から最新まで")
    print("  [4] 最古から指定した日付まで")
    
    while True:
        choice = input("選択 [1]: ").strip() or "1"
        if choice in ["1", "2", "3", "4"]:
            break
        print("1-4の数字を入力してください。")
    
    # Handle date range based on choice
    start_date = None
    end_date = None
    
    if choice == "1":
        # Full range
        print("    全期間の日付範囲を取得中...")
        earliest_available, latest_available = fetch_available_date_range(symbol, market_type)
        if earliest_available is None or latest_available is None:
            print(f"日付範囲の自動取得に失敗しました。")
            print(f"一般的な期間 (2020-01-01 ～ 昨日) を使用しますか？")
            fallback_choice = input("'y' で続行、'n' で中止 [y]: ").strip().lower() or 'y'
            if fallback_choice != 'y':
                print("処理を中止しました。")
                return 1
            start_date = datetime(2020, 1, 1)
            end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        else:
            start_date = earliest_available
            end_date = latest_available
        
        date_desc = f"全期間 ({start_date.date()} ～ {end_date.date()})"
        
    elif choice == "2":
        # Specific range
        while True:
            start_str = input("開始日を入力してください (YYYY-MM-DD): ").strip()
            try:
                start_date = parse_date(start_str)
                break
            except ValueError:
                print("正しい日付形式で入力してください (YYYY-MM-DD)")
        
        while True:
            end_str = input("終了日を入力してください (YYYY-MM-DD): ").strip()
            try:
                end_date = parse_date(end_str)
                break
            except ValueError:
                print("正しい日付形式で入力してください (YYYY-MM-DD)")
        
        if start_date > end_date:
            print("エラー: 開始日は終了日より前である必要があります。")
            return 1
        
        date_desc = f"{start_date.date()} ～ {end_date.date()}"
        
    elif choice == "3":
        # From start date to latest (UTC yesterday)
        while True:
            start_str = input("開始日を入力してください (YYYY-MM-DD): ").strip()
            try:
                start_date = parse_date(start_str)
                break
            except ValueError:
                print("正しい日付形式で入力してください (YYYY-MM-DD)")
        
        # Use yesterday as end date (data is typically available until yesterday)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = today - timedelta(days=1)
        date_desc = f"{start_date.date()} ～ 最新 ({end_date.date()})"
        
    elif choice == "4":
        # From earliest to end date
        while True:
            end_str = input("終了日を入力してください (YYYY-MM-DD): ").strip()
            try:
                end_date = parse_date(end_str)
                break
            except ValueError:
                print("正しい日付形式で入力してください (YYYY-MM-DD)")
        
        print("    最古日付を取得中...")
        earliest_available, _ = fetch_available_date_range(symbol, market_type)
        if earliest_available is None:
            print(f"最古日付の自動取得に失敗しました。")
            print(f"一般的な開始日を使用するか、手動で指定してください。")
            while True:
                fallback_choice = input("開始日を入力してください (YYYY-MM-DD) または 'auto' で2020-01-01: ").strip()
                if fallback_choice.lower() == 'auto':
                    start_date = datetime(2020, 1, 1)
                    break
                else:
                    try:
                        start_date = parse_date(fallback_choice)
                        break
                    except ValueError:
                        print("正しい日付形式で入力してください (YYYY-MM-DD)")
        else:
            start_date = earliest_available
        
        date_desc = f"{start_date.date()} ～ {end_date.date()}"
    
    # Get timeframe option
    print("\n時間足を選択してください:")
    print("  [1] 全ての時間足 (1s, 1m, 5m, 15m, 1h, 4h, 1d)")
    print("  [2] カスタム選択")
    
    while True:
        tf_choice = input("選択 [1]: ").strip() or "1"
        if tf_choice in ["1", "2"]:
            break
        print("1-2の数字を入力してください。")
    
    if tf_choice == "1":
        timeframe = "all"
        tf_desc = "全ての時間足 (1s, 1m, 5m, 15m, 1h, 4h, 1d)"
    elif tf_choice == "2":
        print("\n利用可能な時間足 (複数選択可能):")
        timeframes = ["1s", "1m", "5m", "15m", "1h", "4h", "1d"]
        for i, tf in enumerate(timeframes, 1):
            print(f"  [{i}] {tf}")
        
        while True:
            selection = input("番号を入力してください (例: 1,3,5 または 1 3 5): ").strip()
            if not selection:
                print("少なくとも1つの時間足を選択してください。")
                continue
            
            try:
                # Parse comma-separated or space-separated numbers
                if ',' in selection:
                    numbers = [int(x.strip()) for x in selection.split(',')]
                else:
                    numbers = [int(x.strip()) for x in selection.split()]
                
                selected_timeframes = []
                valid = True
                for num in numbers:
                    if 1 <= num <= len(timeframes):
                        tf = timeframes[num - 1]
                        if tf not in selected_timeframes:
                            selected_timeframes.append(tf)
                        # 重複は無視
                    else:
                        print(f"無効な番号: {num} (1-{len(timeframes)}の範囲で入力してください)")
                        valid = False
                        break
                
                if valid and selected_timeframes:
                    break
                
            except ValueError:
                print("数字を入力してください (例: 1,3,5 または 1 3 5)")
        
        if len(selected_timeframes) == 1:
            timeframe = selected_timeframes[0]
            tf_desc = selected_timeframes[0]
        else:
            # For multiple timeframes, we'll process them as individual downloads
            timeframe = "custom_multiple"
            tf_desc = f"カスタム選択 ({', '.join(selected_timeframes)})"
    
    # Get output directory
    output_dir = input(f"\n出力ディレクトリ [data]: ").strip() or "data"
    
    # Confirmation
    print("\n" + "=" * 30)
    print("  設定確認")
    print("=" * 30)
    print(f"取引ペア: {symbol}")
    market_desc = {MarketType.FUTURES: "先物", MarketType.SPOT: "現物", MarketType.BOTH: "両方"}[market_type]
    print(f"市場タイプ: {market_desc}")
    print(f"期間: {date_desc}")
    print(f"時間足: {tf_desc}")
    print(f"出力先: {output_dir}/")
    print()
    
    confirm = input("この設定で実行しますか？ [Y/n]: ").strip().lower()
    if confirm and confirm not in ["y", "yes"]:
        print("処理をキャンセルしました。")
        return 0
    
    # Execute download
    print("\nダウンロードを開始します...")
    print()
    
    # Call the main download logic
    if timeframe == "custom_multiple":
        return execute_download(symbol, start_date, end_date, timeframe, output_dir, market_type, max_retries=3, custom_timeframes=selected_timeframes)
    else:
        return execute_download(symbol, start_date, end_date, timeframe, output_dir, market_type, max_retries=3)


def execute_download(symbol: str, start_date: datetime, end_date: datetime, timeframe: str, output_dir: str, market_type: MarketType = MarketType.FUTURES, max_retries: int = 3, custom_timeframes: list = None) -> int:
    """Execute the download process with the given parameters."""
    try:
        # Check for future dates
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date > today:
            print(f"Warning: Cannot download future data. Adjusting end date to today.")
            end_date = today
        
        if start_date > end_date:
            print("Error: Start date must be before or equal to end date")
            return 1
        
        # Setup output directory
        output_dir_path = Path(output_dir)
        
        # Process each date
        dates = list(generate_dates(start_date, end_date))
        total_days = len(dates)
        
        print(f"\nProcessing {symbol} from {start_date.date()} to {end_date.date()}...")
        print(f"Total days: {total_days}\n")
        
        success_count = 0
        failed_dates = []
        consecutive_failures = 0
        
        for i, date in enumerate(dates, 1):
            date_str = date.strftime("%Y-%m-%d")
            print(f"[{i}/{total_days}] {date_str}:")
            
            if process_date(symbol, date, output_dir_path, timeframe, market_type, max_retries, custom_timeframes):
                success_count += 1
                consecutive_failures = 0  # Reset consecutive failure counter
            else:
                failed_dates.append(date_str)
                consecutive_failures += 1
                
                # Check for too many consecutive failures
                if consecutive_failures >= 3:
                    print(f"\n⚠️  3日連続でデータが見つかりませんでした。")
                    print(f"   対象の取引ペア '{symbol}' のデータが利用可能か確認してください:")
                    print(f"   https://public.bybit.com/trading/{symbol}")
                    print(f"\n   処理を中止します。")
                    break
        
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
            print(f"  python download.py --start {failed_dates[0]} --end {failed_dates[-1]}")
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


def fetch_available_date_range(symbol: str, market_type: MarketType = MarketType.FUTURES) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Fetch the available date range for a symbol from Bybit public data.
    
    Returns:
        Tuple of (earliest_date, latest_date) or (None, None) if failed
    """
    try:
        # Determine URL based on market type
        if market_type == MarketType.SPOT:
            url = f"https://public.bybit.com/spot/{symbol}/"
        else:  # FUTURES or BOTH (use futures for BOTH)
            url = f"https://public.bybit.com/trading/{symbol}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; BybitHistoricalOHLCVConverter/1.0)"
        }
        
        print(f"    Checking available date range for {symbol}... ", end="", flush=True)
        
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as response:
            html_content = response.read().decode('utf-8')
        
        # Extract file names matching patterns: Try different formats
        if market_type == MarketType.SPOT:
            # Spot uses daily files: SYMBOL_YYYY-MM-DD.csv.gz
            patterns_to_try = [
                rf'{re.escape(symbol)}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv\.gz',  # SYMBOL_YYYY-MM-DD.csv.gz
            ]
        else:
            # Futures uses daily files
            patterns_to_try = [
                rf'{re.escape(symbol)}\[(\d{{4}}-\d{{2}}-\d{{2}})\]\.csv\.gz',  # SYMBOL[YYYY-MM-DD].csv.gz
                rf'{re.escape(symbol)}(\d{{4}}-\d{{2}}-\d{{2}})\.csv\.gz',     # SYMBOLYYYY-MM-DD.csv.gz
                rf'{re.escape(symbol)}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv\.gz',    # SYMBOL_YYYY-MM-DD.csv.gz
                rf'{re.escape(symbol)}-(\d{{4}}-\d{{2}}-\d{{2}})\.csv\.gz',    # SYMBOL-YYYY-MM-DD.csv.gz
            ]
        
        matches = None
        for pattern in patterns_to_try:
            matches = re.findall(pattern, html_content)
            if matches:
                break
        
        if not matches:
            print("No data files found!")
            return None, None
        
        # Parse dates and find min/max
        dates = []
        for date_str in matches:
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                dates.append(date_obj)
            except ValueError:
                continue
        
        if not dates:
            print("No valid dates found!")
            return None, None
        
        earliest_date = min(dates)
        latest_date = max(dates)
        
        print(f"Found {len(dates)} files ({earliest_date.date()} to {latest_date.date()})")
        return earliest_date, latest_date
        
    except Exception as e:
        print(f"Failed to fetch date range: {e}")
        return None, None


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download and convert Bybit tick data to OHLCV format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download full available range (futures)
  python download.py BTCUSDT --full

  # Download specific date range (spot market)
  python download.py BTCUSDT --start 2024-01-01 --end 2024-01-31 --market-type spot

  # Download both futures and spot data
  python download.py BTCUSDT --start 2024-01-01 --market-type both -t all

  # Download from start date to latest
  python download.py BTCUSDT --start 2024-01-01

  # Download from earliest to end date
  python download.py BTCUSDT --end 2024-01-31

  # Download with all timeframes
  python download.py BTCUSDT --full -t all
        """
    )
    
    # Required positional argument
    parser.add_argument("symbol", help="Trading symbol (e.g., BTCUSDT)")
    
    # Date range options (mutually exclusive groups)
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--full",
        action="store_true",
        help="Download all available data for the symbol (auto-detect date range)"
    )
    
    # When not using --full, can specify start/end independently
    parser.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Start date. If not specified with --end, downloads from earliest available"
    )
    parser.add_argument(
        "--end", 
        metavar="YYYY-MM-DD",
        help="End date. If not specified with --start, downloads to latest available"
    )
    
    # Other options
    parser.add_argument(
        "-t", "--timeframe",
        default="1m",
        choices=["1s", "1m", "5m", "15m", "1h", "4h", "1d", "all"],
        help="Target timeframe for OHLCV conversion (default: 1m). Use 'all' to generate all timeframes."
    )
    parser.add_argument(
        "--market-type",
        default="futures",
        choices=["futures", "spot", "both"],
        help="Market type (default: futures)"
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
    """Validate and normalize symbol, automatically append USDT if needed."""
    symbol = symbol.upper().strip()
    
    # 基本的な検証
    if not symbol:
        raise ValueError("Symbol cannot be empty")
    
    # 英数字のみ許可
    if not symbol.isalnum():
        raise ValueError(
            f"Invalid symbol: {symbol}. "
            "Symbol must contain only letters and numbers"
        )
    
    # 最低2文字は必要（BTなど）
    if len(symbol) < 2:
        raise ValueError(
            f"Invalid symbol: {symbol}. "
            "Symbol must be at least 2 characters long"
        )
    
    # USDTが含まれていない場合は自動的に追加
    if not symbol.endswith('USDT'):
        symbol = symbol + 'USDT'
    
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


def build_download_url(symbol: str, date: datetime, market_type: MarketType = MarketType.FUTURES) -> str:
    """Build Bybit download URL for given symbol and date."""
    date_str = date.strftime("%Y-%m-%d")
    
    if market_type == MarketType.SPOT:
        # Spot uses daily files: SYMBOL_YYYY-MM-DD.csv.gz
        filename = f"{symbol}_{date_str}.csv.gz"
        return f"https://public.bybit.com/spot/{symbol}/{filename}"
    else:
        # Futures uses daily files: SYMBOL[YYYY-MM-DD].csv.gz
        filename = f"{symbol}{date_str}.csv.gz"
        return f"https://public.bybit.com/trading/{symbol}/{filename}"


def download_file(url: str, output_path: Path, max_retries: int = 3) -> bool:
    """
    Download file from URL with retry logic.
    
    Returns:
        True if successful, False otherwise
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BybitHistoricalOHLCVConverter/1.0)"
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


def process_date(symbol: str, date: datetime, output_dir: Path, timeframe: str, market_type: MarketType, max_retries: int, custom_timeframes: list = None) -> bool:
    """
    Process a single date: download and convert.
    
    Returns:
        True if successful or already exists, False if failed
    """
    # Handle BOTH market type by processing both futures and spot
    if market_type == MarketType.BOTH:
        futures_success = process_date(symbol, date, output_dir, timeframe, MarketType.FUTURES, max_retries, custom_timeframes)
        spot_success = process_date(symbol, date, output_dir, timeframe, MarketType.SPOT, max_retries, custom_timeframes)
        return futures_success and spot_success
    
    date_str = date.strftime("%Y-%m-%d")
    
    # Setup paths with market type subdirectory
    market_subdir = "futures" if market_type == MarketType.FUTURES else "spot"
    symbol_dir = output_dir / symbol / market_subdir
    symbol_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine timeframes to process
    if timeframe == "all":
        timeframes = ["1s", "1m", "5m", "15m", "1h", "4h", "1d"]
    elif timeframe == "custom_multiple":
        timeframes = custom_timeframes or ["1m"]
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
    
    try:
        # Download daily file for both spot and futures
        if market_type == MarketType.SPOT:
            # Spot uses underscore format
            gz_file = temp_dir / f"{symbol}_{date_str}.csv.gz"
            csv_file = temp_dir / f"{symbol}_{date_str}.csv"
        else:
            # Futures uses bracket format
            gz_file = temp_dir / f"{symbol}{date_str}.csv.gz"
            csv_file = temp_dir / f"{symbol}{date_str}.csv"
        
        # Download only if CSV file doesn't exist
        if not csv_file.exists():
            # Download
            url = build_download_url(symbol, date, market_type)
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
    # Check if we should enter interactive mode
    if len(sys.argv) == 1:
        # No arguments provided - enter interactive mode
        return interactive_mode()
    
    # Parse arguments for CLI mode
    args = parse_arguments()
    
    # Check if only symbol is provided without date options - suggest interactive mode
    if not (args.full or args.start or args.end):
        print("Tip: Run 'python download.py' without arguments for interactive mode")
        print()
        print("Error: Please specify date range using one of:")
        print("  --full                           (download all available data)")
        print("  --start YYYY-MM-DD --end YYYY-MM-DD  (specific range)")
        print("  --start YYYY-MM-DD               (from date to latest)")
        print("  --end YYYY-MM-DD                 (from earliest to date)")
        return 1
    
    try:
        # Validate inputs
        symbol = validate_symbol(args.symbol)
        
        # Market type from argument
        market_type = MarketType[args.market_type.upper()]
        
        # Handle date range logic
        if args.full:
            # Full range: fetch all available data
            earliest_available, latest_available = fetch_available_date_range(symbol, market_type)
            if earliest_available is None or latest_available is None:
                print("Error: Could not determine available date range for this symbol")
                url_path = "spot" if market_type == MarketType.SPOT else "trading"
                print(f"Please check https://public.bybit.com/{url_path}/{symbol} and specify dates manually")
                return 1
            
            start_date = earliest_available
            end_date = latest_available
            print(f"Using full available range: {start_date.date()} to {end_date.date()}")
            
        elif args.start or args.end:
            # Partial range: fetch available range if needed
            if not args.start or not args.end:
                print("Fetching available date range to determine missing dates...")
                earliest_available, latest_available = fetch_available_date_range(symbol, market_type)
                if earliest_available is None or latest_available is None:
                    print("Error: Could not determine available date range for this symbol")
                    url_path = "spot" if market_type == MarketType.SPOT else "trading"
                    print(f"Please check https://public.bybit.com/{url_path}/{symbol}")
                    return 1
            
            # Use provided dates or fall back to available range
            start_date = parse_date(args.start) if args.start else earliest_available
            end_date = parse_date(args.end) if args.end else latest_available
            
            # Ensure dates are within available range if we fetched them
            if not args.start or not args.end:
                if start_date < earliest_available:
                    print(f"Warning: Start date {start_date.date()} is before available data ({earliest_available.date()}). Using {earliest_available.date()}.")
                    start_date = earliest_available
                if end_date > latest_available:
                    print(f"Warning: End date {end_date.date()} is after available data ({latest_available.date()}). Using {latest_available.date()}.")
                    end_date = latest_available
        
        # Execute download
        return execute_download(symbol, start_date, end_date, args.timeframe, args.output_dir, market_type, args.max_retries)
        
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