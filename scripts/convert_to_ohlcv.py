#!/usr/bin/env python3
"""
Convert large tick data CSV files to OHLCV format with gap filling.
Fills missing time intervals with previous close price and zero volume.

Supported timeframes: 1s, 1m, 5m, 15m, 1h, 4h, 1d
"""

import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path
import argparse
from typing import Dict, Optional, List
import math


class TickToOHLCVConverter:
    # Timeframe definitions in seconds
    TIMEFRAMES = {
        '1s': 1,
        '1m': 60,
        '5m': 300,
        '15m': 900,
        '1h': 3600,
        '4h': 14400,
        '1d': 86400
    }
    
    def __init__(self, timeframe: str):
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {list(self.TIMEFRAMES.keys())}")
        
        self.timeframe = timeframe
        self.interval_seconds = self.TIMEFRAMES[timeframe]
        self.ticks_by_interval = {}  # Dictionary to store ticks grouped by interval
        
    def get_interval(self, timestamp: float) -> int:
        """Get the interval start timestamp for the given timestamp."""
        return int(math.floor(timestamp / self.interval_seconds) * self.interval_seconds)
    
    def process_tick(self, row: Dict[str, str]):
        """Store tick data grouped by interval."""
        # Handle different timestamp formats (seconds vs milliseconds)
        timestamp = float(row['timestamp'])
        if timestamp > 1e10:  # If timestamp is in milliseconds
            timestamp = timestamp / 1000
        
        price = float(row['price'])
        # Handle different volume column names (size vs volume)
        size = float(row.get('size', row.get('volume', 0)))
        
        # Get the interval for this tick
        tick_interval = self.get_interval(timestamp)
        
        # Store tick data
        if tick_interval not in self.ticks_by_interval:
            self.ticks_by_interval[tick_interval] = []
        
        self.ticks_by_interval[tick_interval].append({
            'timestamp': timestamp,
            'price': price,
            'size': size
        })
    
    def get_ohlcv_for_interval(self, interval_timestamp: int, ticks: List[Dict]) -> Dict:
        """Calculate OHLCV for a given interval from tick data."""
        if not ticks:
            return None
            
        # Sort ticks by timestamp
        ticks = sorted(ticks, key=lambda x: x['timestamp'])
        
        open_price = ticks[0]['price']
        close_price = ticks[-1]['price']
        high_price = max(tick['price'] for tick in ticks)
        low_price = min(tick['price'] for tick in ticks)
        volume = sum(tick['size'] for tick in ticks)
        trades = len(ticks)
        
        return {
            'timestamp': interval_timestamp,
            'datetime': datetime.fromtimestamp(interval_timestamp).isoformat(),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume,
            'trades': trades
        }
    
    def generate_all_candles(self, start_time: int, end_time: int) -> List[Dict]:
        """Generate all candles including gaps filled with previous close."""
        candles = []
        last_close = None
        next_open = None
        
        # Find the first available data for initial gap filling
        if self.ticks_by_interval:
            first_interval = min(self.ticks_by_interval.keys())
            if first_interval > start_time:
                # We have a gap at the beginning
                first_ticks = self.ticks_by_interval[first_interval]
                if first_ticks:
                    sorted_ticks = sorted(first_ticks, key=lambda x: x['timestamp'])
                    next_open = sorted_ticks[0]['price']
        
        # Generate candle for each interval
        current_time = start_time
        while current_time <= end_time:
            if current_time in self.ticks_by_interval:
                # We have data for this interval
                candle = self.get_ohlcv_for_interval(current_time, self.ticks_by_interval[current_time])
                if candle:
                    candles.append(candle)
                    last_close = candle['close']
            else:
                # No data for this interval - fill the gap
                if last_close is not None:
                    # Use previous close
                    fill_price = last_close
                elif next_open is not None:
                    # No previous data, use next open
                    fill_price = next_open
                else:
                    # No data at all - skip this interval
                    current_time += self.interval_seconds
                    continue
                
                # Create gap-filled candle
                candles.append({
                    'timestamp': current_time,
                    'datetime': datetime.fromtimestamp(current_time).isoformat(),
                    'open': fill_price,
                    'high': fill_price,
                    'low': fill_price,
                    'close': fill_price,
                    'volume': 0.0,
                    'trades': 0
                })
            
            current_time += self.interval_seconds
        
        return candles


def convert_file(input_path: Path, output_path: Path, timeframe: str, chunk_size: int = 100000):
    """Convert tick data file to OHLCV format with gap filling."""
    converter = TickToOHLCVConverter(timeframe)
    
    print(f"Processing {input_path}...")
    print(f"Converting to {timeframe} timeframe with gap filling")
    print(f"Reading tick data...")
    
    rows_processed = 0
    min_timestamp = None
    max_timestamp = None
    
    # First pass: read all ticks and organize by interval
    with open(input_path, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        for row in reader:
            rows_processed += 1
            
            # Process tick
            converter.process_tick(row)
            
            # Track time range
            timestamp = float(row['timestamp'])
            if timestamp > 1e10:
                timestamp = timestamp / 1000
            
            if min_timestamp is None or timestamp < min_timestamp:
                min_timestamp = timestamp
            if max_timestamp is None or timestamp > max_timestamp:
                max_timestamp = timestamp
            
            # Progress update
            if rows_processed % chunk_size == 0:
                print(f"Read {rows_processed:,} ticks...")
    
    print(f"Total ticks read: {rows_processed:,}")
    
    if min_timestamp is None or max_timestamp is None:
        print("No valid data found in input file")
        return
    
    # Determine the full day range (09:00 UTC to next day 08:59:59 UTC)
    # Bybit data runs from 09:00 UTC to next day 08:59:59 UTC
    start_datetime = datetime.fromtimestamp(min_timestamp)
    
    # Find the 09:00 UTC for this day
    if start_datetime.hour < 9:
        # Data from previous day's session
        day_start = start_datetime.replace(hour=9, minute=0, second=0, microsecond=0) - timedelta(days=1)
    else:
        # Data from current day's session
        day_start = start_datetime.replace(hour=9, minute=0, second=0, microsecond=0)
    
    # End time is next day 08:59:59
    day_end = day_start + timedelta(hours=24) - timedelta(seconds=1)
    
    start_interval = converter.get_interval(day_start.timestamp())
    end_interval = converter.get_interval(day_end.timestamp())
    
    print(f"Generating continuous OHLCV data from {day_start} to {day_end}...")
    
    # Generate all candles including gaps
    candles = converter.generate_all_candles(start_interval, end_interval)
    
    # Write output
    print(f"Writing output to {output_path}...")
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        fieldnames = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'trades']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for candle in candles:
            writer.writerow(candle)
    
    print(f"\nConversion complete!")
    print(f"Total {timeframe} candles generated: {len(candles):,}")
    print(f"Expected candles for 24h period: {int(86400 / converter.interval_seconds):,}")
    print(f"Output saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Convert tick data to OHLCV format with gap filling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supported timeframes: 1s, 1m, 5m, 15m, 1h, 4h, 1d

This version fills gaps in the data:
- Missing intervals use previous close price
- Volume is set to 0 for gap-filled intervals
- If no previous data exists, uses the next available open price

Examples:
  # Convert to 1-second candles with gap filling
  python convert_to_ohlcv.py input.csv -t 1s
  
  # Convert to 1-minute candles with custom output
  python convert_to_ohlcv.py input.csv -t 1m -o output_1m.csv
        """
    )
    
    parser.add_argument('input_file', type=str, help='Path to input tick data CSV file')
    parser.add_argument('-t', '--timeframe', type=str, required=True,
                        choices=['1s', '1m', '5m', '15m', '1h', '4h', '1d'],
                        help='Target timeframe for OHLCV conversion')
    parser.add_argument('-o', '--output', type=str, help='Output file path (default: input_file with timeframe suffix)')
    parser.add_argument('--chunk-size', type=int, default=100000, 
                        help='Number of rows to process before showing progress (default: 100000)')
    
    args = parser.parse_args()
    
    # Validate input file
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist")
        sys.exit(1)
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        # Add timeframe suffix before extension
        output_path = input_path.parent / f"{input_path.stem}_{args.timeframe}{input_path.suffix}"
    
    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Perform conversion
    try:
        convert_file(input_path, output_path, args.timeframe, args.chunk_size)
    except KeyboardInterrupt:
        print("\nConversion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()