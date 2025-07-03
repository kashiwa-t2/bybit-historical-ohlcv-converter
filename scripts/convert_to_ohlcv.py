#!/usr/bin/env python3
"""
Convert large tick data CSV files to OHLCV format with multiple timeframe support.
Designed to handle very large files efficiently using streaming.

Supported timeframes: 1s, 1m, 5m, 15m, 1h, 4h, 1d
"""

import csv
import sys
from datetime import datetime
from pathlib import Path
import argparse
from typing import Dict, Optional, Tuple
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
    
    def __init__(self, timeframe: str, chunk_size: int = 100000):
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {list(self.TIMEFRAMES.keys())}")
        
        self.timeframe = timeframe
        self.interval_seconds = self.TIMEFRAMES[timeframe]
        self.chunk_size = chunk_size
        self.current_interval = None
        self.ohlcv_buffer = None
        
    def get_interval(self, timestamp: float) -> int:
        """Get the interval start timestamp for the given timestamp."""
        return int(math.floor(timestamp / self.interval_seconds) * self.interval_seconds)
        
    def reset_buffer(self):
        """Reset the OHLCV buffer for a new interval."""
        self.ohlcv_buffer = {
            'open': None,
            'high': float('-inf'),
            'low': float('inf'),
            'close': None,
            'volume': 0.0,
            'trades': 0
        }
    
    def process_tick(self, row: Dict[str, str]) -> Optional[Dict]:
        """Process a single tick and return OHLCV data if interval is complete."""
        timestamp = float(row['timestamp'])
        price = float(row['price'])
        size = float(row['size'])
        
        # Get the interval for this tick
        tick_interval = self.get_interval(timestamp)
        
        # If this is a new interval, output the previous interval's data
        result = None
        if self.current_interval is not None and tick_interval != self.current_interval:
            if self.ohlcv_buffer['open'] is not None:  # Ensure we have data
                result = {
                    'timestamp': self.current_interval,
                    'datetime': datetime.fromtimestamp(self.current_interval).isoformat(),
                    'open': self.ohlcv_buffer['open'],
                    'high': self.ohlcv_buffer['high'],
                    'low': self.ohlcv_buffer['low'],
                    'close': self.ohlcv_buffer['close'],
                    'volume': self.ohlcv_buffer['volume'],
                    'trades': self.ohlcv_buffer['trades']
                }
            self.reset_buffer()
        
        # Update current interval
        if self.current_interval != tick_interval:
            self.current_interval = tick_interval
            self.reset_buffer()
        
        # Update OHLCV data
        if self.ohlcv_buffer['open'] is None:
            self.ohlcv_buffer['open'] = price
        
        self.ohlcv_buffer['high'] = max(self.ohlcv_buffer['high'], price)
        self.ohlcv_buffer['low'] = min(self.ohlcv_buffer['low'], price)
        self.ohlcv_buffer['close'] = price
        self.ohlcv_buffer['volume'] += size
        self.ohlcv_buffer['trades'] += 1
        
        return result
    
    def get_final_candle(self) -> Optional[Dict]:
        """Get the final candle data."""
        if self.current_interval is not None and self.ohlcv_buffer['open'] is not None:
            return {
                'timestamp': self.current_interval,
                'datetime': datetime.fromtimestamp(self.current_interval).isoformat(),
                'open': self.ohlcv_buffer['open'],
                'high': self.ohlcv_buffer['high'],
                'low': self.ohlcv_buffer['low'],
                'close': self.ohlcv_buffer['close'],
                'volume': self.ohlcv_buffer['volume'],
                'trades': self.ohlcv_buffer['trades']
            }
        return None


def convert_file(input_path: Path, output_path: Path, timeframe: str, chunk_size: int = 100000):
    """Convert tick data file to OHLCV format with specified timeframe."""
    converter = TickToOHLCVConverter(timeframe, chunk_size)
    
    print(f"Processing {input_path}...")
    print(f"Converting to {timeframe} timeframe")
    print(f"Output will be saved to {output_path}")
    
    rows_processed = 0
    candles_written = 0
    
    # Open input and output files
    with open(input_path, 'r', newline='', encoding='utf-8') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Write header
        fieldnames = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'trades']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Process rows in chunks
        for row in reader:
            rows_processed += 1
            
            # Process tick
            candle = converter.process_tick(row)
            if candle:
                writer.writerow(candle)
                candles_written += 1
            
            # Progress update every chunk_size rows
            if rows_processed % chunk_size == 0:
                print(f"Processed {rows_processed:,} ticks, generated {candles_written:,} candles...")
        
        # Don't forget the last candle
        final_candle = converter.get_final_candle()
        if final_candle:
            writer.writerow(final_candle)
            candles_written += 1
    
    print(f"\nConversion complete!")
    print(f"Total ticks processed: {rows_processed:,}")
    print(f"Total {timeframe} candles generated: {candles_written:,}")
    print(f"Output saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Convert tick data to OHLCV format with multiple timeframe support',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supported timeframes: 1s, 1m, 5m, 15m, 1h, 4h, 1d

Examples:
  # Convert to 1-minute candles
  python convert_to_ohlcv.py input.csv -t 1m
  
  # Convert to 5-minute candles with custom output
  python convert_to_ohlcv.py input.csv -t 5m -o output_5m.csv
  
  # Convert to 1-hour candles with large chunk size
  python convert_to_ohlcv.py input.csv -t 1h --chunk-size 500000
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