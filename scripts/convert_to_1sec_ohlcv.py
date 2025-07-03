#!/usr/bin/env python3
"""
Convert large tick data CSV files to 1-second OHLCV format.
Designed to handle very large files efficiently using streaming.
"""

import csv
import sys
from datetime import datetime
from pathlib import Path
import argparse
from typing import Dict, Optional
import math


class TickToOHLCVConverter:
    def __init__(self, chunk_size: int = 100000):
        self.chunk_size = chunk_size
        self.current_second = None
        self.ohlcv_buffer = None
        
    def reset_buffer(self):
        """Reset the OHLCV buffer for a new second."""
        self.ohlcv_buffer = {
            'open': None,
            'high': float('-inf'),
            'low': float('inf'),
            'close': None,
            'volume': 0.0,
            'trades': 0
        }
    
    def process_tick(self, row: Dict[str, str]) -> Optional[Dict]:
        """Process a single tick and return OHLCV data if second is complete."""
        timestamp = float(row['timestamp'])
        price = float(row['price'])
        size = float(row['size'])
        
        # Extract second (floor the timestamp)
        tick_second = int(math.floor(timestamp))
        
        # If this is a new second, output the previous second's data
        result = None
        if self.current_second is not None and tick_second != self.current_second:
            if self.ohlcv_buffer['open'] is not None:  # Ensure we have data
                result = {
                    'timestamp': self.current_second,
                    'datetime': datetime.fromtimestamp(self.current_second).isoformat(),
                    'open': self.ohlcv_buffer['open'],
                    'high': self.ohlcv_buffer['high'],
                    'low': self.ohlcv_buffer['low'],
                    'close': self.ohlcv_buffer['close'],
                    'volume': self.ohlcv_buffer['volume'],
                    'trades': self.ohlcv_buffer['trades']
                }
            self.reset_buffer()
        
        # Update current second
        if self.current_second != tick_second:
            self.current_second = tick_second
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
        if self.current_second is not None and self.ohlcv_buffer['open'] is not None:
            return {
                'timestamp': self.current_second,
                'datetime': datetime.fromtimestamp(self.current_second).isoformat(),
                'open': self.ohlcv_buffer['open'],
                'high': self.ohlcv_buffer['high'],
                'low': self.ohlcv_buffer['low'],
                'close': self.ohlcv_buffer['close'],
                'volume': self.ohlcv_buffer['volume'],
                'trades': self.ohlcv_buffer['trades']
            }
        return None


def convert_file(input_path: Path, output_path: Path, chunk_size: int = 100000):
    """Convert tick data file to 1-second OHLCV format."""
    converter = TickToOHLCVConverter(chunk_size)
    
    # Count lines for progress (optional, can be skipped for very large files)
    print(f"Processing {input_path}...")
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
    print(f"Total 1-second candles generated: {candles_written:,}")
    print(f"Output saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Convert tick data to 1-second OHLCV format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert the sample file
  python convert_to_1sec_ohlcv.py ../data/BTCUSD2025-07-01-short.csv
  
  # Convert a large file with custom chunk size
  python convert_to_1sec_ohlcv.py ../data/BTCUSD2025-07-01.csv --chunk-size 500000
  
  # Specify custom output path
  python convert_to_1sec_ohlcv.py input.csv -o output_1sec.csv
        """
    )
    
    parser.add_argument('input_file', type=str, help='Path to input tick data CSV file')
    parser.add_argument('-o', '--output', type=str, help='Output file path (default: input_file with _1sec suffix)')
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
        # Add _1sec suffix before extension
        output_path = input_path.parent / f"{input_path.stem}_1sec{input_path.suffix}"
    
    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Perform conversion
    try:
        convert_file(input_path, output_path, args.chunk_size)
    except KeyboardInterrupt:
        print("\nConversion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()