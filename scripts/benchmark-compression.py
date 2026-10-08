#!/usr/bin/env python3
"""Compare ZIP compression policies on disposable synthetic data, with phase timings."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import statistics
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--bytes-per-file', type=int, default=16 * 1024 * 1024)
    args = parser.parse_args()
    if not 1 <= args.samples <= 100 or not 1 <= args.bytes_per_file <= 256 * 1024 * 1024:
        parser.error('samples must be 1..100 and bytes-per-file 1..268435456')
    binary = args.binary.resolve(strict=True)
    report = {'scope': 'Synthetic data on the development filesystem; not a deployed-HDD benchmark',
              'dataset': 'One incompressible byte stream with .jpg suffix and one repeated UTF-8 text file',
              'selected_bytes': args.bytes_per_file * 2, 'variants': {}}
    with tempfile.TemporaryDirectory(prefix='backup-compression-benchmark-') as temporary:
        root = Path(temporary)
        source = root / 'source'
        source.mkdir()
        with (source / 'incompressible.jpg').open('wb') as file:
            remaining = args.bytes_per_file
            while remaining:
                chunk = min(remaining, 1024 * 1024)
                file.write(os.urandom(chunk))
                remaining -= chunk
        with (source / 'compressible.txt').open('wb') as file:
            remaining = args.bytes_per_file
            block = (b'Synthetic text with repeatable compression characteristics.\n' * 20000)[:1024 * 1024]
            while remaining:
                chunk = block[:remaining]
                file.write(chunk)
                remaining -= len(chunk)
        for variant, suffixes in [('deflate_all', []), ('store_jpg', ['.jpg'])]:
            directory = root / variant
            directory.mkdir()
            settings = {'config_version': 1, 'state_dir': str(directory / 'state'),
                        'targets': [{'id': 'benchmark', 'source_dir': str(source),
                                     'destination_dir': str(directory / 'archives'),
                                     'storage': {'min_free_bytes': 0},
                                     'retention': {'min_snapshots': 1, 'max_snapshots': args.samples},
                                     'archive': {'compression_level': 1, 'store_extensions': suffixes}}]}
            config = directory / 'config.json'
            config.write_text(json.dumps(settings))
            for _ in range(args.samples):
                result = subprocess.run([str(binary), '--config', str(config), 'backup'],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
                if result.returncode:
                    raise RuntimeError(result.stderr)
            with sqlite3.connect(f'file:{directory / "state/state.sqlite3"}?mode=ro', uri=True) as database:
                records = [json.loads(row[0]) for row in database.execute('SELECT info FROM job_results ORDER BY finished_ms')]
            samples = [{'archive_bytes': record['snapshot']['bytes'], **record['metrics']} for record in records]
            report['variants'][variant] = {'sample_count': len(samples), 'samples': samples,
                                          'mean_total_elapsed_ms': statistics.mean(row['total_elapsed_ms'] for row in samples),
                                          'mean_archive_bytes': statistics.mean(row['archive_bytes'] for row in samples)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as output:
        json.dump(report, output, indent=2)
    print(json.dumps({name: {key: value for key, value in data.items() if key != 'samples'}
                      for name, data in report['variants'].items()}, indent=2))


if __name__ == '__main__':
    main()
