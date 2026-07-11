"""Hadoop Streaming reducer untuk density_grid_hourly -- jumlahkan count per
key (grid_cell@hour, dikirim mapper sudah ter-sort oleh MapReduce), lalu
pecah balik keynya saat print baris output final grid_cell\thour\tcount."""
import sys


def _emit(key, count):
    grid_cell, _, hour = key.rpartition("@")
    print(f"{grid_cell}\t{hour}\t{count}")


def main():
    current_key = None
    current_count = 0
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        key, _, value = line.partition("\t")
        try:
            value = int(value)
        except ValueError:
            continue
        if key != current_key:
            if current_key is not None:
                _emit(current_key, current_count)
            current_key = key
            current_count = 0
        current_count += value
    if current_key is not None:
        _emit(current_key, current_count)


if __name__ == "__main__":
    main()
