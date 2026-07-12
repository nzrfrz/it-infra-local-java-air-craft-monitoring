"""Hadoop Streaming reducer untuk density_grid_hourly -- jumlahkan count per
key (grid_cell@hour, dikirim mapper sudah ter-sort oleh MapReduce), lalu
pecah balik keynya saat print baris output final grid_cell\thour\tcount."""
import sys


def _emit(key, count):
    """Pecah key gabungan "grid_cell@hour" balik jadi 2 kolom terpisah di
    output final -- kebalikan dari penggabungan yang dilakukan mapper."""
    grid_cell, _, hour = key.rpartition("@")
    print(f"{grid_cell}\t{hour}\t{count}")


def main():
    # Hadoop menjamin semua baris dengan key yang sama datang BERURUTAN
    # (sudah di-sort sebelum sampai ke reducer) -- pola standar Hadoop
    # Streaming: akumulasi selama key sama, emit & reset saat key berganti.
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
            # Key baru ditemukan -> key sebelumnya sudah selesai, tulis hasilnya dulu.
            if current_key is not None:
                _emit(current_key, current_count)
            current_key = key
            current_count = 0
        current_count += value
    # Key terakhir tidak pernah "berganti" di dalam loop -- harus di-emit
    # manual setelah loop selesai (off-by-one klasik pola MapReduce reducer).
    if current_key is not None:
        _emit(current_key, current_count)


if __name__ == "__main__":
    main()
