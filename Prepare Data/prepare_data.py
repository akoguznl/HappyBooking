"""
╔══════════════════════════════════════════════════════════════════╗
║         HappyBooking — Veri Bölme (Split)                        ║
║         booking_dirty.csv → %70 Batch + %30 Stream               ║
║                                                                  ║
║  Tarih Kurtarma Stratejisi:                                      ║
║    1. booking_date temizlenip parse edilebiliyorsa → kullan      ║
║    2. Değilse checkin_date'den türet                             ║
║    3. O da olmazsa checkout_date'den türet                       ║
║    4. Hiçbiri olmazsa → gerçekten sil (truly_invalid_dates.csv)  ║
╚══════════════════════════════════════════════════════════════════╝
"""

import sys
import pandas as pd
import os
import logging
from datetime import datetime

# ─────────────────────────────────────────────────────────────────
# LOGGING AYARLARI — Windows UTF-8 Fix
# ─────────────────────────────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_file_handler   = logging.FileHandler("data/split_log.txt", encoding="utf-8")
_stream_handler = logging.StreamHandler(sys.stdout)
_file_handler.setFormatter(_fmt)
_stream_handler.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────────────────────────────
FILE_PATH      = "booking_dirty.csv"
OUTPUT_DIR     = "data"
BATCH_RATIO    = 0.70
SPLIT_COLUMN   = "booking_date"
BOOKING_ID_COL = "booking_id"

# ─────────────────────────────────────────────────────────────────
# TARIH KURTARMA FONKSIYONLARI
# ─────────────────────────────────────────────────────────────────
def clean_date_string(val):
    """Ham tarih string'ini temizle — ozel karakter, bosluk vb. at."""
    if pd.isna(val) or str(val).strip() == "":
        return None
    s = str(val).strip()
    # Basindaki bozuk karakterleri temizle: !! gibi
    s = s.lstrip("!@#$%^&*~`").strip()
    return s if s else None

def try_parse_date(val):
    """
    Birden fazla format deneyerek tarihi parse et.
    Basarisiz olursa None doner.
    """
    if val is None:
        return None
    # Verimizdeki formatlar — sirayla denenir
    formats = [
        "%m/%d/%Y",   # 5/10/2025   <- ana format (M/D/YYYY)
        "%Y-%m-%d",   # 2023-01-15  <- ISO standart
        "%d/%m/%Y",   # 15/10/2025
        "%d-%m-%Y",   # 15-10-2025
        "%Y%m%d",     # 20250510
        "%b %d, %Y",  # Jan 15, 2025
        "%B %d, %Y",  # January 15, 2025
        "%d.%m.%Y",   # 15.10.2025
    ]
    for fmt in formats:
        try:
            parsed = pd.to_datetime(val, format=fmt, errors="raise")
            # Mantikli tarih araligi kontrolu (2000-2035)
            if 2000 <= parsed.year <= 2035:
                return parsed
        except Exception:
            continue
    return None

def recover_booking_date(row):
    """
    Kurtarma onceligi:
      1. booking_date (temizlenmis)
      2. checkin_date
      3. checkout_date
      4. Kurtarilamaz -> None
    """
    # 1. booking_date temizle ve dene
    parsed = try_parse_date(clean_date_string(row.get(SPLIT_COLUMN)))
    if parsed is not None:
        return parsed, "booking_date_cleaned"

    # 2. checkin_date'den turet
    parsed = try_parse_date(clean_date_string(row.get("checkin_date")))
    if parsed is not None:
        return parsed, "derived_from_checkin"

    # 3. checkout_date'den turet
    parsed = try_parse_date(clean_date_string(row.get("checkout_date")))
    if parsed is not None:
        return parsed, "derived_from_checkout"

    return None, "unrecoverable"

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("HappyBooking -- Veri Bolme Baslatildi")
    log.info(f"Tarih: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # 1. DOSYA KONTROLU
    if not os.path.exists(FILE_PATH):
        log.error(f"Dosya bulunamadi: {FILE_PATH}")
        raise FileNotFoundError(f"'{FILE_PATH}' mevcut degil!")

    # 2. VERIYI YUKLE
    log.info(f"Veri yukleniyor: {FILE_PATH}")
    df = pd.read_csv(FILE_PATH)
    log.info(f"Ham veri: {len(df):,} satir, {len(df.columns)} kolon")
    total_raw = len(df)

    # 3. TARIH KURTARMA
    log.info("Tarih sutunu isleniyor + kurtarma stratejisi uygulanıyor...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Ilk standart parse
    df[SPLIT_COLUMN] = pd.to_datetime(df[SPLIT_COLUMN], errors="coerce")

    mask_invalid = df[SPLIT_COLUMN].isna()
    invalid_df   = df[mask_invalid].copy()
    valid_df     = df[~mask_invalid].copy()

    log.info(f"  Standart parse basarili : {len(valid_df):,} satir")
    log.info(f"  Kurtarma gerekiyor      : {len(invalid_df):,} satir")

    if len(invalid_df) > 0:
        # Kurtarma uygula
        results = invalid_df.apply(recover_booking_date, axis=1)
        invalid_df = invalid_df.copy()
        invalid_df[SPLIT_COLUMN]        = [r[0] for r in results]
        invalid_df["_date_recover_src"] = [r[1] for r in results]

        recovered     = invalid_df[invalid_df[SPLIT_COLUMN].notna()]
        unrecoverable = invalid_df[invalid_df[SPLIT_COLUMN].isna()]

        # Kurtarma istatistikleri
        log.info(f"  Kurtarilan              : {len(recovered):,} satir")
        if len(recovered) > 0:
            src_counts = recovered["_date_recover_src"].value_counts()
            for src, cnt in src_counts.items():
                log.info(f"    [{src}]: {cnt:,} satir")

        log.info(f"  Kurtarilamayan (silindi): {len(unrecoverable):,} satir")

        if len(unrecoverable) > 0:
            unrecoverable.to_csv(f"{OUTPUT_DIR}/truly_invalid_dates.csv", index=False)
            log.info(f"  Kaydedildi: {OUTPUT_DIR}/truly_invalid_dates.csv")

        # Birlestir
        valid_df["_date_recover_src"] = "original"
        df = pd.concat([valid_df, recovered], ignore_index=True)
    else:
        df["_date_recover_src"] = "original"

    log.info(f"Toplam gecerli veri (kurtarma sonrasi): {len(df):,} satir")

    # 4. SIRALAMA — Deterministic
    sort_cols = [SPLIT_COLUMN]
    if BOOKING_ID_COL in df.columns:
        sort_cols.append(BOOKING_ID_COL)
    df = df.sort_values(by=sort_cols).reset_index(drop=True)

    # 5. SPLIT
    split_point = int(len(df) * BATCH_RATIO)
    batch_df    = df.iloc[:split_point].copy()
    stream_df   = df.iloc[split_point:].copy()

    # Tarih cakisma kontrolu
    batch_max  = batch_df[SPLIT_COLUMN].max()
    stream_min = stream_df[SPLIT_COLUMN].min()
    if batch_max > stream_min:
        log.error("Batch ve Stream tarihleri cakisiyor!")
        raise ValueError("Tarih cakismasi tespit edildi.")
    log.info("Tarih cakismasi yok -- chronological split basarili.")

    # 6. KAYDET
    batch_path  = f"{OUTPUT_DIR}/hotel_raw_batch.csv"
    stream_path = f"{OUTPUT_DIR}/hotel_raw_stream.csv"
    batch_df.to_csv(batch_path, index=False)
    stream_df.to_csv(stream_path, index=False)

    # 7. OZET RAPOR
    log.info("")
    log.info("=" * 60)
    log.info("SPLIT OZET RAPORU")
    log.info("=" * 60)
    log.info(f"  Ham veri              : {total_raw:,}")
    log.info(f"  Kurtarma sonrasi      : {len(df):,}")
    log.info(f"  Batch (%70)           : {len(batch_df):,} -> {batch_path}")
    log.info(f"  Stream (%30)          : {len(stream_df):,} -> {stream_path}")
    log.info(f"  Batch tarih araligi   : {batch_df[SPLIT_COLUMN].min().date()} -> {batch_max.date()}")
    log.info(f"  Stream tarih araligi  : {stream_min.date()} -> {stream_df[SPLIT_COLUMN].max().date()}")
    log.info("=" * 60)
    log.info("Islem basariyla tamamlandi!")

if __name__ == "__main__":
    main()