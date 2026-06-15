"""
Download 10,000 English WAV files from HuggingFace (LibriSpeech clean-360)
into ./testdata for checksum generation testing.
Uses decode=False to avoid requiring torchcodec; soundfile reads the raw FLAC bytes.
"""

import io
from pathlib import Path

import soundfile as sf
from datasets import load_dataset

TESTDATA = Path("testdata")
TARGET = 10_000
DATASET = "openslr/librispeech_asr"
SPLIT = "train.360"

TESTDATA.mkdir(exist_ok=True)

existing = len(list(TESTDATA.glob("*.wav")))
if existing >= TARGET:
    print(f"Already have {existing} WAV files in {TESTDATA}, nothing to do.")
    raise SystemExit(0)

print(f"Streaming {DATASET} ({SPLIT}), saving {TARGET} WAVs to {TESTDATA}/")

ds = load_dataset(
    DATASET, "clean", split=SPLIT, streaming=True, trust_remote_code=False
).cast_column("audio", __import__("datasets").Audio(decode=False))

count = existing
for sample in ds:
    if count >= TARGET:
        break

    audio = sample["audio"]
    raw: bytes = audio.get("bytes") or b""
    path_hint: str = audio.get("path", "")

    if not raw and path_hint:
        # Some shards store a local path instead of bytes; skip gracefully
        count += 1
        continue

    speaker = sample.get("speaker_id", count)
    chapter = sample.get("chapter_id", 0)
    uid = sample.get("id", f"{count:06d}")
    fname = TESTDATA / f"{speaker}_{chapter}_{uid}.wav"

    if fname.exists():
        count += 1
        continue

    array, sr = sf.read(io.BytesIO(raw))
    sf.write(str(fname), array, sr, subtype="PCM_16")
    count += 1

    if count % 500 == 0:
        print(f"  {count}/{TARGET} files written...")

print(f"Done. {count} WAV files in {TESTDATA}/")
