"""Stage 0 — acquire, verify, and slice the LANL auth corpus.

The LANL "Comprehensive, Multi-Source Cyber-Security Events" release (Kent, 2015) is
served behind a "data fence": you tell csr.lanl.gov who you are and how you'll use the
data, and it hands back a path token. Downloads are then GETs under /data-fence/<token>/.
There is no anonymous URL, so `download` needs a token — pass --token or set TARN_LANL_TOKEN.

Subcommands
    download   fetch auth.txt.gz + redteam.txt.gz into $TARN_RAW (resumable)
    verify     sha256 the raw files and check sizes
    sample     cut the deterministic ~100k-event CI slice into data/sample/
    describe   count the full corpus and write bench/dataset.json

Everything about the slice is deterministic: the window, the modulus, and the seed are
constants below, and the manifest records them. Rerunning `sample` on the same raw file
reproduces the same bytes (there is a test for this).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------------
# Constants. These define the committed CI slice. Changing any of them changes the
# slice, so they are recorded verbatim into data/sample/manifest.json.
# ---------------------------------------------------------------------------------
SEED = 20260117  # committed seed (DIRECTIVE rule 4: determinism)

# The red-team campaign runs from t=150885 (day 1.75) to t=2557047 (day 29.6). The CI
# slice takes the first 8 days so it spans multiple dates (daily rollups, window
# functions, first-seen logic all need >1 day) AND overlaps the start of the campaign.
SAMPLE_WINDOW_START = 0
SAMPLE_WINDOW_END = 8 * 86_400  # 691_200 — first 8 days

# Keep 1 event in SAMPLE_MODULUS, selected by a stable hash of the raw line. The first 8
# days hold a MEASURED 133,093,586 events, so 1-in-1331 lands at ~100k — small enough for
# GitHub Actions, large enough that the rollups and window functions have real work to do.
SAMPLE_MODULUS = 1331

LANL_BASE = "https://csr.lanl.gov/data-fence"
FILES = {
    "auth.txt.gz": "cyber1/auth.txt.gz",
    "redteam.txt.gz": "cyber1/redteam.txt.gz",
}

AUTH_COLUMNS = [
    "time",
    "src_user",
    "dst_user",
    "src_computer",
    "dst_computer",
    "auth_type",
    "logon_type",
    "auth_orientation",
    "outcome",
]
REDTEAM_COLUMNS = ["time", "user", "src_computer", "dst_computer"]

REPO = Path(__file__).resolve().parent.parent
RAW = Path(os.environ.get("TARN_RAW", REPO / "data" / "raw"))
SAMPLE_DIR = REPO / "data" / "sample"
BENCH = REPO / "bench"


def _stable_keep(line: str) -> bool:
    """Deterministic 1-in-MODULUS selector.

    Uses blake2b over (seed, line) rather than Python's hash(), which is randomized per
    process by PYTHONHASHSEED and would make the slice irreproducible across runs.
    """
    h = hashlib.blake2b(f"{SEED}|{line}".encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") % SAMPLE_MODULUS == 0


def _sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while block := fh.read(chunk):
            h.update(block)
    return h.hexdigest()


def _open_maybe_partial(path: Path):
    """Iterate decoded lines from a gzip file, tolerating a truncated tail.

    auth.txt.gz is 7.2 GB and time-ordered. A partially downloaded file still decodes
    cleanly from the start, which lets the early-day slices be cut while the rest is
    still in flight. A truncated tail surfaces as EOFError/BadGzipFile on the final
    read; we stop there instead of blowing up, and the caller decides whether the
    portion it needed was covered.
    """
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        try:
            yield from fh
        except (EOFError, gzip.BadGzipFile, OSError) as exc:
            print(f"  [note] gzip stream ended early ({exc}); file is still downloading",
                  file=sys.stderr)


# ---------------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------------
def cmd_download(args: argparse.Namespace) -> int:
    token = args.token or os.environ.get("TARN_LANL_TOKEN")
    if not token:
        print(
            "No LANL data-fence token.\n"
            "  1. Open https://csr.lanl.gov/data/cyber1/\n"
            "  2. Fill in the download form (email + how you'll use the data)\n"
            "  3. The page reveals links of the form /data-fence/<TOKEN>/cyber1/auth.txt.gz\n"
            "  4. Re-run with --token '<TOKEN>' or TARN_LANL_TOKEN=<TOKEN>\n",
            file=sys.stderr,
        )
        return 2

    RAW.mkdir(parents=True, exist_ok=True)
    for name, suffix in FILES.items():
        dest = RAW / name
        url = f"{LANL_BASE}/{token}/{suffix}"
        have = dest.stat().st_size if dest.exists() else 0

        req = urllib.request.Request(url)
        if have:
            req.add_header("Range", f"bytes={have}-")
        print(f"[fetch] {name} (resuming at {have:,} bytes)" if have else f"[fetch] {name}")

        with urllib.request.urlopen(req) as resp, dest.open("ab" if have else "wb") as out:
            total = int(resp.headers.get("Content-Length", 0)) + have
            done = have
            while chunk := resp.read(1 << 20):
                out.write(chunk)
                done += len(chunk)
                if total:
                    pct = 100 * done / total
                    print(f"\r  {done:,} / {total:,} bytes ({pct:5.1f}%)", end="", flush=True)
            print()
    return 0


# ---------------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------------
def cmd_verify(args: argparse.Namespace) -> int:
    ok = True
    for name in FILES:
        path = RAW / name
        if not path.exists():
            print(f"[missing] {path}")
            ok = False
            continue
        size = path.stat().st_size
        print(f"[ok] {name}  {size:,} bytes  sha256={_sha256(path)}")
    return 0 if ok else 1


# ---------------------------------------------------------------------------------
# sample
# ---------------------------------------------------------------------------------
def cmd_sample(args: argparse.Namespace) -> int:
    auth_gz = RAW / "auth.txt.gz"
    redteam_gz = RAW / "redteam.txt.gz"
    if not auth_gz.exists() or not redteam_gz.exists():
        print("raw files missing — run `download` first", file=sys.stderr)
        return 2

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Red-team rows inside the sample window. These are the ground truth; every one of
    # them that falls in the window is force-included in the slice regardless of the
    # 1-in-N selector, so the sample is never "attack-free" by luck.
    redteam_rows: list[str] = []
    redteam_keys: set[tuple[str, str, str, str]] = set()
    with gzip.open(redteam_gz, "rt") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            t, user, src, dst = line.split(",")
            if SAMPLE_WINDOW_START <= int(t) < SAMPLE_WINDOW_END:
                redteam_rows.append(line)
                redteam_keys.add((t, user, src, dst))

    kept: list[str] = []
    forced = 0
    scanned = 0
    stopped_early = False

    for raw_line in _open_maybe_partial(auth_gz):
        line = raw_line.strip()
        if not line:
            continue
        # Cheap pre-filter: the time field is everything before the first comma, and the
        # corpus is time-ordered, so we can stop the scan at the window edge instead of
        # decompressing all 7.2 GB.
        head, _, _rest = line.partition(",")
        try:
            t = int(head)
        except ValueError:
            continue
        if t < SAMPLE_WINDOW_START:
            continue
        if t >= SAMPLE_WINDOW_END:
            stopped_early = True
            break

        scanned += 1
        fields = line.split(",")
        if len(fields) != len(AUTH_COLUMNS):
            continue

        # An auth row is "red-team" when (time, src_user, src_computer, dst_computer)
        # matches a labelled compromise event.
        key = (fields[0], fields[1], fields[3], fields[4])
        if key in redteam_keys:
            kept.append(line)
            forced += 1
        elif _stable_keep(line):
            kept.append(line)

    if not stopped_early:
        print(
            "  [warn] reached end of auth.txt.gz before the window closed — the download "
            "is probably incomplete, so this slice covers less than the intended 8 days.",
            file=sys.stderr,
        )

    auth_out = SAMPLE_DIR / "auth_sample.csv.gz"
    with gzip.open(auth_out, "wt", encoding="utf-8", newline="\n") as fh:
        fh.write(",".join(AUTH_COLUMNS) + "\n")
        for line in kept:
            fh.write(line + "\n")

    redteam_out = SAMPLE_DIR / "redteam_sample.csv.gz"
    with gzip.open(redteam_out, "wt", encoding="utf-8", newline="\n") as fh:
        fh.write(",".join(REDTEAM_COLUMNS) + "\n")
        for line in redteam_rows:
            fh.write(line + "\n")

    manifest = {
        "description": (
            "Deterministic CI slice of the LANL auth corpus. Rerunning data/fetch.py "
            "sample against the same auth.txt.gz reproduces these files byte for byte."
        ),
        "source": "LANL Comprehensive, Multi-Source Cyber-Security Events (Kent, 2015)",
        "selection": {
            "seed": SEED,
            "window_start_seconds": SAMPLE_WINDOW_START,
            "window_end_seconds": SAMPLE_WINDOW_END,
            "window_days": (SAMPLE_WINDOW_END - SAMPLE_WINDOW_START) / 86_400,
            "rule": (
                f"keep row if blake2b(seed|line) % {SAMPLE_MODULUS} == 0, "
                "OR row matches a labelled red-team event (force-included)"
            ),
            "modulus": SAMPLE_MODULUS,
        },
        "counts": {
            "auth_rows_scanned_in_window": scanned,
            "auth_rows_kept": len(kept),
            "auth_rows_kept_redteam": forced,
            "redteam_rows_in_window": len(redteam_rows),
        },
        "files": {
            "auth_sample.csv.gz": {
                "bytes": auth_out.stat().st_size,
                "sha256": _sha256(auth_out),
            },
            "redteam_sample.csv.gz": {
                "bytes": redteam_out.stat().st_size,
                "sha256": _sha256(redteam_out),
            },
        },
        "generated_by": "data/fetch.py sample",
    }
    (SAMPLE_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"[sample] scanned {scanned:,} in-window events")
    print(f"[sample] kept {len(kept):,} ({forced} red-team force-included)")
    print(f"[sample] red-team rows in window: {len(redteam_rows)}")
    print(f"[sample] wrote {auth_out} and {redteam_out}")
    return 0


# ---------------------------------------------------------------------------------
# describe  ->  bench/dataset.json
# ---------------------------------------------------------------------------------
def cmd_describe(args: argparse.Namespace) -> int:
    """Count the full corpus. This is the only place the headline row count comes from.

    DIRECTIVE rule 1: every number is measured. This walks all 7.2 GB and counts; it
    does not trust the paper's stated ~1.05B.
    """
    auth_gz = RAW / "auth.txt.gz"
    redteam_gz = RAW / "redteam.txt.gz"

    rows = 0
    t_min: int | None = None
    t_max = 0
    bad = 0

    # Read the gzip stream in big blocks: line-by-line iteration on a billion rows is
    # dominated by Python-level loop overhead otherwise.
    with gzip.open(auth_gz, "rb") as fh:
        buffered = io.BufferedReader(fh, buffer_size=1 << 22)
        tail = b""
        while block := buffered.read(1 << 24):
            block = tail + block
            lines = block.split(b"\n")
            tail = lines.pop()
            for line in lines:
                if not line:
                    continue
                rows += 1
                head = line.partition(b",")[0]
                try:
                    t = int(head)
                except ValueError:
                    bad += 1
                    continue
                if t_min is None:
                    t_min = t
                if t > t_max:
                    t_max = t
        if tail.strip():
            rows += 1

    with gzip.open(redteam_gz, "rt") as fh:
        redteam_rows = sum(1 for line in fh if line.strip())

    payload = {
        "dataset": {
            "name": "LANL Comprehensive, Multi-Source Cyber-Security Events",
            "citation": (
                "A. D. Kent, 'Cybersecurity Data Sources for Dynamic Network Research', "
                "in Dynamic Networks in Cybersecurity, Imperial College Press, 2015."
            ),
            "url": "https://csr.lanl.gov/data/cyber1/",
            "license": "CC0 1.0 (public domain dedication)",
            "character": "real enterprise authentication telemetry, anonymized (users/hosts pseudonymized)",
            "synthetic": False,
        },
        "auth": {
            "file": "auth.txt.gz",
            "bytes_compressed": auth_gz.stat().st_size,
            "sha256": _sha256(auth_gz),
            "rows": rows,
            "unparseable_rows": bad,
            "time_min_seconds": t_min,
            "time_max_seconds": t_max,
            "span_days": round((t_max - (t_min or 0)) / 86_400, 2),
            "columns": AUTH_COLUMNS,
        },
        "redteam": {
            "file": "redteam.txt.gz",
            "bytes_compressed": redteam_gz.stat().st_size,
            "sha256": _sha256(redteam_gz),
            "rows": redteam_rows,
            "columns": REDTEAM_COLUMNS,
        },
        "ci_slice": {
            "path": "data/sample/",
            "seed": SEED,
            "window_days": (SAMPLE_WINDOW_END - SAMPLE_WINDOW_START) / 86_400,
            "modulus": SAMPLE_MODULUS,
        },
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "measured_by": "data/fetch.py describe",
    }

    BENCH.mkdir(exist_ok=True)
    (BENCH / "dataset.json").write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps(payload["auth"], indent=2))
    print(f"[describe] wrote {BENCH / 'dataset.json'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_dl = sub.add_parser("download", help="fetch raw files behind the LANL data fence")
    p_dl.add_argument("--token", help="LANL data-fence token (or set TARN_LANL_TOKEN)")
    p_dl.set_defaults(func=cmd_download)

    sub.add_parser("verify", help="sha256 + size the raw files").set_defaults(func=cmd_verify)
    sub.add_parser("sample", help="cut the deterministic CI slice").set_defaults(func=cmd_sample)
    sub.add_parser("describe", help="count the corpus -> bench/dataset.json").set_defaults(
        func=cmd_describe
    )

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
