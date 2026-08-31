# ============================================================
# FACTIVA PIPELINE — LIVE CMD OUTPUT VERSION
#
# Features:
#   0) Ensure query_regrex_all.csv exists in the working folder
#   1) Execute revised 01_FPU..._rev*.ipynb
#   2) Execute revised 02_Clean..._rev*.ipynb only if Step 1 succeeds
#   3) Stream notebook stdout/stderr to CMD in real time
#   4) Save executed outputs back into each notebook
#
# Put this file in the SAME folder as the two notebooks.
#
# Run:
#   python run_all_live.py
# ============================================================

from pathlib import Path
import asyncio
import shutil
import sys
import time
import traceback

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError


# ------------------------------------------------------------
# 1. WINDOWS EVENT LOOP
# ------------------------------------------------------------

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except AttributeError:
        pass


# ------------------------------------------------------------
# 2. SETTINGS
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

NOTEBOOK_1_FILENAME = "fpu_factiva.ipynb"
NOTEBOOK_2_FILENAME = "fpu_clean.ipynb"

REQUIRED_REGEX_FILENAME = "query_regrex_all.csv"
TARGET_REGEX_PATH = BASE_DIR / REQUIRED_REGEX_FILENAME


# ------------------------------------------------------------
# 3. ENSURE REQUIRED CSV EXISTS
# ------------------------------------------------------------

def ensure_query_regex_file() -> Path:
    print("\n" + "=" * 78, flush=True)
    print("CHECK REQUIRED FILE — query_regrex_all.csv", flush=True)
    print("=" * 78, flush=True)

    if TARGET_REGEX_PATH.exists():
        print("Found in working folder:", flush=True)
        print(f"  {TARGET_REGEX_PATH}", flush=True)
        return TARGET_REGEX_PATH

    print("Not found in working folder.", flush=True)
    print("Searching recursively under:", flush=True)
    print(f"  {BASE_DIR}", flush=True)

    candidates = [
        p for p in BASE_DIR.rglob(REQUIRED_REGEX_FILENAME)
        if p.resolve() != TARGET_REGEX_PATH.resolve()
    ]

    if not candidates:
        raise FileNotFoundError(
            f"Could not find '{REQUIRED_REGEX_FILENAME}' anywhere under:\n"
            f"{BASE_DIR}"
        )

    def rank_candidate(path: Path):
        try:
            depth = len(path.relative_to(BASE_DIR).parts)
        except ValueError:
            depth = 999

        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0

        return (depth, -mtime, str(path).lower())

    candidates = sorted(candidates, key=rank_candidate)

    print(f"\nFound {len(candidates)} candidate(s):", flush=True)
    for i, candidate in enumerate(candidates, start=1):
        print(f"  {i}. {candidate}", flush=True)

    selected = candidates[0]

    print("\nSelected source:", flush=True)
    print(f"  {selected}", flush=True)

    shutil.copy2(selected, TARGET_REGEX_PATH)

    print("\nCopied to:", flush=True)
    print(f"  {TARGET_REGEX_PATH}", flush=True)

    return TARGET_REGEX_PATH


# ------------------------------------------------------------
# 4. GET EXACT NOTEBOOK
# ------------------------------------------------------------

def get_notebook_exact(filename: str) -> Path:
    path = BASE_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Required notebook not found:\n  {path}"
        )
    return path


# ------------------------------------------------------------
# 5. LIVE-STREAMING NOTEBOOK CLIENT
# ------------------------------------------------------------

class LiveNotebookClient(NotebookClient):
    """
    NotebookClient that mirrors kernel stream/error messages to the
    terminal while still storing normal outputs in the notebook.
    """

    def process_message(self, msg, cell, cell_index):
        msg_type = msg.get("msg_type")
        content = msg.get("content", {})

        # Normal print(...) / stdout / stderr
        if msg_type == "stream":
            text = content.get("text", "")
            if text:
                print(text, end="", flush=True)

        # Python traceback from a failed cell
        elif msg_type == "error":
            traceback_lines = content.get("traceback", [])
            if traceback_lines:
                print("\n".join(traceback_lines), flush=True)

        # Useful plain-text display output
        elif msg_type in {"execute_result", "display_data"}:
            data = content.get("data", {})
            plain = data.get("text/plain")
            if plain:
                # Avoid dumping giant tables/objects into CMD.
                plain = str(plain)
                if len(plain) <= 3000:
                    print(plain, flush=True)
                else:
                    print(
                        plain[:3000] + "\n...[display output truncated in CMD]...",
                        flush=True,
                    )

        return super().process_message(msg, cell, cell_index)


# ------------------------------------------------------------
# 6. EXECUTE NOTEBOOK WITH LIVE OUTPUT
# ------------------------------------------------------------

def run_notebook_live(notebook_path: Path, step_name: str) -> None:
    print("\n" + "=" * 78, flush=True)
    print(step_name, flush=True)
    print("=" * 78, flush=True)
    print(f"Notebook : {notebook_path.name}", flush=True)
    print(f"Folder   : {notebook_path.parent}", flush=True)
    print("Starting notebook execution...\n", flush=True)

    with notebook_path.open("r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    kernel_name = (
        nb.metadata.get("kernelspec", {}).get("name")
        or "python3"
    )

    print(f"Kernel   : {kernel_name}", flush=True)
    print("-" * 78, flush=True)

    client = LiveNotebookClient(
        nb,
        timeout=None,               # no per-cell timeout
        kernel_name=kernel_name,
        resources={"metadata": {"path": str(BASE_DIR)}},
        allow_errors=False,
    )

    try:
        client.execute()
    except Exception:
        # Save all outputs generated before the failure.
        with notebook_path.open("w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        print("\n" + "!" * 78, flush=True)
        print(f"NOTEBOOK FAILED: {notebook_path.name}", flush=True)
        print("Outputs up to the failed cell were saved back to the notebook.", flush=True)
        print("!" * 78, flush=True)
        raise

    # Save executed notebook in place.
    with notebook_path.open("w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    print("-" * 78, flush=True)
    print(f"Completed successfully: {notebook_path.name}", flush=True)


# ------------------------------------------------------------
# 7. MAIN PIPELINE
# ------------------------------------------------------------

def main():
    print("=" * 78, flush=True)
    print("FACTIVA PIPELINE — REVISED NOTEBOOKS — LIVE OUTPUT", flush=True)
    print("=" * 78, flush=True)
    print(f"Working folder: {BASE_DIR}", flush=True)
    print("", flush=True)
    print("Execution order:", flush=True)
    print("  Step 0: Ensure query_regrex_all.csv", flush=True)
    print("  Step 1: 3STAGE Factiva API / Master Output (cache-resume enabled)", flush=True)
    print("  Step 2: Revised Clean GFPU Master Dataset", flush=True)
    print("", flush=True)
    print("Notebook print() output will appear in this CMD window.", flush=True)
    print("Step 2 runs only if Step 1 succeeds.", flush=True)

    start = time.time()

    try:
        regex_file = ensure_query_regex_file()

        print("\nRequired CSV ready:", flush=True)
        print(f"  {regex_file}", flush=True)

        notebook_1 = get_notebook_exact(NOTEBOOK_1_FILENAME)
        notebook_2 = get_notebook_exact(NOTEBOOK_2_FILENAME)

        print("\nNotebooks selected:", flush=True)
        print(f"  1. {notebook_1.name}", flush=True)
        print(f"  2. {notebook_2.name}", flush=True)

        # Step 1
        run_notebook_live(
            notebook_1,
            "STEP 1/2 — REVISED FACTIVA API / MASTER OUTPUT",
        )

        print("\nStep 1 finished successfully.", flush=True)
        print("Proceeding to Step 2...", flush=True)

        # Step 2
        run_notebook_live(
            notebook_2,
            "STEP 2/2 — REVISED CLEAN GFPU MASTER DATASET",
        )

    except KeyboardInterrupt:
        print("\n\nPIPELINE INTERRUPTED BY USER.", flush=True)
        sys.exit(130)

    except Exception as exc:
        print("\n" + "=" * 78, flush=True)
        print("PIPELINE STOPPED", flush=True)
        print("=" * 78, flush=True)
        print(f"{type(exc).__name__}: {exc}", flush=True)
        sys.exit(1)

    elapsed = time.time() - start

    print("\n" + "=" * 78, flush=True)
    print("PIPELINE COMPLETED SUCCESSFULLY", flush=True)
    print("=" * 78, flush=True)
    print("Step 0: required CSV ready", flush=True)
    print("Step 1: completed", flush=True)
    print("Step 2: completed", flush=True)
    print(f"Total elapsed time: {elapsed / 60:.1f} minutes", flush=True)


if __name__ == "__main__":
    main()
