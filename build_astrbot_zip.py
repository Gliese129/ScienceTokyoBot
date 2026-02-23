from __future__ import annotations

import hashlib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

PROJECT_ROOT = Path(__file__).resolve().parent
DIST_DIR = PROJECT_ROOT / "dist"

PLUGIN_ROOT_DIR = "astrbot_plugin_isct_bot"  # must be a directory entry in zip
SAFE_UPLOAD_BASENAME = "astrbot_plugin_isct_bot"  # only letters/digits/underscore for AstrBot upload import path

INCLUDE_PATHS = [
    "main.py",
    "plugin",
    "guards",
    "adapters",
    "runtime",
    "services",
    "isct_core",
    "config",
    "docs",
    "metadata.yaml",
    "_conf_schema.json",
    "README.md",
    "CHANGELOG.md",
]

EXCLUDE_DIR_NAMES = {"__pycache__", ".pytest_cache", ".git", "tests", "dist", "data"}
EXCLUDE_SUFFIXES = {".pyc", ".pyo"}


def should_include(rel_path: Path) -> bool:
    # rel_path is relative to PROJECT_ROOT
    if any(part in EXCLUDE_DIR_NAMES for part in rel_path.parts):
        return False
    if rel_path.suffix in EXCLUDE_SUFFIXES:
        return False
    # Check file existence using absolute path
    return (PROJECT_ROOT / rel_path).is_file()


def collect_files() -> list[Path]:
    files: list[Path] = []
    for item in INCLUDE_PATHS:
        target = PROJECT_ROOT / item
        if not target.exists():
            continue

        if target.is_file():
            rel = target.relative_to(PROJECT_ROOT)
            if should_include(rel):
                files.append(target)
            continue

        for sub in target.rglob("*"):
            rel = sub.relative_to(PROJECT_ROOT)
            if should_include(rel):
                files.append(sub)

    # Keep deterministic order, but directory entry will be written first anyway
    files.sort(key=lambda p: p.relative_to(PROJECT_ROOT).as_posix())
    return files


def build_zip() -> Path:
    output_zip = DIST_DIR / f"{SAFE_UPLOAD_BASENAME}.zip"
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    for old in DIST_DIR.glob("*.zip"):
        old.unlink()
    files = collect_files()

    with ZipFile(output_zip, mode="w", compression=ZIP_DEFLATED) as zf:
        # 1) Ensure the FIRST entry is a directory (critical for AstrBot installer)
        zf.writestr(f"{PLUGIN_ROOT_DIR}/", "")

        # 2) Put everything under that root directory
        for file_path in files:
            rel = file_path.relative_to(PROJECT_ROOT).as_posix()
            arcname = f"{PLUGIN_ROOT_DIR}/{rel}"
            zf.write(file_path, arcname=arcname)

    return output_zip


def verify_zip_content(zip_path: Path) -> tuple[bool, str]:
    with ZipFile(zip_path, mode="r") as zf:
        names = set(zf.namelist())
        main_member = f"{PLUGIN_ROOT_DIR}/main.py"
        main_py = zf.read(main_member).decode("utf-8")
        kv_member = f"{PLUGIN_ROOT_DIR}/runtime/sqlite_runtime.py"
        kv_runtime_py = zf.read(kv_member).decode("utf-8")
        html_member = f"{PLUGIN_ROOT_DIR}/services/html_utils.py"
        html_utils_py = zf.read(html_member).decode("utf-8")
        metadata_member = f"{PLUGIN_ROOT_DIR}/metadata.yaml"
        metadata_yaml = zf.read(metadata_member).decode("utf-8")
    if f"{PLUGIN_ROOT_DIR}/services/exam.py" not in names:
        return False, "zip missing services/exam.py"
    if "storage=self" in main_py:
        return False, "main.py still contains legacy KVRuntime(storage=...) initialization"
    if "get_astrbot_data_path" not in main_py:
        return False, "main.py does not use get_astrbot_data_path for plugin_data path"
    if "runtime.sqlite3" not in main_py:
        return False, "main.py does not initialize SQLite runtime file"
    if "sqlite3.connect" not in kv_runtime_py:
        return False, "runtime/sqlite_runtime.py is not SQLite-backed"
    if "def html_to_text" not in html_utils_py:
        return False, "services/html_utils.py missing html_to_text"
    if "name: astrbot_plugin_isct_bot" not in metadata_yaml:
        return False, "metadata.yaml missing required plugin name"
    return True, "main.py check passed"


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


if __name__ == "__main__":
    output = build_zip()
    ok, message = verify_zip_content(output)
    digest = sha256_of_file(output)
    print(f"Built: {output}")
    print(f"Verify: {message}")
    print(f"SHA256: {digest}")
    if not ok:
        raise SystemExit(2)
