from pathlib import Path

def export_artifacts(db_path: str, export_dir: str) -> dict:
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    return {"export_dir": export_dir, "db_path": db_path}
