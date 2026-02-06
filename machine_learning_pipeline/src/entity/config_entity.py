from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: Path
    source_URL: str  # Could be DB connection string or URL
    local_data_file: Path
    unzip_dir: Path
