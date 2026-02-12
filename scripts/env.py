from pylabwons import DataDictionary
from pathlib import Path
import os


def _get_root() -> Path:
    """
    프로젝트 루트를 자동으로 찾는다.
    - PyCharm / VSCode / Colab / GitHub Actions 대응
    - markers 중 하나라도 있으면 루트로 판단
    """
    markers = (".git", ".gitignore", "pyproject.toml", "setup.py")
    try:
        start_path = Path(__file__).resolve()
    except NameError:
        start_path = Path.cwd()

    current = start_path if start_path.is_dir() else start_path.parent

    for parent in [current, *current.parents]:
        if any((parent / m).exists() for m in markers):
            return parent

    raise RuntimeError("프로젝트 루트를 찾을 수 없습니다.")


HOST = "local"
if any([key.lower().startswith('colab') for key in os.environ]):
    HOST = 'google_colab'
if any([key.lower().startswith('github') for key in os.environ]):
    HOST = 'github_action'

RUNTIME = ''

class PATH:
    ROOT = _get_root()
    DATA = ROOT / "data"
    PARQ = DataDictionary(
        AFTERMARKET = DATA / "src/aftermarket.parquet",
        WICS = DATA / "src/wics.parquet",
        FUNDAMENTALS = DATA / "src/fundamentals.parquet",
    )


if __name__ == "__main__":
    print(ROOT)
    print(DATA)
    print(PARQ)