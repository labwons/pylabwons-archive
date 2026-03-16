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
if any([val.lower().startswith('hkefico') for val in os.environ.values()]):
    HOST = 'hkefico'

RUNTIME = os.environ.get("GITHUB_EVENT_NAME", "*")

class PATH:
    ROOT = _get_root()
    DATA = ROOT / "data"
    PARQUET = DataDictionary(
        MARKET = DATA / "src/market.parquet",
        BASELINE = DATA / "src/baseline.parquet",
        SECTOR = DATA / "src/sector.parquet",
        NUMBER = DATA / "src/number.parquet",
    )
    OHLCV = DATA / "src/ohlcv"
    LOG = DATA / "src/log"
    CSV = DataDictionary(
        BASELINE = DATA / "src/baseline.csv"
    )
    JSON = DataDictionary(
        BUILD = DATA / "log/build.json"
    )
    HTML = DataDictionary(
        MARKETMAP = DATA / "src/html/marketmap.html",
        TEMPLATE = DATA / "src/html/template",
    )


    if HOST in ['hkefico', 'local']:
        DOWNLOADS = Path(os.getenv('USERPROFILE')) / 'Downloads'
    else:
        DOWNLOADS = Path(os.getcwd())


if __name__ == "__main__":
    print(PATH.ROOT)
    print(PATH.DATA)
    print(PATH.PARQUET)