from pandas import DataFrame
from pathlib import Path
from typing import Any, List, Union
import pandas as pd
import requests, io, urllib3

# InsecureRequestWarning 경고 무시 설정
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DataFrameHeir(DataFrame):

    @property
    def _constructor(self):
        return DataFrameHeir

    def __init__(self, *args, **kwargs):
        # Type Check
        # Can only pass [str, Path, DataFrame]
        # for [str, Path] case, it parses the source into DataFrame, and then insert to
        # source list: _src
        _src:List[Union[Any, DataFrame]] = []
        for n, arg in enumerate(args):

            if isinstance(arg, DataFrame):
                _src.append(arg)
                continue

            if isinstance(arg, Path):
                arg = str(arg)

            if isinstance(arg, str):
                if arg.startswith('http'):
                    resp = requests.get(arg, verify=False)
                    if not resp.status_code == 200:
                        raise ConnectionError(f'Failed to fetch: {arg}')

                    if arg.endswith('.csv'):
                        _src.append(self._from_file(arg, raw=io.StringIO(resp.content), **kwargs))
                        continue
                    if arg.endswith('.pkl') or arg.endswith('.parquet'):
                        _src.append(self._from_file(arg, raw=io.BytesIO(resp.content), **kwargs))
                        continue
                _src.append(self._from_file(arg, **kwargs))
                continue

            raise TypeError(f'Unknown type: {n} / {type(arg)}')

        if len(_src) == 1:
            super().__init__(_src[0])
            return
        super().__init__(self._merge_dataframes(*_src, **kwargs))
        return

    @classmethod
    def _from_file(cls, file:str, raw=None, **kwargs) -> DataFrame:
        if file.endswith('.csv'):
            return pd.read_csv(raw if raw else file, encoding=kwargs.get('encoding', 'utf-8'))
        elif file.endswith('.parquet'):
            return pd.read_parquet(raw if raw else file, engine=kwargs.get('engine', 'pyarrow'))
        elif file.endswith('.pkl'):
            return pd.read_pickle(raw if raw else file)
        else:
            raise TypeError(f'Unknown file format: ".{file.split(".")[-1]}"')

    @classmethod
    def _merge_dataframes(cls, *args, **kwargs) -> DataFrame:
        base = args[0]
        objs = []
        for _df in args[1:]:
            _df = _df[list(set(_df.columns) - set(base.columns))]
            objs.append(_df)
            base = base.join(_df, how='left')

        method = kwargs.get('method', 'concat')
        if method == 'concat':
            return pd.concat(
                [base] + objs,
                axis=kwargs.get('axis', 1),
                ignore_index=kwargs.get('ignore_index', False)
            )
        if method == 'join':
            return base
        raise KeyError(f'Unknown method: "{method}"')
