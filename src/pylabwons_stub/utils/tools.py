from typing import Union
import numpy as np
import pandas as pd


def int2krw(krw: Union[int, float], limit:str='억') -> Union[str, float]:
    """
    KRW (원화) 입력 시 화폐 표기 법으로 변환(자동 계산)
    @krw 단위는 원 일 것
    """
    if pd.isna(krw) or np.isnan(krw):
        return np.nan
    if krw >= 1e+12:
        krw /= 1e+8
        currency = f'{int(krw // 10000)}조'
        if int(krw % 10000):
            currency += f' {int(krw % 10000)}억'
        return currency
    if krw >= 1e+8:
        krw /= 1e+4
        currency = f'{int(krw // 10000)}억'
        if limit == '억':
            return currency
        if int(krw % 10000):
            currency += f' {int(krw % 10000)}만'
        return currency
    return f'{int(krw // 10000)}만'