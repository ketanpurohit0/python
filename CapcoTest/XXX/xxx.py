from pathlib import Path
from typing import Tuple, Any

import pandas as pd
import numpy as np


def read_ubo_xlsx(file: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    trans_df = pd.read_excel(
        io=file, sheet_name="TRANS", engine="openpyxl", dtype={"AccountNo": str}
    )
    ubo_alloc = pd.read_excel(
        io=file, sheet_name="UBO_ALLOC", engine="openpyxl", dtype={"ICY_NO": str}
    )
    ubo_with = pd.read_excel(
        io=file, sheet_name="UBO_WITHOLD", engine="openpyxl", dtype={"ICY_NO": str}
    )

    return trans_df.dropna(), ubo_alloc.dropna(), ubo_with.dropna()


def assign_ch3_ch4_as_trans_type(trans_df: pd.DataFrame, ubo_alloc: pd.DataFrame, ubo_with:pd.DataFrame) -> pd.DataFrame:
    """Given 'trans_df:' assign a new column 'TRANS_TYPE' with values 'CH3', 'CH4' or 'ERROR'."""
    trans_df_m1 = (trans_df.merge(ubo_alloc, left_on=['AccountNo'], right_on=['ICY_NO'], suffixes=['','_ALLOC'])
                   .merge(ubo_with, left_on=['AccountNo'], right_on=['ICY_NO'], suffixes=['', '_WITHOLD']))

    mask_in_alloc_range1 = trans_df_m1['FROM_DT'] <= trans_df_m1['EntryDate']
    mask_in_alloc_range2 = trans_df_m1['EntryDate'] <= trans_df_m1['TO_DT']
    trans_df_m1['IN_ALLOC_RANGE'] = mask_in_alloc_range1 & mask_in_alloc_range2

    mask_in_with_range1 = trans_df_m1['FROM_DT_WITHOLD'] <= trans_df_m1['EntryDate']
    mask_in_with_range2 = trans_df_m1['EntryDate'] <= trans_df_m1['TO_DT_WITHOLD']
    trans_df_m1['IN_WITHOLD_RANGE'] = mask_in_with_range1 & mask_in_with_range2



    trans_df_m1 = trans_df_m1.assign(TRANS_TYPE=lambda df: np.where(df.IN_ALLOC_RANGE & df.IN_WITHOLD_RANGE, "CH4", None))
    trans_df_m1 = trans_df_m1.assign(TRANS_TYPE=lambda df: np.where( ~df.IN_ALLOC_RANGE &  ~df.IN_WITHOLD_RANGE, "CH3", df.TRANS_TYPE))
    trans_df_m1['TRANS_TYPE'] =  trans_df_m1['TRANS_TYPE'].replace({None: 'ERROR'})

    columns_to_return = list(trans_df.columns)
    columns_to_return.append("TRANS_TYPE")

    return trans_df_m1[columns_to_return]




if __name__ == "__main__":
    file = Path(r"C:\MyWork\GIT\python\test0\Example_UBO.xlsx")
    trans_df, ubo_alloc, ubo_with = read_ubo_xlsx(file)
    trans_df = assign_ch3_ch4_as_trans_type(trans_df, ubo_alloc, ubo_with)
    pass
