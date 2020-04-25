
# third-party libraries
import pandas as pd


def clean_data(df_list:list, id_columns:list)->list:
    """
    Performs a common set of processing steps on a list of dataframes including:
        lowering the case of all strings
    Arguments:
        df_list - list(pd.DataFrame): list of dataframes
        id_columns - list(str): the names of the column ids that won't be processed
    Returns:
        df_list - list(pd.DataFrame): processed list of dataframes
    """
    for df in df_list:
        assert type(df)==pd.DataFrame
        # lower the case on all object columns
        for column in df:
            if df[column].dtype==object:
                # don't alter the column id's
                if column not in id_columns:
                    # lower the case and strip whitespace
                    df[column] = df[column].str.lower().str.strip()

    return df_list


def remove_pattern(series:pd.Series, pattern:str)->pd.Series:
    """
    Removes all occurances of the regex pattern in the input series
    Arguments:
        series - pd.Series
        pattern - str: regex pattern that specifies the text to be removed
    Returns:
        series - pd.Series: series with all matches to pattern removed
    """
    assert type(series)==pd.Series
    return series.str.replace(pattern, '').str.strip()