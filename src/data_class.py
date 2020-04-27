
# project libraries
from utils.io import csv_to_pd

class Dataset:

    def __init__(self, data_path:str=None, id_column:str=None):
        """
        Creates a dataset object with attributes:
            df - pd.DataFrame: a dataframe of the input data
            id_columm - str: the name of the id column
        Arguments:
            data_path - str: path to dataset to read
                if None, the attribute is uninitialized
            id_column - str: id column of dataset
        """
        if data_path is not None:
            self.df = csv_to_pd(data_path)
        self.id_column = id_column
    
    def clean_data(self)->None:
        """
        Performs processing steps on the dataframes including:
             - lowering the case of all strings (objects)
             - stripping out whitespace
        Note: the column_id defined in self.id_column is not processed
        Returns:
            None - all actions are done in-place
        """
        for column in self.df:
            if self.df[column].dtype==object:
                # don't alter the column_id's
                if column != self.id_column:
                    # lower the case and strip whitespace
                    self.df[column] = self.df[column].str.lower().str.strip()

    def remove_pattern(self, col_name:str, pattern:str)->None:
        """
        Removes all occurances of the regex pattern in input column name
        """
        assert col_name in self.df, "column not in dataframe"
        assert self.df[col_name].dtype==object, \
            "Series datatype not supported"
        self.df[col_name] = self.df[col_name].str.replace(pattern, '').str.strip()
