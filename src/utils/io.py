# standard libraries
import csv
# third-party libraries
import pandas as pd


def csv_to_pd(csv_path:str)->pd.DataFrame:
    """
    reads a csv using the pd.read_csv method
    """
    return pd.read_csv(csv_path, sep=',', encoding="utf-8",)

def csv_to_dict(csv_path:str)->dict:
    """
    reads a csv and returns a dict of processed values
    where the first column is the key and the second column
    is the value
    """
    with open(csv_path, 'r') as fid:
        reader = csv.reader(fid, delimiter=',')
        header = next(reader)
        csv_dict = dict()
        for line in reader:
            key = line[0].lower().strip()
            value = line[1].lower().strip()
            csv_dict.update({key:value})
        return csv_dict




def write_csv(df:pd.DataFrame, output_csv:str)->None:
    raise NotImplementedError