#standard libraries
import os
import argparse
from datetime import date
import csv
# third-party libraries
import pandas as pd
import numpy as np
import editdistance
# project libraries
from data_class import Dataset
from utils.io import csv_to_pd, csv_to_dict, write_to_csv

np.set_printoptions(linewidth=300)
pd.options.display.max_rows = 300


def main(dataset_dir:str, output_csv:str):
    # define the data paths
    entso_path = os.path.join(dataset_dir, "entso.csv")
    gppd_path = os.path.join(dataset_dir, "gppd.csv")
    platts_path = os.path.join(dataset_dir, "platts.csv")
    fuel_thes_path = os.path.join(dataset_dir, "fuel_thesaurus.csv")

    # read and process the data
    fuel_dict = csv_to_dict(fuel_thes_path)
    data_tuple = read_process(entso_path, platts_path, gppd_path)
    entso_data, platts_data, gppd_data = data_tuple

    # join the datasets
    joined_data = data_join(entso_data, platts_data, gppd_data, fuel_dict)

    # write the result to csv
    write_columns = ["unit_id_ent", "unit_id_plt", "plant_id_gppd"]
    write_to_csv(joined_data.df, output_csv, write_columns)

def read_process(entso_path:str, platts_path:str, gppd_path:str)->tuple:
    """
    This function reads and proceses the data. 
    Arguments:
        entso_path - str: filepath to the primary dataset (entso)
        platts_path - str: filepath to the first secondary dataset (platts)
        gppd_path - str: filepath to the second secondary dataset (gppd)
    Returns:
        tuple of the processed Dataset objects
    """
    # create the Dataset objects for each set
    entso_data = Dataset(entso_path, id_column="unit_id")
    platts_data = Dataset(platts_path, id_column="unit_id")
    gppd_data = Dataset(gppd_path, id_column="plant_id")

    entso_data.clean_data()
    platts_data.clean_data()
    gppd_data.clean_data()

    # remove the (xx) country abbreviation from country column
    country_pattern = "\([a-z][a-z]\)"
    entso_data.remove_pattern(col_name="country", pattern=country_pattern)
    # removing dashes, underscores, and spaces in unit names
    # this will make calcuting the name distance more accurate
    punc_pattern = "[-_ ]"
    entso_data.remove_pattern(col_name="unit_name", pattern=punc_pattern)
    platts_data.remove_pattern(col_name="UNIT", pattern=punc_pattern)
    gppd_data.remove_pattern(col_name="plant_name", pattern=punc_pattern)

    return entso_data, platts_data, gppd_data


def data_join(entso_data:Dataset, platts_data:Dataset, gppd_data:Dataset, 
    fuel_dict:dict)->Dataset:
    """
    Joins the three datasets. First entso and plats are joined. Then, 
    gppd is joined.
    """
    
    merged_data = merge_entso_platts(entso_data, platts_data, fuel_dict)

    merged_data = merge_gppd(merged_data, gppd_data)

    return merged_data

def merge_entso_platts(entso_data:Dataset, platts_data:Dataset,
    fuel_dict:dict) -> Dataset:
    """
    Entso and platts are joined based on:country and fuel type, 
    and then the plants with the closest names are selected using
    filter_unit_name(). 
    """
    merged_data = Dataset()
    # merge based on country columns
    merged_data.df = entso_data.df.merge(platts_data.df,
        left_on=['country'], right_on=['COUNTRY'], how='left', suffixes=('_ent', '_plt'))

    # creates a new column which maps the entso unit_fuel column
    # to the platts UNIT_FUEL column
    merged_data.df["entso_unit_fuel_map"] = merged_data.df.\
        apply(lambda row: fuel_dict[row.unit_fuel], axis=1)
    # selects rows where entso-mapped fuel column equals the UNIT_FUEL in platts
    merged_data.df = merged_data.df.query('UNIT_FUEL==entso_unit_fuel_map')

    # select plant names that are similar
    merged_data.df = filter_unit_name(merged_data.df)

    return merged_data


def filter_unit_name(merged_df:pd.DataFrame)->pd.DataFrame:
    """
    Iteratively selects names that are close together based
    on the Levenstein distance (number of added/inserted/
    removed letters of make two strings identical). 

    TODO: this iterative approach is very inefficient and would
    not scale. Future work would speed up this algorithm. 

    TODO: the max_dist parameter has been manually tuned to be 6. 
    In future, some thought would be put into how to calculate this 
    programmatically.
    """
    # accepted string distance between names
    max_dist = 6
    filter_df = pd.DataFrame(columns=merged_df.columns)
    for dist in range(max_dist):
        for index, row in merged_df.iterrows():
            # this checks of the unit_name is already in
            # filtered_df, if so skip the row
            # unit_name_entso is row index 4 
            if not any(filter_df.unit_name.isin([row[4]])):
                # UNIT_platts is index 10
                if editdistance.eval(row[4], row[10]) < dist:
                    filter_df = filter_df.append(row)

    return filter_df


def merge_gppd(merged_data:Dataset, gppd_data:Dataset)->Dataset:
    """
    This function joins the gppd dataset to the entso-platts dataset on 
    country and fuel type, and then rows are selected where wepp_id and 
    platts plant_id are equal.
    
    TODO: use filter_unit_name to select duplicated wepp_id - plant_id pairs
    """
    merged_data.df = merged_data.df.merge(gppd_data.df,
        left_on=['country'], right_on=['country_long'], how='left', suffixes=('', '_gppd'))
   
    merged_data.df = merged_data.df.query('plant_primary_fuel==entso_unit_fuel_map')
    
    # cast the plant_id to int and then to string
    merged_data.df['plant_id'] = merged_data.df['plant_id'].astype("int64").apply(str)
    # the above type cast is necessary because plant_id is a string
    merged_data.df = merged_data.df.query('wepp_id==plant_id')

    return merged_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Maps three datasets (entso, platts, gppd) in dataset_dir \
        to a single dataset which is written out output_csv"
    )
    parser.add_argument("--dataset-dir", type=str, default="../data/",
        help="The directory where the 3 datasets are stored.")
    
    today = str(date.today())
    parser.add_argument("--output-csv", type=str, default=f"../result/{today}_mapping_drz.csv",
        help="The path to where the result will be written.")

    args = parser.parse_args()

    # if directory of output-csv doesn't exist, create it
    if not os.path.exists(os.path.dirname(args.output_csv)):
        os.mkdir(os.path.dirname(args.output_csv))

    main(args.dataset_dir, args.output_csv)
