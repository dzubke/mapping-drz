#standard libraries
import os
import argparse
from datetime import date
import csv
# third-party libraries
import pandas as pd
import numpy as np
# project libraries
from utils.preprocess import clean_data, remove_pattern
from utils.io import csv_to_pd, csv_to_dict, write_csv

np.set_printoptions(linewidth=300)
pd.options.display.max_rows = 300


def main(dataset_dir:str, output_csv:str):
    entso_path = os.path.join(dataset_dir, "entso.csv")
    gppd_path = os.path.join(dataset_dir, "gppd.csv")
    platts_path = os.path.join(dataset_dir, "platts.csv")
    fuel_thes_path = os.path.join(dataset_dir, "fuel_thesaurus.csv")

    df_tuple = read_process(entso_path, platts_path, gppd_path)
    fuel_dict = csv_to_dict(fuel_thes_path)

    joined_df = df_join(*df_tuple, fuel_dict)
    #write_csv(joined_df, output_csv)

def read_process(prim_data_path:str, sec_data_1:str, sec_data_2:str)->tuple:
    """
    This function performs a mapping of the entries of the datasets in sec_data_1
    and sec_data_2 into the dataset in prim_data_path. 
    Arguments:
        prim_data_path - str: filepath to the primary dataset (entso)
        sec_data_1 - str: filepath to the first secondary dataset (platts)
        sec_data_2 - str: filepath to the second secondary dataset (gppd)
    Returns:
        tuple of the processed dataframes
    """
    entso_df = csv_to_pd(prim_data_path)
    platts_df = csv_to_pd(sec_data_1)
    gppd_df = csv_to_pd(sec_data_2)

    # id columns not to be processed
    id_columns = ["unit_id", "plant_id"]

    df_list = clean_data([entso_df, platts_df, gppd_df],
                            id_columns)
    entso_df, platts_df, gppd_df = df_list

    # remove the (xx) country abbreviation from entso_df.country
    country_pattern = "\([a-z][a-z]\)"
    entso_df.country = remove_pattern(entso_df.country, country_pattern)

    return entso_df, platts_df, gppd_df




def df_join(entso_df:pd.DataFrame, platts_df:pd.DataFrame, gppd_df:pd.DataFrame, 
    fuel_dict:dict)->pd.DataFrame:
    """
    Joins the powerplant row in hte platts_df andd gppd_df onto
    the entso_df. Returns the joined dataframe.
    """
     
    print("\n========entso shape==========")
    print(entso_df.head())
    print(entso_df.shape)
    print(f"country: {entso_df['country'].value_counts()}")
    print(f"fuel: {entso_df.loc[:,'unit_fuel'].value_counts()}")
    print("\n========platts==========")
    print(platts_df.head())
    print(f"{platts_df.shape}")
    print(f"country:{platts_df['COUNTRY'].value_counts()}")
    print(f"fuel :{platts_df['UNIT_FUEL'].value_counts()}")
    print("\n========gppd==========")
    print(gppd_df.head())
    print(f"{gppd_df.shape}")
    print(f"country:{gppd_df['country_long'].value_counts()}")
    print(f"fuel :{gppd_df['plant_primary_fuel'].value_counts()}")
    merged_df = entso_df.merge(platts_df,
        left_on=['country'], right_on=['COUNTRY'], how='left', suffixes=('_ent', '_plt'))
    print("\n========merged==========")
    print(merged_df.head())
    print(merged_df[merged_df['country']=='germany'].shape)
    print(merged_df)
    merged_df["entso_unit_fuel_map"] = merged_df.apply(lambda row: fuel_dict[row.unit_fuel], axis=1)
    query_df = merged_df.query('UNIT_FUEL==entso_unit_fuel_map')
    print("\n========queried==========")
    print(query_df.head())
    print(query_df.shape)
    country_fuel_columns = ["unit_id_ent", "unit_id_plt","country", "COUNTRY","unit_fuel", "entso_unit_fuel_map"]
    name_columns=["unit_id_ent", "plant_name","unit_id_plt", "PLANT","UNIT"]
    print(query_df[name_columns].iloc[1500:1700,:])
    print(query_df[query_df['country']=='germany'].shape)









    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="maps three datasets (entso, platts, gppd) in dataset_dir \
        to a single dataset which is written out output_csv"
    )
    parser.add_argument("--dataset-dir", type=str, default="../data/",
        help="A path to a stored model.")
    today = str(date.today())
    parser.add_argument("--output-csv", type=str, default=f"../data/{today}_mapping_drz.csv",
        help="A path to a stored model.")

    args = parser.parse_args()

    main(args.dataset_dir, args.output_csv)
