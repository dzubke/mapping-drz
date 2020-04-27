#standard libraries
import os
import argparse
from datetime import date
import csv
# third-party libraries
import pandas as pd
import numpy as np
import editdistance as ed
# project libraries
from data_class import Dataset
from utils.preprocess import clean_data
from utils.io import csv_to_pd, csv_to_dict, write_to_csv

np.set_printoptions(linewidth=300)
pd.options.display.max_rows = 300


def main(dataset_dir:str, output_csv:str):
    # define the paths
    entso_path = os.path.join(dataset_dir, "entso.csv")
    gppd_path = os.path.join(dataset_dir, "gppd.csv")
    platts_path = os.path.join(dataset_dir, "platts.csv")
    fuel_thes_path = os.path.join(dataset_dir, "fuel_thesaurus.csv")

    # read and process the data
    fuel_dict = csv_to_dict(fuel_thes_path)
    entso_data, platts_data, gppd_data = read_process(entso_path, 
        platts_path, gppd_path)

    # join the datasets
    joined_data = df_join(entso_data, platts_data, gppd_data, fuel_dict)
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
    entso_data = Dataset(entso_path, "unit_id")
    platts_data = Dataset(platts_path, "unit_id")
    gppd_data = Dataset(gppd_path, "plant_id")

    entso_data.clean_data()
    platts_data.clean_data()
    gppd_data.clean_data()

    # remove the (xx) country abbreviation from country column
    country_pattern = "\([a-z][a-z]\)"
    entso_data.remove_pattern(col_name="country", pattern=country_pattern)
    # removing dashes, underscores, and spaces in unit names
    punc_pattern = "[-_ ]"
    entso_data.remove_pattern(col_name="unit_name", pattern=punc_pattern)
    platts_data.remove_pattern(col_name="UNIT", pattern=punc_pattern)
    gppd_data.remove_pattern(col_name="plant_name", pattern=punc_pattern)

    return entso_data, platts_data, gppd_data


def df_join(entso_data:Dataset, platts_data:Dataset, gppd_data:Dataset, 
    fuel_dict:dict)->Dataset:
    """
    Joins the powerplant row in hte platts_df andd gppd_df onto
    the entso_df. Returns the joined dataframe.
    """
    print("\n========entso shape==========")
    print(entso_data.df.head())
    print(entso_data.df.shape)
    print(f"country: {entso_data.df['country'].value_counts()}")
    print(f"fuel: {entso_data.df.loc[:,'unit_fuel'].value_counts()}")
    print("\n========platts==========")
    print(platts_data.df.head())
    print(f"{platts_data.df.shape}")
    print(f"country:{platts_data.df['COUNTRY'].value_counts()}")
    print(f"fuel :{platts_data.df['UNIT_FUEL'].value_counts()}")
    print("\n========gppd==========")
    print(gppd_data.df.head())
    print(f"{gppd_data.df.shape}")
    print(f"country:{gppd_data.df['country_long'].value_counts()}")
    print(f"fuel :{gppd_data.df['plant_primary_fuel'].value_counts()}")

    merged_data = merge_entso_platts(entso_data, platts_data, fuel_dict)

    merged_data = merge_gppd(merged_data, gppd_data, fuel_dict)

    return merged_data

def merge_entso_platts(entso_data:Dataset, platts_data:Dataset,
    fuel_dict:dict) -> Dataset:
    """
    Merges the entso and platts datasets 
    """
    merged_data = Dataset()
    # merges based on country columns
    merged_data.df = entso_data.df.merge(platts_data.df,
        left_on=['country'], right_on=['COUNTRY'], how='left', suffixes=('_ent', '_plt'))

    print("\n========merged==========")
    print(merged_data.df.head())
    print(merged_data.df[merged_data.df['country']=='germany'].shape)
    print(merged_data.df)

    # creates a new column which maps the entso unit_fuel column
    # to the platts UNIT_FUEL column
    merged_data.df["entso_unit_fuel_map"] = merged_data.df.\
        apply(lambda row: fuel_dict[row.unit_fuel], axis=1)
    # selects rows where entso-mapped fuel column equals the platts fuels
    merged_data.df = merged_data.df.query('UNIT_FUEL==entso_unit_fuel_map')

    print("\n========queried==========")
    print(merged_data.df.head())
    print(merged_data.df.shape)
    country_fuel_columns = ["unit_id_ent", "unit_id_plt","country", "COUNTRY","unit_fuel", "entso_unit_fuel_map"]
    name_columns=["unit_id_ent", "plant_name", "unit_name","unit_id_plt", "PLANT","UNIT"]
    print(merged_data.df[name_columns].iloc[1500:1700,:])
    print(merged_data.df[merged_data.df['country']=='germany'].shape)

    merged_data.df = filter_unit_name(merged_data.df)

    return merged_data


def filter_unit_name(merged_df:pd.DataFrame)->pd.DataFrame:
    """
    filters the rows in the merged_df based on the unit_name (entso) 
    and UNIT (platts)
    """
    # accepted string distance between names
    max_dist = 6
    filter_df = pd.DataFrame(columns=merged_df.columns)
    for dist in range(max_dist):
        for index, row in merged_df.iterrows():
            # if unit_name has not already been appended, continue
            if not any(filter_df.unit_name.isin([row[4]])):
                # unit_name is index 4, UNIT is index 10
                if ed.eval(row[4], row[10]) < dist:
                    filter_df = filter_df.append(row)

    # mask_merged_df = merged_df.apply(
    #     lambda row: ed.eval(row.unit_name, row.UNIT) < allowed_dist, axis=1)
    name_columns=["unit_id_ent", "plant_name", "unit_name","unit_id_plt", "PLANT","UNIT"]
    print(filter_df[name_columns])

    return filter_df


def merge_gppd(merged_data:Dataset, gppd_data:Dataset, fuel_dict:dict):
    """
    Merges the gppd dataset onto the merged dataset (entso + platts)
    """
    merged_data.df = merged_data.df.merge(gppd_data.df,
        left_on=['country'], right_on=['country_long'], how='left', suffixes=('', '_gppd'))
    
    print("\n========gppd merged - 1==========")
    print(merged_data.df.head())
    print(merged_data.df[merged_data.df['country']=='germany'].shape)
    
    merged_data.df = merged_data.df.query('plant_primary_fuel==entso_unit_fuel_map')
        
    print("\n========gppd merged - 2==========")
    name_columns=["unit_id_ent", "plant_id", "wepp_id","unit_name", "country",]
    merged_data.df['plant_id'] = merged_data.df['plant_id'].astype("int64").apply(str)

    print(merged_data.df[name_columns].head(n=100))
    print(merged_data.df[merged_data.df['country']=='germany'].shape)
    #merged_data.df = merged_data.df[merged_data.df['wepp_id'].notna()]

    #merged_data.df['wepp_id'] = merged_data.df['wepp_id'].astype("int64")
    

    merged_data.df = merged_data.df.query('wepp_id==plant_id')
    print("\n========gppd merged - X ==========")
    name_columns=["unit_id_ent", "plant_id", "wepp_id","unit_name", "country",]
    #merged_data.df = merged_data.df[merged_data.df['plant_id']=='1030333']

    print("\n========gppd merged - 3 ==========")
    print(merged_data.df[name_columns].dtypes)

    print(merged_data.df[merged_data.df['country']=='germany'].shape)

    return merged_data
    



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="maps three datasets (entso, platts, gppd) in dataset_dir \
        to a single dataset which is written out output_csv"
    )
    parser.add_argument("--dataset-dir", type=str, default="../data/",
        help="A path to a stored model.")
    today = str(date.today())
    parser.add_argument("--output-csv", type=str, default=f"../result/{today}_mapping_drz.csv",
        help="A path to a stored model.")

    args = parser.parse_args()

    main(args.dataset_dir, args.output_csv)
