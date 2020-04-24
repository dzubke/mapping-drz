#standard libraries
import os
import argparse
from datetime import date
# third-part libraries
import pandas as pd



def main(dataset_dir:str, output_csv:str):
    entso_path = os.path.join(dataset_dir, "entso.csv")
    gppd_path = os.path.join(dataset_dir, "gppd.csv")
    platts_path = os.path.join(dataset_dir, "platts.csv")
    fuel_thes_path = os.path.join(dataset_dir, "fuel_thesaurus.csv")

    mapping(entso_path, platts_path, gppd_path, fuel_thes_path, output_csv)

def mapping(prim_data_path:str, sec_data_1:str, sec_data_2:str, 
    fuel_map:str, output_csv:str)->None:
    """
    This function performs a mapping of the entries of the datasets in sec_data_1
    and sec_data_2 into the dataset in prim_data_path. 
    Arguments:
        prim_data_path str: filepath to the primary dataset (entso)
        sec_data_1 str: filepath to the first secondary dataset (platts)
        sec_data_2 str: filepath to the second secondary dataset (gppd)
        fuel_map str: filepath to fuel type mapping 
        output_csv str: filepath where the output csv file is written
    Returns:
        None : a csv file is written 

    """
    entso_df = pd.read_csv(prim_data_path)
    print(entso_df.head())




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
