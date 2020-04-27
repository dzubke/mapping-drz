This project joins three datasets of powerplant data into a single table written to a csv.

# Setup
With conda: 
```
conda env create -f environment.yml
conda activate wt_map_drz
```

With pip: 
Create a new environment with python 3.7. Then run: 
```
pip install -r requirements.txt 
```

# Run
Navigate to `src` directory and run the mapping command:
```
cd src
python mapping.py
```


# Command defaults
You can set the directory where the data is downloaded with the
`--dataset-dir` option in the mapping.py command to this directory. The default option is `"./data"`.

You can also specify the filepath of the output csv with the `--output-csv` option. The default is `"./result/{date}_mapping_drz.csv"`


# Future Work
 - The current approach to selecting the closest plant name in the filter_unit_name() function is not efficient. A faster approach would need to be developed as the current approach wouldn't scale. 
 - No tests are current written. I would write these tests, promise :)  
 - Also, the merge_entso_platts and merge_gppd functions do similar operations (match on country and then fuel type). I would aim to combine each matching into their own general function. 
 - There are duplicates in the mapping.csv (WRI1019295 and WRI1019296) because the outputs are by unit and plant_id_gppd is by plant. I would filter the duplicates using a similar name-distance as in filter_unit_name() to select the correct unit. 

# Final comments
 - Thanks for the opportunity!