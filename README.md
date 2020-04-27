
This project joins three datasets of powerplant data into a single table written to a csv.


# Setup

Download the three datasets and put them in a directory. 

In an new environment run:

conda install --file requirements.txt

# Run
S
Navigate to src directory and run command:
	cd src
	python mapping.py


# Command defaults

You can set the directory where the data is downloaded with the
--dataset-dir option in the mapping.py command to this directory.The default option is "./data".

You can also specify the filepath of the output csv with the --output-csv option. The default is "./result/mapping_drz.csv"


# Future Work

 - The current approach to selecting the closest plant name in the filter_unit_name() function is not efficient. A faster approach would need to be developed as the current approach wouldn't scale. 
 - No tests are current written. I would do this if given more time. (I know everyone would always write more test in the future... but I would do it. Seriously.)
 - Also, the merge_entso_platts and merge_gppd functions do similar operations (match on country and then fuel type). I would aim to combine each into their own general function. 
 - There are duplicates in the output (WRI1019295 and WRI1019296) because the outputs are by unit and plant_id_gppd. I would filter the duplicates using a similar name-distance as in filter_unit_name(). 

# Final comments

 - Thanks for the opportunity!