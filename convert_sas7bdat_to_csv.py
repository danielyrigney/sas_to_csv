import os
import pandas as pd

# Define the folder path to iterate through
folder_path = "./data/"

# Iterate through each file and folder in the given folder path
for root, dirs, files in os.walk(folder_path):
    for file in files:
        # Check if the file is a sas7bdat file
        if file.endswith(".sas7bdat"):
            # Load the sas7bdat file into a pandas dataframe
            sas_df = pd.read_sas(os.path.join(root, file))
            
            # Define the path and filename for the output csv file
            csv_file_path = os.path.join(root, os.path.splitext(file)[0] + ".csv")
            
            # Save the dataframe as a csv file
            sas_df.to_csv(csv_file_path, index=False)
            
            # Print the filename of the converted file
            print(f"Converted {file} to {csv_file_path}")



