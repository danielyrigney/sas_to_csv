import os
import shutil


source_folderpath = './data/unmasked/'
destination_folderpath = './data/converged/'
    
# Create the destination folder if it does not exist
os.makedirs(destination_folderpath, exist_ok=True)
    
# Iterate through all files and subdirectories in the source folder
for root, dirs, files in os.walk(source_folderpath):
    for filename in files:
        print(filename)
        # Check if the file is a CSV
        if filename.endswith('.csv'):
            # Construct the new filename based on the original folder path
            new_filename = '-'.join(os.path.relpath(root, source_folderpath).split(os.sep)) + '-' + filename
                
            # Construct the source and destination paths
            file_path = os.path.join(root, filename)
            destination_path = os.path.join(destination_folderpath, new_filename)
                
            # Copy the CSV file to the destination folder
            shutil.copy2(file_path, destination_path)