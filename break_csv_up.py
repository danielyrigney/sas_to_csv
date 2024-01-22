import os 
import pandas as pd

# break csv into 500 rows each and save to csv
def break_csv_up(csv_file):
    # Read the CSV file
    data = pd.read_csv(csv_file)
    # Extract the header
    header = data.columns.values.tolist()
    # Extract the data
    data = data.values.tolist()
    # break csv into 500 rows each and save to csv
    for i in range(0, len(data), 500):
        df = pd.DataFrame(data[i:i + 500])
        df.to_csv('data' + str(i) + '.csv', index=False, header=header)
    print('data has been broken up into 500 rows each and saved to csv')
    return header, data

current_working_directory = os.getcwd()
data_directory = os.path.join(current_working_directory, "data", "converged")
csv_file = os.path.join(data_directory, "2022-assessment-performance-campus.csv")

break_csv_up(csv_file)

