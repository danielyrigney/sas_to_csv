import pandas as pd
import os

# take the first 10 lines from a csv and save to a new csv
def take_1000_rows(csv_file):
    # Read the CSV file
    data = pd.read_csv(csv_file)
    # Extract the header
    header = data.columns.values.tolist()
    # Extract the data
    data = data.values.tolist()
    # take the first 10 lines from a csv and save to a new csv
    df = pd.DataFrame(data[0:5000000])
    df.to_csv('smalldata' + str(1) + '.csv', index=False, header=header)
    print('data has been taken from csv and saved to csv')

# current_working_directory = os.getcwd()
# data_directory = os.path.join(current_working_directory, "data", "converged")
# csv_file = os.path.join(data_directory, "2022-assessment-performance-campus.csv")
csv_file = '2019-assessment-performance-campus.csv'
# csv_file = './data/processed/2021-assessment-performance-campus.csv'

take_1000_rows(csv_file)

