import numpy as np
import pandas as pd
import os
from decimal import Decimal, ROUND_HALF_UP

def read_csv_file(csv_file):
    # Read the CSV file
    data = pd.read_csv(csv_file)
    print(data.shape)    
    
    # If the file has a column named 'DISTRICT, drop the column
    #if 'DISTRICT' in data.columns:
    #    data = data.drop('DISTRICT', axis=1)
    columns_to_drop = data.columns[(data.columns != 'CAMPUS') & ((data.columns.str.len() > 13) | (data.columns.str.len() < 13))]
    data = data.drop(columns_to_drop, axis=1)

    columns_to_drop = data.columns[(data.columns != 'CAMPUS') & (~data.columns.str[1].isin(['D', 'N']))]
    data = data.drop(columns_to_drop, axis=1)

    columns_to_drop = data.columns[(data.columns != 'CAMPUS') & (~data.columns.str[11].isin(['9']))]
    data = data.drop(columns_to_drop, axis=1)

    columns_to_drop = data.columns[(data.columns != 'CAMPUS') & (~data.columns.str[8].isin(['T', '1']))]
    data = data.drop(columns_to_drop, axis=1)

    print(data.shape)
    
    # Extract the header
    header = data.columns.values.tolist()

    print(header[0:10])

    # Extract the data
    data = data.values.tolist()
    print('Data has been loaded into dataframe')
    return header, data

def process_data_with_pandas(header, data):
    # Convert the data to a DataFrame
    data = pd.DataFrame(data, columns=header)
    
    # Convert the data from wide to long format
    data = pd.melt(
        data,
        id_vars=['CAMPUS'],  # Use 'CAMPUS' as the identifier variable
        value_vars=header[1:],
        var_name='Column Name',
        value_name='Value'
    )

    print("Data has been converted from wide to long format")

    # Remove all rows where the value is NaN
    data = data.dropna(subset=['Value'])
    
    # Convert all values in 'Column name' to uppercase
    data['Column Name'] = data['Column Name'].str.upper()

    # Get column name without the calculation code 
    data['Previous Code'] = data['Column Name'].apply(get_new_code)
    
    # Create a new column for the student group
    data['Student Group'] = ''

    condition = data['Column Name'].str[1] == 'D'
    data['Student Group'] = np.where(condition, data['Column Name'].apply(get_student_groupD), data['Column Name'].apply(get_student_groupN))

    print("Student group column has been added")
    # Extract the student grade
    data['Student Grade'] = data['Column Name'].apply(get_student_grade)
    print("Student grade column has been added")
    # Extract the subject
    data['Subject'] = data['Column Name'].apply(get_subject)
    print("Subject column has been added")
    # Extract the calculation
    data['Calculation'] = data['Column Name'].apply(get_calculation)
    print("Calculation column has been added")
    # Extract the level
    data['Level'] = data['Column Name'].apply(get_level)
    print("Level column has been added")
    # Extract the student group prefix
    data['Student Group Prefix'] = data['Column Name'].apply(get_student_group_prefix)
    print("Student group prefix column has been added")
    # Extract the test
    data['Test'] = data['Column Name'].apply(get_test)
    print("Test column has been added")
    # Extract the type
    data['Type'] = data['Column Name'].apply(get_type)
    print("Type column has been added")
    # Extract the level/standard
    data['Level/Standard'] = data['Column Name'].apply(get_level_standard)
    print("Level/Standard column has been added")
    # Extract the year
    data['Year'] = data['Column Name'].apply(get_year)
    print("Year column has been added")
    # Extract the learning mode
    #data['Learning Mode'] = data['Column Name'].apply(get_learning_mode)
    #print("Learning mode column has been added")

    # Drop the column name
    data = data.drop('Column Name', axis=1)

    print("All columns have been addeded based on the coding system, starting the pivot")

    # Convert the data from long to wide format
    data = data.pivot(
        index=['CAMPUS', 'Previous Code', 'Student Group', 'Student Grade', 'Subject', 'Level', 'Student Group Prefix',
               'Test', 'Type', 'Level/Standard', 'Year'],
        columns='Calculation',
        values='Value'
    ).reset_index()

    # Rename the columns
    data.columns.name = None

    # Delete rows where the value of year is '21'
    data = data[data['Year'] != '18']

    # Delete rows where the value of type is 'Participation'
    # data = data[data['Type'] != 'Participation']

     # Delete rows where the value of type is 'School Progress'
    data = data[data['Type'] != 'School Progress']

    # In the denominator use ffill to fill in rows that are null
    data['Denominator'] = data['Denominator'].fillna(method='ffill')

    # Calculate the percentage from the numerator and denominator and round to the nearest whole number, also ensure that .5 rounds up    
    data['Percentage'] = round(data['Numerator'] / data['Denominator'] * 100, 5)
    data['Percentage'] = data['Percentage'].apply(lambda x: Decimal(x).quantize(0, ROUND_HALF_UP))

    # Delete rows where the value of 'Level/Standard' is 'No Level'
    data = data[data['Level/Standard'] != 'No Level']

    # Print out the total number of rows where 'Percentage' is different from 'Rate'
    print("Difference: " + str(len(data[data['Percentage'] != data['Rate']])))

    # Return the data
    return data

def get_new_code(column_name):
    # Replace the 13th character with an "x" and return the new code
    return column_name[:12] + 'x' + column_name[13:]

def get_student_groupD(column_name):
    if len(column_name) < 3:
        return ''
    student_group_lookup = {
        'A': 'All', 
        'B': 'Black', 
        'W': 'White', 
        'H': 'Hispanic', 
        'I': 'Indian',
        '2': 'Two or More Races', 
        '3': 'Asian', 
        '4': 'Pac Isl', 
        'M': 'Male', 
        'F': 'Female',
        'S': 'Special Ed', 
        '6': 'Non-SpecEd', 
        'E': 'Econ Dis', 
        'N': 'Non-Econ', 
        'R': 'At Risk',
        '7': 'Non-AtRisk', 
        'L': 'ELL', 
        '8': 'Non-ELL', 
        'O': 'ELL=F', 
        'P': 'ELL=S',
        'G': 'Migrant', 
        '9': 'Non Migrant', 
        'V': 'CATE (2021)', 
        'D': 'Non CATE (2021)',
        '1': 'CATE =1(2021)', 
        'C': 'ESL', 
        'U': 'Bilingual', 
        '5': 'LEP w/Services',
        'Z': 'LEP no Services', 
        'X': 'ESL/Content-Based', 
        'Y': 'ESL/Pull-Out',
        'Q': 'Dual Lang Immers/Two-Way', 
        'T': 'Dual Lang Immers/One-Way',
        'J': 'Trans Bil/Early Exit', 
        'K': 'Trans Bil/Late Exit', 
        '0': 'Current + Monitored ELL',
    }
    return student_group_lookup.get(column_name[2], '')

def get_student_groupN(column_name):
    if len(column_name) < 3:
        return ''
    student_group_lookup = {
        'H': 'Homeless (2019)', 
        'F': 'Foster Care (2019)', 
        'S': 'Former Special Ed',
        'C': 'Continuously Enrolled', 
        'M': 'Mobile (Non-Continuously Enrolled)',
        'B': 'ALP Bilingual (Exception) (2021)', 
        'E': 'ALP ESL (Waiver) (2021)',
        'N': 'Never EL (2021)', 
        'T': 'Monitored & Former EL (2021)'
    }
    return student_group_lookup.get(column_name[2], '')

def get_student_grade(column_name):
    if len(column_name) < 5:
        return ''
    student_grade_lookup = {
        '00': 'All', '03': '3', '04': '4', '05': '5', '06': '6', '07': '7', '08': '8',
        '09': '9', '10': '10', '11': '11'
    }
    grade = column_name[3:5]
    return student_grade_lookup.get(grade, '')

def get_subject(column_name):
    if len(column_name) < 9:
        return ''
    subject_lookup = {
        'A1': 'Alg 1', 'E1': 'English 1', 'E2': 'English 2', 'BI': 'Biology',
        'US': 'US History', 'MA': 'Math', 'RE': 'Reading', 'WR': 'Writing',
        'SC': 'Science', 'SS': 'Social Studies', 'M0': 'Math Total',
        'R0': 'Read Total', 'W0': 'Writing Total', 'C0': 'Science Total',
        'S0': 'SS Total', '00': 'All subjects', 'B0': 'Both Total',
        'RM': 'Reading & Math', 'A0': 'Accelerated Total',
        'AR': 'Accelerated Reading', 'AM': 'Accelerated Math',
        'AS': 'Accelerated Science', 'R1': 'English 1', 'R2': 'English 2'
    }
    subject = column_name[6:8]
    return subject_lookup.get(subject, '')

def get_calculation(column_name):
    if len(column_name) < 13:
        return ''
    calculation_lookup = {
        'N': 'Numerator', 'D': 'Denominator', 'R': 'Rate'
    }
    calculation = column_name[12]
    return calculation_lookup.get(calculation, '')

def get_level(column_name):
    if len(column_name) < 1:
        return ''
    level_lookup = {
        'C': 'Campus', 'D': 'District', 'R': 'Region', 'S': 'State'
    }
    return level_lookup.get(column_name[0], '')

def get_student_group_prefix(column_name):
    if len(column_name) < 2:
        return ''
    prefix_lookup = {
        'D': 'Domain', 'N': 'New (indicated in blue)'
    }
    return prefix_lookup.get(column_name[1], '')

def get_test(column_name):
    if len(column_name) < 6:
        return ''
    test_lookup = {
        'A': 'All Tests', 'S': 'STAAR English', 'P': 'STAAR Spanish', 'T': 'Alternate'
    }
    return test_lookup.get(column_name[5], '')

def get_type(column_name):
    if len(column_name) < 9:
        return ''
    type_lookup = {
        'T': 'Participation', '1': 'Student Achievement', '2': 'School Progress', '3': 'Closing the Gaps',
        'R': 'Performance, Acct Subset', 'Q': 'Performance, Non-Acct Subset', 'B': 'Performance, All (Both R and Q)',
        'E': 'STAAR EOC STAAR Reading and Math (Include EOC)',
        'D': 'STAAR Reading and Math (Exclude EOC)'
    }
    return type_lookup.get(column_name[8], '')

def get_level_standard(column_name):
    if len(column_name) < 10:
        return ''
    level_standard_lookup = {
        '0': 'No Level', 'S': 'Approaches GL (Fed)', 's': 'Approaches GL (Fed)', '2': 'Meets GL', '3': 'Masters GL',
        'G': 'Met or Exceeded Growth', 'E': 'Exceeds Growth', 'F': 'Meets (Fed)', 'A': 'Masters (Fed)'
    }
    return level_standard_lookup.get(column_name[9], '')

def get_year(column_name):
    if len(column_name) < 13:
        return ''
    year_lookup = {
        '17': '17', '18': '18', '19': '19', '20': '20', '21': '21', '22': '22'
    }
    year = column_name[10:12]
    return year_lookup.get(year, '')

def get_learning_mode(column_name):
    # if length of column_name is less than 14, return empty string
    if len(column_name) < 14:
        return ''
    learning_mode_lookup = {
        'V': 'Virtual', 'P': 'In Person'
    }
    return learning_mode_lookup.get(column_name[13], '')

def main():
    current_working_directory = os.getcwd()
    data_directory = os.path.join(current_working_directory, "data", "converged")
    csv_file = os.path.join(data_directory, "2019-assessment-performance-campus.csv")
    # csv_file = './data/split_files/data0.csv'
    # csv_file = 'input.csv'
    
    # Read the CSV file
    header, data = read_csv_file(csv_file)
    # Process the data
    try:
        data = process_data_with_pandas(header, data)
        # Save the data to a CSV file
        data.to_csv('2019-assessment-performance-campus.csv', index=False)
        print("Data processing completed successfully.")
    except KeyError as e:
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()
