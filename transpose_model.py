import csv
import os
import pandas as pd

def read_csv_file(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        data = list(reader)
    return header, data

def get_student_group(column_name):
    student_group_lookup = {
        'A': 'All', 'B': 'Black', 'W': 'White', 'H': 'Hispanic', 'I': 'Indian',
        '2': 'Two or More Races', '3': 'Asian', '4': 'Pac Isl', 'M': 'Male', 'F': 'Female',
        'S': 'Special Ed', '6': 'Non-SpecEd', 'E': 'Econ Dis', 'N': 'Non-Econ', 'R': 'At Risk',
        '7': 'Non-AtRisk', 'L': 'ELL', '8': 'Non-ELL', 'O': 'ELL=F', 'P': 'ELL=S',
        'G': 'MiGrant', '9': 'Non Migrant', 'V': 'CATE (2021)', 'D': 'Non CATE (2021)',
        '1': 'CATE =1(2021)', 'C': 'ESL', 'U': 'Bilingual'
    }
    return student_group_lookup.get(column_name[2], '')

def get_student_grade(column_name):
    student_grade_lookup = {
        '00': 'All', '03': '3', '04': '4', '05': '5', '06': '6', '07': '7', '08': '8',
        '09': '9', '10': '10', '11': '11'
    }
    grade = column_name[3:5]
    return student_grade_lookup.get(grade, '')


def get_subject(column_name):
    subject_lookup = {
        'A1': 'Alg 1', 'E1': 'English 1', 'E2': 'English 2', 'BI': 'Biology',
        'US': 'US History', 'MA': 'Math', 'RE': 'Reading', 'WR': 'Writing',
        'SC': 'Science', 'SS': 'Social Studies', 'M0': 'Math Total',
        'R0': 'Read Total', 'W0': 'Writing Total', 'C0': 'Science Total',
        'S0': 'SS Total', '00': 'All subjects', 'B0': 'Both Total',
        'RM': 'Reading & Math', 'A0': 'Accelerated Total',
        'AR': 'Accelerated Reading', 'AM': 'Accelerated Math',
        'AS': 'Accelerated Science'
    }
    subject = column_name[6:8]
    return subject_lookup.get(subject, '')

def get_calculation(column_name):
    calculation_lookup = {
        'N': 'Numerator', 'D': 'Denomenator', 'R': 'Rate'
    }
    calculation = column_name[12]
    return calculation_lookup.get(calculation, '')

def process_csv_data(header, data):
    processed_data = []
    for row in data:
        primary_key = row[0]
        for i in range(1, len(row)):
            column_value = row[i]
            column_name = header[i]
            student_group = get_student_group(column_name)
            student_grade = get_student_grade(column_name)
            subject = get_subject(column_name)
            calculation = get_calculation(column_name)
            processed_data.append([primary_key, student_group, student_grade, subject, calculation, column_value])
    processed_data = pd.DataFrame(processed_data, columns=['Primary Key', 'Student Group', 'Student Grade', 'Subject', 'Calculation', 'Value'])
    return processed_data


def combine_num_den_rate(processed_data):
    
    processed_data = processed_data.pivot(
        index=['Primary Key', 'Student Group', 'Student Grade', 'Subject'],
        columns='Calculation',
        values='Value'
    ).reset_index()

    processed_data.columns.name = None

    return processed_data

def write_csv_file(processed_data, csv_file):
    # Convert processed_data to a DataFrame
    # Write the DataFrame to a CSV file
    processed_data.to_csv(csv_file, index=False)

def main():
    current_working_directory = os.getcwd()
    data_directory = os.path.join(current_working_directory, "data", "converged")
    csv_file = os.path.join(data_directory, "2022-assessment-participation-district.csv")
    # csv_file = 'input.csv'
    processed_file = 'confid_output.csv'  
    """
    To do: paramatarize the input, output will be input filename + _processed
    """
    header, data = read_csv_file(csv_file)
    processed_data = process_csv_data(header, data)
    processed_data = combine_num_den_rate(processed_data)
    write_csv_file(processed_data, processed_file)

if __name__ == '__main__':
    main()
