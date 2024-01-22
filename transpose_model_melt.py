import pandas as pd

def read_csv_file(csv_file):
    # Read the CSV file
    data = pd.read_csv(csv_file)
    # Extract the header
    header = data.columns.values.tolist()
    # Extract the data
    data = data.values.tolist()
    return header, data


def process_data_with_pandas(header, data):
    # Convert the data to a DataFrame
    data = pd.DataFrame(data, columns=header)
    # Convert the data from wide to long format
    data = pd.melt(
        data,
        id_vars=['Primary Key'],
        value_vars=header[1:],
        var_name='Column Name',
        value_name='Value'
    )
    # Extract the student group
    data['Student Group'] = data['Column Name'].apply(get_student_group)
    # Extract the student grade
    data['Student Grade'] = data['Column Name'].apply(get_student_grade)
    # Extract the subject
    data['Subject'] = data['Column Name'].apply(get_subject)
    # Extract the calculation
    data['Calculation'] = data['Column Name'].apply(get_calculation)
    # Drop the column name
    data = data.drop('Column Name', axis=1)
    # Convert the data from long to wide format
    data = data.pivot_table(
        index=['Primary Key', 'Student Group', 'Student Grade', 'Subject'],
        columns='Calculation',
        values='Value'
    ).reset_index()
    # Rename the columns
    data.columns.name = None
    # Return the data
    return data

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


def main():
    # Read the CSV file
    header, data = read_csv_file('input.csv')
    # Process the data
    data = process_data_with_pandas(header, data)
    # Save the data to a CSV file
    data.to_csv('data_transposed.csv', index=False)

if __name__ == '__main__':
    main()