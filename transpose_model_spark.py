import os
from decimal import Decimal, ROUND_HALF_UP
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DecimalType, IntegerType

def read_csv_file(csv_file, spark):
    # Configure the CSV file reader with the maxColumns option
    csv_reader = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("maxColumns", 25000) \
        .csv(csv_file)

    # Read the CSV file
    data = csv_reader.collect()
    print(len(data))
    
    # Drop columns with invalid names
    columns_to_drop = [column for column in data.columns if column != 'CAMPUS' and ('DISTRICT' in column or len(column) != 13)]
    data = data.drop(*columns_to_drop)
    print(data.count())

    # Extract the header
    header = data.columns

    # Convert the data types
    data = data.select(*[col(column).cast(DecimalType(18, 5)) for column in header[1:]])

    print("Data has been loaded into DataFrame")
    return header, data

def process_data_with_pyspark(header, data):
    # Convert the data from wide to long format
    data = data.selectExpr("CAMPUS", "stack({}, {}) as (Column_Name, Value)".format(len(header) - 1, ", ".join(header[1:])))
    
    print("Data has been converted from wide to long format")

    # Remove rows with null values in 'Value'
    data = data.na.drop(subset=["Value"])
    
    # Convert all values in 'Column_Name' to uppercase
    data = data.withColumn("Column_Name", col("Column_Name").cast(StringType()).upper())

    # Create a new column for the student group
    get_student_group_udf = udf(get_student_group, StringType())
    data = data.withColumn("Student_Group", get_student_group_udf(col("Column_Name")))

    print("Student group column has been added")
    # Extract other columns using UDFs
    get_student_grade_udf = udf(get_student_grade, StringType())
    get_subject_udf = udf(get_subject, StringType())
    get_calculation_udf = udf(get_calculation, StringType())
    get_level_udf = udf(get_level, StringType())
    get_student_group_prefix_udf = udf(get_student_group_prefix, StringType())
    get_test_udf = udf(get_test, StringType())
    get_type_udf = udf(get_type, StringType())
    get_level_standard_udf = udf(get_level_standard, StringType())
    get_year_udf = udf(get_year, StringType())
    data = data.withColumn("Student_Grade", get_student_grade_udf(col("Column_Name")))
    data = data.withColumn("Subject", get_subject_udf(col("Column_Name")))
    data = data.withColumn("Calculation", get_calculation_udf(col("Column_Name")))
    data = data.withColumn("Level", get_level_udf(col("Column_Name")))
    data = data.withColumn("Student_Group_Prefix", get_student_group_prefix_udf(col("Column_Name")))
    data = data.withColumn("Test", get_test_udf(col("Column_Name")))
    data = data.withColumn("Type", get_type_udf(col("Column_Name")))
    data = data.withColumn("Level_Standard", get_level_standard_udf(col("Column_Name")))
    data = data.withColumn("Year", get_year_udf(col("Column_Name")))

    print("Columns have been added based on the coding system, starting the pivot")

    # Convert the data from long to wide format
    data = data.groupBy("CAMPUS", "Previous_Code", "Student_Group", "Student_Grade", "Subject", "Level",
                        "Student_Group_Prefix", "Test", "Type", "Level_Standard", "Year").pivot("Calculation").sum("Value")
    
    # Rename the columns
    data = data.toDF(*[column.replace("sum(Value)", column) for column in data.columns])

    # Delete rows where the value of 'Year' is '18'
    data = data.filter(col("Year") != "18")

    # Delete rows where the value of 'Type' is 'School Progress'
    data = data.filter(col("Type") != "School Progress")

    # In the denominator, fill null values with the last non-null value
    fill_denominator = udf(lambda x: x or None, DecimalType(18, 5))
    data = data.withColumn("Denominator", fill_denominator(col("Denominator")).over(Window.partitionBy("CAMPUS").orderBy("Year")))
    
    # Calculate the percentage from the numerator and denominator and round to the nearest whole number, ensuring that .5 rounds up
    data = data.withColumn("Percentage", (col("Numerator") / col("Denominator") * 100).cast(IntegerType()))
    data = data.withColumn("Percentage", round(col("Percentage") + 0.5))

    # Delete rows where the value of 'Level_Standard' is 'No Level'
    data = data.filter(col("Level_Standard") != "No Level")

    # Print out the total number of rows where 'Percentage' is different from 'Rate'
    print("Difference:", data.filter(col("Percentage") != col("Rate")).count())

    # Return the data
    return data

# UDFs for data transformations
def get_student_group(column_name):
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

def main():
    spark = SparkSession.builder.master("local").appName("DataProcessing").getOrCreate()
    current_working_directory = os.getcwd()
    data_directory = os.path.join(current_working_directory, "data", "converged")
    csv_file = os.path.join(data_directory, "2018-assessment-performance-campus.csv")
    
    # Read the CSV file
    header, data = read_csv_file(csv_file, spark)
    
    # Process the data
    try:
        data = process_data_with_pyspark(header, data)
        
        # Save the data to a CSV file
        data.write.csv('2018-assessment-performance-campus.csv', header=True, mode="overwrite")
        print("Data processing completed successfully.")
    except Exception as e:
        print(f"Error: {str(e)}")

    spark.stop()

if __name__ == '__main__':
    main()
