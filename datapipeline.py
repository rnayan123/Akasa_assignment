import pandas as pd
import pyodbc
from datetime import datetime

# Load CSV data
df = pd.read_csv(r'E:\AKASA_ASSIGNMENT\formatted.csv')

# Function to convert to 24-hour format
def clean_and_convert_to_24_hour_format(time_str):
    if pd.isna(time_str):  # Check for NaN values
        return None
    # Remove "AM" or "PM" if the time is already in 24-hour format
    if "PM" in time_str or "AM" in time_str:
        try:
            # Convert 12-hour format to 24-hour format
            return datetime.strptime(time_str, '%I:%M %p').strftime('%H:%M')
        except ValueError:
            # If conversion fails, strip AM/PM and treat as 24-hour format
            time_str = time_str.replace("PM", "").replace("AM", "").strip()
    
    try:
        # Attempt to parse the remaining string as 24-hour time
        return datetime.strptime(time_str, '%H:%M').strftime('%H:%M')
    except ValueError:
        # Return None if conversion fails
        return None

# Apply the conversion function to the columns
df['DepartureTime'] = df['DepartureTime'].apply(clean_and_convert_to_24_hour_format)
df['ArrivalTime'] = df['ArrivalTime'].apply(clean_and_convert_to_24_hour_format)

# Print first 5 rows to verify the time conversion
print(df.head())

# Database connection setup (replace with your credentials)
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                    'SERVER=LAPTOP-JR0QUA3M;' \
                    'DATABASE=aviation_analysis;' \
                    'UID=sa;' \
                    'PWD=nayan1810;'

# Use context manager for database connection and cursor
try:
    with pyodbc.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            # Loop over the DataFrame rows and insert/update data into SQL Server
            for index, row in df.iterrows():
                # SQL query using MERGE for insert or update
                merge_query = """
                MERGE INTO flights AS target
                USING (SELECT ? AS FlightNumber, ? AS DepartureDate, ? AS DepartureTime, 
                              ? AS ArrivalDate, ? AS ArrivalTime, ? AS Airline, 
                              ? AS DelayMinutes, ? AS FlightDuration) AS source
                ON (target.FlightNumber = source.FlightNumber AND target.DepartureDate = source.DepartureDate)
                WHEN MATCHED THEN
                    UPDATE SET target.DepartureTime = source.DepartureTime,
                               target.ArrivalDate = source.ArrivalDate,
                               target.ArrivalTime = source.ArrivalTime,
                               target.Airline = source.Airline,
                               target.DelayMinutes = source.DelayMinutes,
                               target.FlightDuration = source.FlightDuration
                WHEN NOT MATCHED THEN
                    INSERT (FlightNumber, DepartureDate, DepartureTime, ArrivalDate, ArrivalTime, Airline, DelayMinutes, FlightDuration)
                    VALUES (source.FlightNumber, source.DepartureDate, source.DepartureTime, source.ArrivalDate, source.ArrivalTime, source.Airline, source.DelayMinutes, source.FlightDuration);
                """

                # Execute the query with the row data
                cursor.execute(merge_query, row['FlightNumber'], row['DepartureDate'], row['DepartureTime'], 
                               row['ArrivalDate'], row['ArrivalTime'], row['Airline'], 
                               row['DelayMinutes'], row['FlightDuration'])

            # Commit the transaction to the database
            connection.commit()

except Exception as e:
    print(f"An error occurred: {e}")
