import os
import mysql.connector
import tables

# Configuration: Modify table prefixes here
OLD_PREFIX = 'mdlyh_'  # The prefix of the old database
NEW_PREFIX = 'mdl_'    # The prefix of the new database

# Database connection parameters
OLD_DB_HOST = 'localhost'  # Host of the old MySQL database
OLD_DB_NAME = 'old_moodle_db'  # Name of the old Moodle database
OLD_DB_USER = 'your_user'  # Your database username
OLD_DB_PASSWORD = 'your_password'  # Your database password

NEW_DB_HOST = 'localhost'  # Host of the new MariaDB database
NEW_DB_NAME = 'new_moodle_db'  # Name of the new Moodle database
NEW_DB_USER = 'your_user'  # Your database username
NEW_DB_PASSWORD = 'your_password'  # Your database password

# Connect to old database (MySQL)
def connect_old_db():
    return mysql.connector.connect(
        host=OLD_DB_HOST,
        user=OLD_DB_USER,
        password=OLD_DB_PASSWORD,
        database=OLD_DB_NAME
    )

# Connect to new database (MariaDB)
def connect_new_db():
    return mysql.connector.connect(
        host=NEW_DB_HOST,
        user=NEW_DB_USER,
        password=NEW_DB_PASSWORD,
        database=NEW_DB_NAME
    )

# Function to get data from a specific table in the old database
def get_old_data(table_name):
    connection = connect_old_db()
    cursor = connection.cursor()
    query = f"SELECT * FROM {OLD_PREFIX}{table_name}"
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to generate INSERT statements
def generate_insert_statements(table_name, data):
    insert_statements = []
    if data:
        # Get column names
        columns_query = f"DESCRIBE {OLD_PREFIX}{table_name}"
        connection = connect_old_db()
        cursor = connection.cursor()
        cursor.execute(columns_query)
        columns = [column[0] for column in cursor.fetchall()]
        cursor.close()
        connection.close()

        # Prepare insert statements
        for row in data:
            values = ', '.join([f"'{str(value)}'" if value is not None else 'NULL' for value in row])
            insert_statement = f"INSERT INTO {NEW_PREFIX}{table_name} ({', '.join(columns)}) VALUES ({values});"
            insert_statements.append(insert_statement)

    return insert_statements

# Function to export SQL to a file
def export_to_sql_file(insert_statements, filename):
    with open(filename, 'w') as f:
        for statement in insert_statements:
            f.write(statement + "\n")

# Main process
def main():
    # Tables to transfer (you can adjust this list)
    tables_to_transfer = tables.tables_to_transfer()
    
    all_insert_statements = []

    for table in tables_to_transfer:
        print(f"Fetching data from table: {table}")
        data = get_old_data(table)
        print(f"Generating SQL insert statements for table: {table}")
        insert_statements = generate_insert_statements(table, data)
        all_insert_statements.extend(insert_statements)

    # Export SQL to file
    print(f"Exporting SQL to file: moodle_data_dump.sql")
    export_to_sql_file(all_insert_statements, 'moodle_data_dump.sql')
    print("Data export complete!")

if __name__ == "__main__":
    main()
