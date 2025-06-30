import os
import mysql.connector
import tables

# Configuration: Modify table prefixes here
OLD_PREFIX = 'mdlyh_'
NEW_PREFIX = 'mdl_'

# Database connection parameters
OLD_DB_HOST = 'localhost'
OLD_DB_NAME = 'old_moodle_db'
OLD_DB_USER = 'your_user'
OLD_DB_PASSWORD = 'your_password'

NEW_DB_HOST = 'localhost'
NEW_DB_NAME = 'new_moodle_db'
NEW_DB_USER = 'your_user'
NEW_DB_PASSWORD = 'your_password'

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
    query = "SELECT * FROM {}{}".format(OLD_PREFIX, table_name)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to generate INSERT statements
def generate_insert_statements(table_name, data):
    insert_statements = []
    if data:
        columns_query = "DESCRIBE {}{}".format(OLD_PREFIX, table_name)
        connection = connect_old_db()
        cursor = connection.cursor()
        cursor.execute(columns_query)
        columns = [column[0] for column in cursor.fetchall()]
        cursor.close()
        connection.close()

        for row in data:
            values_list = []
            for value in row:
                if value is None:
                    values_list.append("NULL")
                else:
                    escaped_value = str(value).replace("'", "''")  # escape single quotes
                    values_list.append("'{}'".format(escaped_value))
            values = ', '.join(values_list)
            insert_statement = "INSERT INTO {}{} ({}) VALUES ({});".format(
                NEW_PREFIX, table_name, ', '.join(columns), values
            )
            insert_statements.append(insert_statement)

    return insert_statements

# Function to export SQL to a file
def export_to_sql_file(insert_statements, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for statement in insert_statements:
            f.write(statement + "\n")

# Main process
def main():
    tables_to_transfer = tables.tables_to_transfer()
    all_insert_statements = []

    for table in tables_to_transfer:
        print("Fetching data from table: {}".format(table))
        data = get_old_data(table)
        print("Generating SQL insert statements for table: {}".format(table))
        insert_statements = generate_insert_statements(table, data)
        all_insert_statements.extend(insert_statements)

    print("Exporting SQL to file: moodle_data_dump.sql")
    export_to_sql_file(all_insert_statements, 'moodle_data_dump.sql')
    print("Data export complete!")

if __name__ == "__main__":
    main()
