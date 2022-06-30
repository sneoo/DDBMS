import psycopg2
from psycopg2 import Error

try:
    # Connect to an existing database
    connection = psycopg2.connect(user="postgres",
                                  password="1234",
                                  host="localhost",
                                  port="5432",
                                  database="Student")
    print("db connection got established!")
    # Print PostgreSQL details
    # Create a cursor to perform database operations
    cursor = connection.cursor()
    dropTable = 'drop table if exists range_part'
    createTable = 'create table if not exists range_part'

    for i in range(3):
        cursor.execute(dropTable + str(i) + ';')
    for i in range(3):
        cursor.execute(createTable + str(i) + ' (ID int, Name varchar(30), Dept varchar(30), Cgpa int);')
    connection.commit()


    insertFirstPartition = """insert into range_part0 SELECT * FROM student where dept = 'cse'""" + ';'
    cursor.execute(insertFirstPartition)

    insertRemainingPartitions = """insert into range_part1 SELECT * FROM student where dept = 'eee'""" + ';'
    cursor.execute(insertRemainingPartitions)

    insertRemainingPartitions = """insert into range_part2 SELECT * FROM student where dept = 'Me'""" + ';'
    cursor.execute(insertRemainingPartitions)
    connection.commit()

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")