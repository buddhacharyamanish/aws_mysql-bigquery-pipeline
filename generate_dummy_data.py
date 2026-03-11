from faker import Faker
import random
import mysql.connector

fake = Faker()

conn = mysql.connector.connect(
    host="localhost",
    user="sampleuser",
    password="Sample@123",
    database="sampledb"
)

cursor = conn.cursor()

for _ in range(1000):

    name = fake.name()
    created_date = fake.date_time()
    email = fake.safe_email()
    phoneno = fake.numerify("04########")


    cursor.execute(
        "INSERT INTO sample_table (name, created_date) VALUES (%s,%s)",
        (name, created_date)
    )

    cursor.execute(
        "INSERT INTO sample_table2 (email, phoneno) VALUES (%s,%s)",
        (email, phoneno)
    )

conn.commit()
cursor.close()
conn.close()