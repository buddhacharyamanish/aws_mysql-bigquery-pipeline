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

    cursor.execute(
        "INSERT INTO sample_table (id, name, created_date) VALUES (%s,%s,%s)",
        (id, name, created_date)
    )

conn.commit()
cursor.close()
conn.close()