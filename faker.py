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
    email = fake.email()
    city = fake.city()

    cursor.execute(
        "INSERT INTO sample_table (id, name, created_date) VALUES (%s,%s,%s)",
        (name, email, city)
    )

conn.commit()
cursor.close()
conn.close()