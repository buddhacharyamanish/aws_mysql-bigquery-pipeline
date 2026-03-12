from faker import Faker
import random
import mysql.connector
from datetime import datetime

fake = Faker()

conn = mysql.connector.connect(
    host="localhost",
    user="sampleuser",
    password="Sample@123",
    database="sampledb"
)

cursor = conn.cursor()

for _ in range(2000):

    name = fake.name()
    created_date = datetime.now()
    email = fake.safe_email()
    phoneno = fake.numerify("04########")
    address = fake.address()


    cursor.execute(
        "INSERT INTO sample_table (name, created_date,address) VALUES (%s,%s,%s)",
        (name, created_date,address)
    )

    cursor.execute(
        "INSERT INTO sample_table2 (email, phoneno) VALUES (%s,%s)",
        (email, phoneno)
    )

conn.commit()
cursor.close()
conn.close()
