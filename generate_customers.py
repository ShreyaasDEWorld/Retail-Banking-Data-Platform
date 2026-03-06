# generate_customers.py

import pandas as pd
import numpy as np
import random
from faker import Faker
from config import NUM_CUSTOMERS, RANDOM_SEED

fake = Faker()
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

def generate_customers():

    income_bands = ["LOW", "MID", "HIGH", "PREMIUM"]
    income_weights = [0.3, 0.4, 0.2, 0.1]

    risk_segments = ["LOW", "MEDIUM", "HIGH"]

    customers = []

    for _ in range(NUM_CUSTOMERS):

        first_name = fake.first_name()
        last_name = fake.last_name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
        email = fake.unique.email()
        #phone = fake.phone_number()
        phone = str(random.randint(6000000000, 9999999999))
        city = fake.city()

        income_band = random.choices(income_bands, weights=income_weights)[0]

        # Risk logic based on income
        if income_band == "LOW":
            risk = random.choices(risk_segments, weights=[0.3, 0.4, 0.3])[0]
        elif income_band == "MID":
            risk = random.choices(risk_segments, weights=[0.5, 0.4, 0.1])[0]
        else:
            risk = random.choices(risk_segments, weights=[0.7, 0.25, 0.05])[0]

        customers.append([
            first_name,
            last_name,
            dob,
            email,
            phone,
            city,
            income_band,
            risk
        ])

    df = pd.DataFrame(customers, columns=[
        "first_name",
        "last_name",
        "dob",
        "email",
        "phone_number",
        "city",
        "income_band",
        "risk_segment"
    ])

    return df