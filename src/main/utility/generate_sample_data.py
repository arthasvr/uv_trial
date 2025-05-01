import csv
from faker import Faker

fake = Faker()

employee_list = []


def create_employees(num_employees):
    for _ in range(0, num_employees):
        employee = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "job": fake.job(),
            "department": fake.random_element(
                elements=("HR", "IT", "Finance", "Marketing", "Sales")
            ),
            "salary": fake.random_int(min=30000, max=100000, step=1000),
        }
        employee_list.append(employee)
    return employee_list


def write_to_csv(num_of_entries):
    with open(
        "F:\\python_projects\\uv_trial\\resources\\data\\employee_data.csv",
        "w",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "first_name",
                "last_name",
                "email",
                "phone_number",
                "job",
                "department",
                "salary",
            ],
        )
        writer.writeheader()
        writer.writerows(create_employees(num_of_entries))
