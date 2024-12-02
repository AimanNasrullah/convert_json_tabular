import json
import os
from faker import Faker

fake = Faker()

def generate_json_files(output_dir, num_files, records_per_file):
    os.makedirs(output_dir, exist_ok=True)
    for i in range(num_files):
        data = []
        for _ in range(records_per_file):
            record = {
                "id": fake.uuid4(),
                "name": fake.name(),
                "email": fake.email(),
                "address": fake.address(),
                "city": fake.city(),
                "state": fake.state(),
                "country": fake.country(),
                "zip": fake.zipcode(),
                "phone": fake.phone_number(),
                "created_at": fake.date_time().isoformat()
            }
            data.append(record)
        file_path = os.path.join(output_dir, f"data_{i + 1}.json")
        with open(file_path, "w") as f:
            json.dump(data, f)
    print(f"Generated {num_files} JSON files in '{output_dir}'.")

generate_json_files("json_files", num_files=1000, records_per_file=1000)
