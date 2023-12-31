from faker import Faker
import random
import pymysql

fake = Faker()

db = pymysql.connect(host='localhost', user='kitkat', password='123', database='DATN')
cursor = db.cursor()

car_brands = ['Toyota', 'Honda', 'Ford', 'BMW', 'Mercedes-Benz', 'Audi', 'Lexus', 'Volkswagen', 'Hyundai', 'Chevrolet']
car_models = ['Corolla', 'Accord', 'F-150', '3 Series', 'E-Class', 'A4', 'RX', 'Golf', 'Elantra', 'Cruze']

# Tạo dữ liệu cho bảng Customers
for _ in range(10000):
    customer_name = fake.name()
    address = fake.address()
    email = fake.email()
    phone_number = fake.phone_number()[:20]

    insert_customer_query = f"INSERT INTO Customers (customer_name, address, email, phone_number) VALUES ('{customer_name}', '{address}', '{email}', '{phone_number}')"
    cursor.execute(insert_customer_query)

# Tạo dữ liệu cho bảng CarModels
for i in range(10000):
    i1 = random.randint(1, 1000)
    i2 = random.randint(1000, 2000)
    model_name = f"{random.choice(car_brands)} {random.choice(car_models)}"
    description = fake.text()
    price = round(random.uniform(10000, 100000), 2)
    specifications = fake.text()

    insert_car_model_query = f"INSERT INTO CarModels (model_name, description, price, specifications) VALUES ('{model_name}', '{description}', {price}, '{specifications}')"
    cursor.execute(insert_car_model_query)

# Tạo dữ liệu cho bảng Suppliers
for _ in range(10000):
    supplier_name = fake.company()
    address = fake.address()
    contact_person = fake.name()
    phone_number = fake.phone_number()[:20]

    insert_supplier_query = f"INSERT INTO Suppliers (supplier_name, address, contact_person, phone_number) VALUES ('{supplier_name}', '{address}', '{contact_person}', '{phone_number}')"
    cursor.execute(insert_supplier_query)

# Lấy danh sách customer_id và model_id
valid_customer_ids_query = "SELECT id FROM Customers"
cursor.execute(valid_customer_ids_query)
valid_customer_ids = [row[0] for row in cursor.fetchall()]

available_model_ids_query = "SELECT id FROM CarModels"
cursor.execute(available_model_ids_query)
available_model_ids = [row[0] for row in cursor.fetchall()]

# Tạo dữ liệu cho bảng Transactions
for _ in range(100000):
    customer_id = random.choice(valid_customer_ids)
    model_id = random.choice(available_model_ids)
    amount = round(random.uniform(100, 1000), 2)
    payment_method = random.choice(['Cash', 'Credit Card', 'Debit Card'])
    transaction_type = random.choice(['purchase', 'service'])

    insert_transaction_query = f"INSERT INTO Transactions (customer_id, model_id, amount, payment_method, transaction_type) VALUES ({customer_id}, {model_id}, {amount}, '{payment_method}', '{transaction_type}')"
    cursor.execute(insert_transaction_query)

# Tạo dữ liệu cho bảng Reviews và ServiceHistory
for customer_id in valid_customer_ids:
    if random.choice([True, False]):
        model_id = random.choice(available_model_ids)
        rating = random.randint(1, 5)
        comment = fake.text()

        insert_review_query = f"INSERT INTO Reviews (customer_id, model_id, rating, comment) VALUES ({customer_id}, {model_id}, {rating}, '{comment}')"
        cursor.execute(insert_review_query)

    if random.choice([True, False]):
        model_id = random.choice(available_model_ids)
        service_date = fake.date_this_year(before_today=True)
        description = fake.text()
        cost = round(random.uniform(50, 500), 2)

        insert_service_history_query = f"INSERT INTO ServiceHistory (model_id, service_date, description, cost) VALUES ({model_id}, '{service_date}', '{description}', {cost})"
        cursor.execute(insert_service_history_query)

# Tạo dữ liệu cho bảng Employees
positions = ['Manager', 'Salesperson', 'Technician', 'Administrator', 'Accountant']
for _ in range(100):
    employee_name = fake.name()
    position = random.choice(positions)
    hire_date = fake.date_this_century()
    contact_number = fake.phone_number()[:20]
    email = fake.email()
    address = fake.address()

    insert_employee_query = f"INSERT INTO Employees (employee_name, position, hire_date, contact_number, email, address) VALUES ('{employee_name}', '{position}', '{hire_date}', '{contact_number}', '{email}', '{address}')"
    cursor.execute(insert_employee_query)

db.commit()
db.close()