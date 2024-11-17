'''
PART I: IMPORTING LIBRARIES
'''
# libraries for Airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# general libraries
import json
import random
import time
import logging
from datetime import timedelta

# faker libraries
from faker import Faker

# kafka libraries
from kafka import KafkaProducer

'''
PART II: AIRFLOW CONFIGURATION
'''

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# define the default arguments
default_args = {
    'owner': 'HuyNgo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG
dag = DAG(
    'pipeline_real_time_data_streaming',
    default_args=default_args,
    description='A pipeline that generates fake data and streams it to Kafka',
    schedule_interval='@daily',
    start_date=days_ago(0), 
    catchup=False,                        
    is_paused_upon_creation=False       
)                  

'''
PART III: AIRFLOW PIPELINE
'''

# set up faker
fake = Faker()

# define a function to generate fake customer data
customers = []

def generate_customer():
    customer = {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "location": fake.address(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "account_created": fake.past_date().isoformat(),
        "last_login": fake.date_time_this_month().isoformat()
    }
    customers.append(customer["customer_id"])
    return customer

# define a function to generate fake product data
products = []

def generate_product():
    categories = ['Electronics', 'Books', 'Clothing', 'Home & Garden']
    product = {
        "product_id": fake.uuid4(),
        "name": fake.word().title(),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 500), 2),
        "stock_quantity": random.randint(0, 100),
        "supplier": fake.company(),
        "rating": round(random.uniform(1, 5), 1)
    }
    products.append(product["product_id"])
    return product

# define a function to generate fake transaction data
def generate_transaction():
    customer_id = random.choice(customers)
    product_id = random.choice(products)
    return {
        "transaction_id": fake.uuid4(),
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "date_time": fake.date_time_this_year().isoformat(),
        "status": random.choice(["completed", "pending", "canceled"]),
        "payment_method": random.choice(["credit card", "PayPal", "bank transfer"])
    }
    
# define a function to generate fake product view data
def generate_product_view():
    return {
        "view_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "view_duration": random.randint(10, 300) 
    }
    
# define a function to generate fake system log data
def generate_system_log():
    log_levels = ["INFO", "WARNING", "ERROR"]
    return {
        "log_id": fake.uuid4(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "level": random.choice(log_levels),
        "message": fake.sentence()
    }
    
# define a function to generate fake user interaction data
def generate_user_interaction():
    interaction_types = ["wishlist_addition", "review", "rating"]
    return {
        "interaction_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "interaction_type": random.choice(interaction_types),
        "details": fake.sentence() if interaction_types == "review" else None
    }
    
# define a function to send data to Kafka with error handling and logging
def stream_data(batch_size=100):
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka1:29092', 'kafka2:29094'],
            max_block_ms=5000
        )
        logger.info("Kafka producer initialized successfully with multiple brokers.")
    except Exception as e:
        logger.error("Failed to initialize Kafka producer: %s", str(e))
        return None

    for i in range(batch_size):
        try:
            # decide whether to generate a customer or product
            if random.random() < 0.5:
                customer = generate_customer()
                producer.send('customers', json.dumps(customer).encode())
                logger.info("Sent customer data to Kafka: %s", customer)
            else:
                product = generate_product()
                producer.send('products', json.dumps(product).encode())
                logger.info("Sent product data to Kafka: %s", product)

            # generate and send transaction, product view, and user interaction data if lists are populated
            if customers and products:
                transaction = generate_transaction()
                producer.send('transactions', json.dumps(transaction).encode())
                logger.info("Sent transaction data to Kafka: %s", transaction)

                product_view = generate_product_view()
                producer.send('product_views', json.dumps(product_view).encode())
                logger.info("Sent product view data to Kafka: %s", product_view)

                user_interaction = generate_user_interaction()
                producer.send('user_interactions', json.dumps(user_interaction).encode())
                logger.info("Sent user interaction data to Kafka: %s", user_interaction)

            # Generate and send system log data
            system_log = generate_system_log()
            producer.send('system_logs', json.dumps(system_log).encode())
            logger.info("Sent system log data to Kafka: %s", system_log)

            # Random sleep to simulate variable data generation intervals
            time.sleep(random.uniform(0.01, 0.1))
        
        except Exception as e:
            # Log any errors encountered during data generation or sending
            logger.error("Error occurred while generating or sending data: %s", str(e))

    try:
        producer.close()
        logger.info("Kafka producer closed successfully.")
    except Exception as e:
        # Log if there was an issue closing the producer
        logger.error("Failed to close Kafka producer: %s", str(e))
            
'''
PART IV: DEFINE TASKS
'''
    
stream_data_task = PythonOperator(
    task_id='stream_data',
    python_callable=stream_data,
    dag=dag,
)

'''
EXPLANATION:
'''

# The DAG is scheduled to be run once per day (schedule_interval='@daily'). This means that the function stream_data() will also be executed once per day.  
# Every time the function stream_data() is executed, it generates and sends data to Kafka. 

# bootstrap_servers: this parameter specifies the address of the Kafka broker(s), which is where the data will be sent. In this case, 'kafka1:29092' is used, 
#                    which means the producer will connect to the Kafka broker 1 running at the hostname 'kafka1' on port '29092'. This address allows the Kafka client to locate and 
#                    connect to the Kafka cluster, allowing it to publish messages to the desired topics. 
# max_block_ms: this parameter sets the maximum amount of time (in miliseconds) that the producer will block while trying to send a message to Kafka. If the producer
#               cannot send the data within this time due to network or server issues, an exception is raised. In this case, setting 'max_block_ms=5000' means that if the producer 
#               cannot connect to Kafka within 5 seconds, it will timeout. This helps prevent the producer from hanging indefinitely in case of connectivity issues.

# A batch prefers to a specific set of messages that are created and sent to Kafka in a single function call. By setting 'batch_size=100', the function stream_data() will generate
# and send 100 data points (or messages) and then exit. 

# In Kafka, a 'message' is an individual piece of data sent to a Kafka topic. In this case, each customer, product, transaction, etc. that is sent to Kafka is a single message.
# Kafka organizes messages into topics which act as categories for organizing messages by type. 