import pika
import psycopg2
from psycopg2 import Error
from influxdb import InfluxDBClient

def fetch_switches():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",  # Database name
            user="postgres",    # PostgreSQL username
            password="password",  # PostgreSQL password
            host="localhost"
        )
        cursor = conn.cursor()
        # Fetch switch data from the switches table
        cursor.execute("SELECT id, name, ip, status FROM switches LIMIT 5")  # Limit to 5 switches
        switches = cursor.fetchall()
        conn.close()
        return switches
    except Error as e:
        print(f"Error fetching switches: {e}")
        return []

def callback(ch, method, properties, body):
    switch_id = body.decode('utf-8')
    temperature = get_temperature(switch_id)
    write_to_influxdb(switch_id, temperature)

def get_temperature(switch_id):
    command = f"snmpget -v 2c -c public 192.168.1.100 1.3.6.1.4.1.9.9.13.1.3.1.3"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # Extract temperature from the result
    temperature = result.stdout.strip().split()[-1]
    return temperature

def write_to_influxdb(switch_id, temperature):
    client = InfluxDBClient(host='localhost', port=8086, username='admin', password='password', database='temperature_db')
    data = [
        {
            "measurement": "temperature",
            "tags": {
                "switch_id": switch_id
            },
            "fields": {
                "value": float(temperature)
            }
        }
    ]
    client.write_points(data)
    print(f"Temperature data written to InfluxDB for switch: {switch_id}")

# Fetch switch data from PostgreSQL
switches = fetch_switches()
if not switches:
    print("No switches found in the database.")
    exit()

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=False)

# Send switch IDs to the task queue
for switch in switches:
    switch_ip = switch[2]  # Assuming IP address is in the third column
    channel.basic_publish(exchange='', routing_key='task_queue', body=switch_ip)
    print(f"Switch IP {switch_ip} added to task queue.")

connection.close()
