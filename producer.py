import json
from kafka import KafkaProducer

def produce_raw_flights(input_file, topic_name, bootstrap_servers='localhost:9092'):
    """
    Read JSON file line by line and send directly to Kafka
    No validation, no error handling - just read and send
    """
    
    print(f"📖 Reading: {input_file}")
    print(f"📤 Producing to: {topic_name}\n")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    sent_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            
            if not line:
                continue
            
            try:
                data = json.loads(line)
                
                producer.send(topic_name, value=data)
                sent_count += 1
                
                if sent_count % 100 == 0:
                    print(f"✓ Sent {sent_count} messages...")
                    
            except json.JSONDecodeError:
                continue
    
    producer.flush()
    producer.close()
    
    print(f"\n✓ Total sent: {sent_count} messages")


if __name__ == "__main__":
    INPUT_FILE = 'flights_summary.json'      
    TOPIC_NAME = 'my-first-topic'               
    KAFKA_SERVER = 'localhost:9092'        
    
    produce_raw_flights(INPUT_FILE, TOPIC_NAME, KAFKA_SERVER)