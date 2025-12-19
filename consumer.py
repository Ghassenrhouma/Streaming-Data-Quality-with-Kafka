import json
import re
from kafka import KafkaConsumer
from collections import defaultdict

def parse_count(count_value):
   
    if count_value is None:
        return None, False
    
    if isinstance(count_value, int):
        return count_value, False
    
    if isinstance(count_value, str):
        count_str = count_value.strip()
        
        try:
            return int(count_str), True
        except ValueError:
            pass
        
        text_numbers = {
            'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4,
            'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9,
            'ten': 10, 'twenty': 20, 'twenty-five': 25, 'thirty': 30,
            'forty': 40, 'fifty': 50, 'sixty': 60, 'sixty-two': 62,
            'seventy': 70, 'eighty': 80, 'ninety': 90,
            'hundred': 100, 'thousand': 1000, 'one thousand': 1000
        }
        
        count_lower = count_str.lower()
        if count_lower in text_numbers:
            return text_numbers[count_lower], True
        
        numbers = re.findall(r'-?\d+', count_str)
        if numbers:
            return int(numbers[0]), True
    
    return None, False


def fix_incomplete_json(line):
   
    line = line.strip()
    
    if not line.startswith('{'):
        return None
    
    open_braces = line.count('{')
    close_braces = line.count('}')
    
    if open_braces > close_braces:
        line += '}' * (open_braces - close_braces)
    
    return line


def consume_and_clean_kafka(topic_name, bootstrap_servers='localhost:9092'):
    
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8'),
        enable_auto_commit=True
    )
    
    print(f"✓ Connected to {topic_name}\n")
    
    stats = {
        'total': 0,
        'valid': 0,
        'corrected': 0,
        'parse_error': 0,
        'unfixable': 0,
        'duplicates': 0
    }
    
    corrections_log = {
        'count_fixed': 0,
        'json_fixed': 0,
        'negative_converted': 0,
        'text_number_converted': 0,
        'null_filled': 0
    }
    
    route_aggregation = defaultdict(int)
    seen_exact = set()
    
    for message in consumer:
        stats['total'] += 1
        raw_data = message.value
        data = None
        was_corrected = False
        
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            fixed_line = fix_incomplete_json(raw_data)
            if fixed_line:
                try:
                    data = json.loads(fixed_line)
                    corrections_log['json_fixed'] += 1
                    was_corrected = True
                    print(f" Fixed JSON: {raw_data[:50]}... → parsed successfully")
                except json.JSONDecodeError:
                    pass
            
            if data is None:
                stats['parse_error'] += 1
                print(f"✗ Unparseable: {raw_data[:60]}...")
                continue
        
        origin = data.get('ORIGIN_COUNTRY_NAME')
        dest = data.get('DEST_COUNTRY_NAME')
        count = data.get('count')
        
        if 'route' in data and isinstance(data['route'], dict):
            origin = data['route'].get('from')
            dest = data['route'].get('to')
            corrections_log['json_fixed'] += 1
            was_corrected = True
            print(f"Alternative structure: route.from/to → standard fields")
        
        try:
            if origin is None or not str(origin).strip():
                stats['unfixable'] += 1
                print(f"✗ Unfixable: origin is null/empty")
                continue
            
            if dest is None or not str(dest).strip():
                stats['unfixable'] += 1
                print(f"✗ Unfixable: dest is null/empty")
                continue
            
            origin_clean = str(origin).strip().title()
            dest_clean = str(dest).strip().title()
            
        except Exception as e:
            stats['unfixable'] += 1
            print(f"✗ String cleaning error: {e}")
            continue
        
        count_clean, count_corrected = parse_count(count)
        
        if count_clean is None:
            if count is None:
                stats['unfixable'] += 1
                print(f"✗ Unfixable: count=None for {origin_clean} → {dest_clean}")
                continue
            else:
                stats['unfixable'] += 1
                print(f"✗ Unfixable: count='{count}' for {origin_clean} → {dest_clean}")
                continue
        
        if count_corrected:
            was_corrected = True
            corrections_log['count_fixed'] += 1
            if isinstance(count, str) and not count.strip().isdigit():
                corrections_log['text_number_converted'] += 1
                print(f" Text→Number: '{count}' → {count_clean}")
        
        if count_clean < 0:
            count_clean = abs(count_clean)
            was_corrected = True
            corrections_log['negative_converted'] += 1
            print(f" Negative→Positive: {origin_clean} → {dest_clean} ({count_clean})")
        
        if count_clean == 0:
            stats['unfixable'] += 1
            print(f"✗ Zero count: {origin_clean} → {dest_clean}")
            continue
        
        exact_key = (origin_clean, dest_clean, count_clean)
        if exact_key in seen_exact:
            stats['duplicates'] += 1
            print(f"⚠ Duplicate: {origin_clean} → {dest_clean} ({count_clean})")
            continue
        seen_exact.add(exact_key)
        
        route_key = (origin_clean, dest_clean)
        route_aggregation[route_key] += count_clean
        
        if was_corrected:
            stats['corrected'] += 1
        stats['valid'] += 1
        
        symbol = "." if was_corrected else "✓"
        print(f"{symbol} {origin_clean} → {dest_clean} (+{count_clean})")
    
    aggregated_records = []
    for (origin, dest), total_count in sorted(route_aggregation.items(),
                                               key=lambda x: x[1],
                                               reverse=True):
        aggregated_records.append({
            'ORIGIN_COUNTRY_NAME': origin,
            'DEST_COUNTRY_NAME': dest,
            'total_count': total_count
        })
    
    with open('flights_aggregated.json', 'w', encoding='utf-8') as f:
        for record in aggregated_records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    
    
    consumer.close()


if __name__ == "__main__":
    consume_and_clean_kafka('flights-raw', 'localhost:9092')