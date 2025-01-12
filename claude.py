import socket
import struct
import json
from kafka import KafkaProducer

# Required: pip install kafka-python pyfixbuf

def parse_ipfix_packet(packet):
    """
    Parse an IPFIX packet and return the flow data as a dictionary.
    Modify this function based on the IPFIX format used.
    """
    # Assuming IPFIX header size of 16 bytes (standard format)
    ipfix_header = packet[:16]
    version, length, export_time, sequence, domain_id = struct.unpack('!HHIII', ipfix_header)

    # Example: Parse additional flow data (modify as per your IPFIX template)
    flows = []
    data_offset = 16  # Start after the header
    while data_offset < len(packet):
        try:
            # Example parsing: source IP, destination IP, bytes, packets
            src_ip, dst_ip, bytes_count, packets_count = struct.unpack('!4s4sII', packet[data_offset:data_offset + 16])
            flows.append({
                "source_ip": socket.inet_ntoa(src_ip),
                "destination_ip": socket.inet_ntoa(dst_ip),
                "bytes": bytes_count,
                "packets": packets_count
            })
            data_offset += 16  # Move to next flow record
        except struct.error:
            # Handle incomplete data
            break
    return {
        "header": {
            "version": version,
            "length": length,
            "export_time": export_time,
            "sequence": sequence,
            "domain_id": domain_id
        },
        "flows": flows
    }

def modify_flow_data(flow_data):
    """
    Modify IPFIX flow data before sending it to Kafka.
    """
    for flow in flow_data["flows"]:
        # Example modification: Add a custom field
        flow["custom_field"] = "processed"
    return flow_data

def send_to_kafka(producer, topic, flow_data):
    """
    Send the modified flow data to a Kafka topic.
    """
    try:
        producer.send(topic, json.dumps(flow_data).encode('utf-8'))
        print(f"Sent to Kafka topic {topic}: {flow_data}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

def main():
    # Kafka producer setup
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    kafka_topic = 'ipfix_topic'
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # TCP socket setup
    tcp_port = 9995
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('0.0.0.0', tcp_port))
    sock.listen(5)

    print(f"Listening for IPFIX packets on TCP port {tcp_port}...")

    try:
        while True:
            client_socket, client_address = sock.accept()
            print(f"Connection received from {client_address}")
            with client_socket:
                while True:
                    data = client_socket.recv(4096)
                    if not data:
                        break
                    print(f"Received {len(data)} bytes")

                    # Parse the IPFIX packet
                    flow_data = parse_ipfix_packet(data)

                    # Modify the flow data
                    modified_flow_data = modify_flow_data(flow_data)

                    # Send the modified flow data to Kafka
                    send_to_kafka(producer, kafka_topic, modified_flow_data)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        sock.close()
        producer.close()

if __name__ == "__main__":
    main()
