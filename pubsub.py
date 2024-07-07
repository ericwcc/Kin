from confluent_kafka import Producer, KafkaException

class MessagePublisher:
    def __init__(self, configs):
        self._producer = Producer(configs)
    
    def produce(self, topic, key, value):
        try:
            self._producer.poll(0)
            self._producer.produce(topic, value, key, on_delivery=self._ack)
            return True
        except KafkaException as ex:
            print(f'kafka error: {ex.str()}, topic={topic}, key={key}, value={value}')
            return False
    
    def flush(self):
        self._producer.flush()

    def _ack(err, msg):
        if err:
            print(f'kafka: received error callback, error={err.str()}')
        else:
            print(f'kafka: received callback, message={msg.value()}')
    
    def close(self):
        return True
