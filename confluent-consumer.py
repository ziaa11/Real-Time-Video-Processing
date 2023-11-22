from confluent_kafka import Consumer, KafkaException
import cv2
import numpy as np
import time

def process_frame(frame):

    input_img = frame
    hsv_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2HSV)
    value = hsv_img[:, :, 2]
    saturation = hsv_img[:, :, 1]

    min_value = np.min(value)
    max_value = np.max(value)
    epsilon = 0.001

    normalized_value = (value - min_value) / (max_value - min_value + epsilon)

    a = 1.5
    transformed_value = np.sin((normalized_value - 0.5) * a) * 0.5 + 0.5
    transformed_saturation = saturation *2

    hsv_img[:, :, 2] = (transformed_value * 255).astype(np.uint8) 
    hsv_img[:, :, 1] = transformed_saturation 

    output_img = cv2.cvtColor(hsv_img, cv2.COLOR_HSV2BGR)
    return output_img

def consume_frames(consumer_group_id, topic):
    conf = {
        'bootstrap.servers': 'your_bootstrap_servers',
        'group.id': consumer_group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print("End of partition reached")
                    break
                else:
                    print(msg.error())
                    continue

            frame = np.frombuffer(msg.value(), dtype=np.uint8)
            frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)

            processed_frame = process_frame(frame)

            cv2.imshow('Processed Frame', processed_frame)
            cv2.waitKey(1)

            # time.sleep(0.1)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_frames('defog1', 'video')
