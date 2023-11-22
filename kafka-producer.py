from kafka import KafkaProducer
import cv2
import numpy as np
import sys

class VideoProducer:
    def __init__(self, path, topic, broker):
        self.path = path
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: v.tobytes())
        self.frame_count = 0

    def encode_frame(self, frame):
        _, encoded_frame = cv2.imencode('.jpg', frame)
        return encoded_frame

    def produce(self):
        cap = cv2.VideoCapture(self.path)
        try:
            while cap.isOpened():
                ret, frame = cap.read()
                if ret:
                    self.frame_count += 1
                    if self.frame_count%2 == 0 and cap.get(cv2.CAP_PROP_FPS) >= 60:
                        encoded_frame = self.encode_frame(frame)
                        self.producer.send(self.topic, value=encoded_frame)
                else:
                    break
        except Exception as e:
            print(f"Error: {e}")
        finally:
            cap.release()
            self.producer.close()

if __name__ == '__main__':
    video_path = '/home/zia/Desktop/Cap/WC23.mp4'
    topic = sys.argv[1]
    broker = 'localhost:9092'

    producer = VideoProducer(video_path, topic, broker)
    producer.produce()


