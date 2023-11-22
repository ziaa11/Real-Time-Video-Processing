# import os
from kafka import KafkaConsumer
import numpy as np
import cv2
import sys

class VideoConsumer:
    def __init__(self, topic, output_path, broker):
        self.kafka_topic = topic
        self.consumer = KafkaConsumer(topic, bootstrap_servers=broker)
        self.output_path = output_path
        self.video_writer = None

    def decode_frame(self, bytes_frame):
        frame = cv2.imdecode(np.frombuffer(bytes_frame, dtype=np.uint8), cv2.IMREAD_COLOR)
        if frame is not None:
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        return frame

    def consume_and_save(self):
        for message in self.consumer:
            frame = self.decode_frame(message.value)

            if self.video_writer is None and frame is not None:
                height, width = frame.shape[:2]
                fourcc = cv2.VideoWriter_fourcc(*'XVID')
                self.video_writer = cv2.VideoWriter(self.output_path, fourcc, 20.0, (width, height), isColor=False)

            if frame is not None:
                self.video_writer.write(frame)

    def release_writer(self):
        if self.video_writer is not None:
            self.video_writer.release()

if __name__ == '__main__':
    topic = sys.argv[1]
    output_path = '/home/zia/Desktop/Cap/output_video.avi'
    kafka_broker = 'localhost:9092'
    consumer = VideoConsumer(topic, output_path, kafka_broker)
    
    try:
        consumer.consume_and_save()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.release_writer()

# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
# import numpy as np
# import cv2

# class VideoProcessor:

#     def decode_frame(self, bytes_frame):
#         frame = cv2.imdecode(np.frombuffer(bytes_frame, dtype=np.uint8), cv2.IMREAD_COLOR)
#         if frame is not None:
#             frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
#         return frame

#     def process(self, value):
#         frame = self.decode_frame(value.encode('utf-8'))
#         return frame

# if __name__ == '__main__':
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(10) 

#     kafka_props = {
#         'bootstrap.servers': 'localhost:9092',
#         'group.id': 'test-group'
#     }

#     topic = sys.argv[1]
#     consumer = FlinkKafkaConsumer(topic, SimpleStringSchema(), properties=kafka_props)

#     video_processor = VideoProcessor()

#     data_stream = env.add_source(consumer)
#     processed_stream = data_stream.map(video_processor.process)

#     env.execute("VideoProcessingJob")
