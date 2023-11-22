import cv2

import numpy as np



def sub_sampling():

    input_video_path = r"/Users/priyagawli/Documents/Capstone/Video/WC23.mp4"
    output_video_path = r'/Users/priyagawli/Documents/Capstone/Video/new-WC23.mp4'

    cap = cv2.VideoCapture(input_video_path)

    if not cap.isOpened():
        print("Error: Could not open video file.")
        return

    original_fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_video_path, fourcc, original_fps, (frame_width, frame_height))

    frame_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if original_fps == 60:
            if frame_count % 2 == 0:
                new_frame = dehaze(frame)
                print("frame number :" ,frame_count)

        else:
            new_frame = dehaze(frame)
            print("frame number :", frame_count)

        new_frame = dehaze(frame)
        out.write(new_frame)

        frame_count += 1

    cap.release()
    out.release()
    print("Conversion done.")


def dehaze(frame, contrast_factor=0.9, luminance_factor=25):
    input_img = frame

    hsv_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2HSV)
    value = hsv_img[:, :, 2]
    saturation = hsv_img[:, :, 1]

    #Applying the normalized min-max rule to the value channel
    min_value = np.min(value)
    max_value = np.max(value)
    epsilon = 0.001

    normalized_value = (value - min_value) / (max_value - min_value + epsilon)

    a = 1.5
    transformed_value = np.sin((normalized_value - 0.5) * a) * 0.5 + 0.5
    transformed_saturation = saturation *2

    hsv_img[:, :, 2] = (transformed_value * 255).astype(np.uint8)  # Scale to [0, 255]
    hsv_img[:, :, 1] = transformed_saturation 

    output_img = cv2.cvtColor(hsv_img, cv2.COLOR_HSV2BGR)
    return output_img

if __name__ == "__main__":

    sub_sampling()