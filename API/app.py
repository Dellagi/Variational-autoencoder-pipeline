from flask import Flask, request, render_template, jsonify

from flask_socketio import SocketIO, emit
from PIL import Image
from PIL.ExifTags import TAGS
from kafka import KafkaProducer
import os
import base64


app = Flask(__name__)
socketio = SocketIO(app)


@app.route("/image", methods=["POST"])
def train():
    try:
        producer = KafkaProducer(bootstrap_servers=os.environ["kafka_server"])
        content = request.json
        encoded_image = content["base64_encoded_image"]

        base64_decoded = base64.b64decode(encoded_image)
        image = Image.open(io.BytesIO(base64_decoded))

        exifdata = image.getexif()
        kafka_dict = {"base64_encoded_image": encoded_image}

        for tag_id in exifdata:
            tag = TAGS.get(tag_id, tag_id)
            data = exifdata.get(tag_id)
            if isinstance(data, bytes):
                data = data.decode()
            kafka_dict.update({f"{tag}": f"{data}"})

        kafka_dict.update(
            {
                "Filename": image.filename,
                "Image Height": image.height,
                "Image Width": image.width,
                "Image Format": image.format,
                "Image Mode": image.mode,
            }
        )

        producer.send(os.environ["kafka_topic_1"], json.dumps(kafka_dict))
        producer.flush()

        respStruct = {
            "status": "success",
            "message": "The image was processed and publish successfully, Topic: {}".format(
                os.environ["kafka_topic_1"]
            ),
        }
        return make_response(jsonify(respStruct)), 200

    except FileNotFoundError as e:
        return (str(e), 500)


@socketio.on("videofeed")
def image(frame_):

    stream_buffer = StringIO()
    stream_buffer.write(frame_)

    base64_decoded = base64.b64decode(frame_)
    image = Image.open(io.BytesIO(base64_decoded))

    image_BGR = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    image_BGR = imutils.resize(image_BGR, width=700)
    image_BGR = cv2.flip(image_BGR, 1)
    im_jpg = cv2.imencode(".jpg", image_BGR)[1]

    exifdata = im_jpg.getexif()

    im_jpg_b64 = base64.b64encode(im_jpg).decode("utf-8")
    kafka_dict = {"base64_encoded_image": im_jpg_b64}

    for tag_id in exifdata:
        tag = TAGS.get(tag_id, tag_id)
        data = exifdata.get(tag_id)
        if isinstance(data, bytes):
            data = data.decode()
        kafka_dict.update({f"{tag}": f"{data}"})

    kafka_dict.update(
        {
            "Filename": im_jpg.filename,
            "Image Height": im_jpg.height,
            "Image Width": im_jpg.width,
            "Image Format": im_jpg.format,
            "Image Mode": im_jpg.mode,
        }
    )

    producer.send(os.environ["kafka_topic_1"], json.dumps(kafka_dict))
    producer.flush()




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
