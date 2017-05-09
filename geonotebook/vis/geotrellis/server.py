import io
import numpy as np
import rasterio
import threading
import time

from PIL import Image, ImageDraw, ImageFont


def make_image(arr):
    return Image.fromarray(arr.astype('uint8')).convert('L')

def clamp(x):
    if (x < 0.0):
        x = 0
    elif (x >= 1.0):
        x = 255
    else:
        x = (int)(x * 255)
    return x

def alpha(x):
    if ((x <= 0.0) or (x > 1.0)):
        return 0
    else:
        return 255

clamp = np.vectorize(clamp)
alpha = np.vectorize(alpha)
lock = threading.Lock()

def moop(png, port):
    from flask import Flask, make_response, abort

    app = Flask(__name__)
    app.reader = None

    @app.route("/time")
    def ping():
        return time.strftime("%H:%M:%S") + "\n"


    # @app.route("/exists/<layer_name>")
    # def exists(layer_name):
    #     return str(layer_name in pyramids) + "\n"

    # @app.route("/remove/<layer_name>")
    # def remove(layer_name):
    #     if layer_name in pyramids:
    #         del pyramids[layer_name]
    #         return "yes\n"
    #     else:
    #         return "no\n"

    @app.route("/tile/<layer_name>/<int:x>/<int:y>/<int:zoom>.png")
    def tile(layer_name, x, y, zoom):

        # fetch data
        try:
            # pyramid = pyramids[layer_name]
            # rdd = pyramid[zoom]
            img = png.lookup(x, y, zoom)
            # arr = tile[0]['data']
            # nodata = -32768 #tile[0]['no_data_value']
        except:
            img = None

        if img == None or len(img) == 0:
            # abort(404)
            image = Image.new('RGBA', (256,256))
            draw = ImageDraw.Draw(image)
            draw.rectangle([0, 0, 255, 255], outline=(255,0,0,255))
            draw.line([(0,0),(255,255)], fill=(255,0,0,255))
            draw.line([(0,255),(255,0)], fill=(255,0,0,255))
            draw.text((136,122), str(13) + ', ' + str(12), fill=(255,0,0,255))
            del draw
            bio = io.BytesIO()
            image.save(bio, 'PNG')
            img = [bio.getvalue()]

        # bands = arr.shape[0]
        # if bands >= 3:
        #     bands = 3
        # else:
        #     bands = 1
        # arrs = [np.array(arr[i, :, :]).reshape(256, 256) for i in range(bands)]

        # # create tile
        # if bands == 3:
        #     images = [make_image(clamp(remap(arr))) for arr in arrs]
        #     alfa = alpha(arrs[0])
        #     # alfa[arrs[0] == nodata] = 0
        #     images.append(make_image(alfa))
        #     image = Image.merge('RGBA', images)
        # else:
        #     gray = make_image(clamp(remap(arrs[0])))
        #     alfa = alpha(arrs[0])
        #     # alfa[arrs[0] == nodata] = 0
        #     image = Image.merge('RGBA', list(gray, gray, gray, make_image(alfa)))

        # return tile
        #bio = io.BytesIO(img[0])
        #Image.open(bio)
        response = make_response(img[0])
        response.headers['Content-Type'] = 'image/png'
        return response

    app.run(host='0.0.0.0', port=port)
