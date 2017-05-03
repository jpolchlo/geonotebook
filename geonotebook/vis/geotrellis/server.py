import io
import numpy as np
import rasterio
import time

from PIL import Image


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

def moop(pyramids):
    from flask import Flask, make_response, abort
    from flask_cors import cross_origin
    from PIL import Image

    app = Flask(__name__)
    app.reader = None

    @app.route('/time')
    def ping():
        return time.strftime("%H:%M:%S")

    @app.route("/<layer_name>/<int:x>/<int:y>/<int:zoom>.png")
    @cross_origin()
    def tile(layer_name, x, y, zoom):

        # fetch data
        try:
            pyramid = pyramids[layer_name]
            rdd = pyramid[zoom]
            tile = rdd.lookup(col=x, row=y)
            arr = tile[0]['data']
        except:
            abort(404)
        bands = arr.shape[0]
        if bands >= 3:
            bands = 3
        else:
            bands = 1
        arrs = [np.array(arr[i, :, :]).reshape(256, 256) for i in range(bands)]

        # create tile
        if bands == 3:
            images = [make_image(clamp(arr)) for arr in arrs]
            images.append(make_image(alpha(arrs[0])))
            image = Image.merge('RGBA', images)
        else:
            gray = make_image(clamp(arrs[0]))
            alfa = make_image(alpha(arrs[0]))
            image = Image.merge('RGBA', list(gray, gray, gray, alfa))

        # return tile
        bio = io.BytesIO()
        image.save(bio, 'PNG')
        response = make_response(bio.getvalue())
        response.headers['Content-Type'] = 'image/png'
        return response

    app.run(host='0.0.0.0', port=8033)
