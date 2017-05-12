import io
import logging
import numpy as np
import rasterio
import threading
import sys
import time

from flask import Flask, make_response, abort, request
from PIL import Image

lock = threading.Lock()

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

def set_server_routes(app):
    app.config['PROPAGATE_EXCEPTIONS'] = True

    def shutdown_server():
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    @app.route("/time")
    def ping():
        return time.strftime("%H:%M:%S") + "\n"

    @app.route('/shutdown')
    def shutdown():
        shutdown_server()

def make_tile_server(port, fn):
    '''
    Makes a tile server and starts it on the given port, using a function
    that takes z, x, y as the tile route.
    '''
    app = Flask(__name__)
    # logging.basicConfig(level=logging.DEBUG, format='%(relativeCreated)6d %(threadName)s %(message)s')

    set_server_routes(app)

    @app.route("/tile/<int:z>/<int:x>/<int:y>.png")
    def tile(z, x, y):
        try:
            return fn(z, x, y)
        except Exception as e:
            return make_response("Tile route error: %s" % str(e), 500)

    return app.run(host='0.0.0.0', port=port, threaded=True)

def rdd_server(port, pyramid):
    def tile(z, x, y):
        # logging.debug("---------------DOES THIS SHOW UP")

        # fetch data
        rdd = pyramid[z]
        tile = rdd.lookup(col=x, row=y)

        arr = tile[0]['data']

        if arr == None:
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
        #bio = io.BytesIO(img[0])
        #Image.open(bio)
        response = make_response(img[0])
        response.headers['Content-Type'] = 'image/png'

        return response

    return make_tile_server(port, tile)

def catalog_layer_server(port, value_reader, layer_name, key_type, render_tile):
    def tile(z, x, y):
        tile = value_reader.readTile(key,
                                     layer_name,
                                     layer_zoom,
                                     col,
                                     row,
                                     "")
        arr = tile['data']

        image = render_tile(arr)

        # image = Image.merge('RGBA', rgba)

        # if render_tile:
        #     image = make_image(arr)
        # else:
        #     bands = arr.shape[0]
        #     if bands >= 3:
        #         bands = 3
        #     else:
        #         bands = 1
        #         arrs = [np.array(arr[i, :, :]).reshape(256, 256) for i in range(bands)]

        #         # create tile
        #         if bands == 3:
        #             images = [make_image(clamp(arr)) for arr in arrs]
        #             images.append(make_image(alpha(arrs[0])))
        #             image = Image.merge('RGBA', images)
        #         else:
        #             gray = make_image(clamp(arrs[0]))
        #             alfa = make_image(alpha(arrs[0]))
        #             image = Image.merge('RGBA', list(gray, gray, gray, alfa))
        bio = io.BytesIO()
        image.save(bio, 'PNG')

        # return tile
        response = make_response(bio.getvalue())
        response.headers['Content-Type'] = 'image/png'

        return response

    return make_tile_server(port, tile)
