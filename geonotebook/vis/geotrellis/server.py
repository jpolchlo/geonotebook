import io
import logging
import numpy as np
import rasterio
import threading
import sys
import time

from flask import Flask, make_response, abort, request
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

    @app.route("/tile/<layer_name>/<int:x>/<int:y>/<int:zoom>.png")
    def tile(layer_name, x, y, zoom):

        # fetch data
        try:
            img = png.lookup(x, y, zoom)
        except:
            img = None

        if img == None or len(img) == 0:
            if png.debug:
                image = Image.new('RGBA', (256,256))
                draw = ImageDraw.Draw(image)
                draw.rectangle([0, 0, 255, 255], outline=(255,0,0,255))
                draw.line([(0,0),(255,255)], fill=(255,0,0,255))
                draw.line([(0,255),(255,0)], fill=(255,0,0,255))
                draw.text((136,122), str(x) + ', ' + str(y) + ', ' + str(zoom), fill=(255,0,0,255))
                del draw
                bio = io.BytesIO()
                image.save(bio, 'PNG')
                img = [bio.getvalue()]
            else:
                abort(404)

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
