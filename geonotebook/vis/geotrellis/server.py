import io
import logging
import numpy as np
import rasterio
import threading
import sys
import time
import traceback

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

from flask import Flask, make_response, abort, request
from PIL import Image

def respond_with_image(image):
    bio = io.BytesIO()
    image.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'

    return response

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

    set_server_routes(app)

    @app.route("/tile/<int:z>/<int:x>/<int:y>.png")
    def tile(z, x, y):
        try:
            return fn(z, x, y)
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    # return app.run(host='0.0.0.0', port=port, threaded=True)
    return app.run(host='0.0.0.0', port=port)

def rdd_server(port, pyramid, render_tile):
    def tile(z, x, y):

        # fetch data
        rdd = pyramid[z]
        tile = rdd.lookup(col=x, row=y)

        arr = tile[0]['data']

        if arr == None:
            abort(404)

        image = render_tile(arr)

        return respond_with_image(image)

    return make_tile_server(port, tile)

def catalog_layer_server(port, value_reader, layer_name, key_type, tile_type, avroregistry, render_tile):
    from geopyspark.avroserializer import AvroSerializer

    def tile(z, x, y):
        tile = value_reader.readTile(key_type,
                                     layer_name,
                                     z,
                                     x,
                                     y,
                                     "")
        decoder = avroregistry._get_decoder(tile_type)
        encoder = avroregistry._get_encoder(tile_type)

        ser = AvroSerializer(tile._2(), decoder, encoder)
        arr = ser.loads(tile._1())[0]['data']
        image = render_tile(arr)

        return respond_with_image(image)

    return make_tile_server(port, tile)
