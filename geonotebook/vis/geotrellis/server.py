import io
import logging
import numpy as np
import os
import rasterio
import threading
import sys
import time
import traceback

from gevent.pywsgi import WSGIServer

from flask import Flask, make_response, abort, request
from PIL import Image

def respond_with_image(image):
    bio = io.BytesIO()
    image.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    return response

    return response

def make_tile_server(port, fn):
    '''
    Makes a tile server and starts it on the given port, using a function
    that takes z, x, y as the tile route.
    '''
    app = Flask(__name__)
    http_server = WSGIServer(('', port), app)

    f = open(os.devnull, "w")
    # sys.stdout = f
    sys.stderr = f

    def shutdown():
        time.sleep(0.5)
        http_server.stop()

    @app.route('/shutdown')
    def shutdown():
        try:
            t = threading.Thread(target=shutdown)
            t.start()
            # Do not return a response, as this causes odd issues.
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    @app.route("/tile/<int:z>/<int:x>/<int:y>.png")
    def tile(z, x, y):
        try:
            return fn(z, x, y)
        except Exception as e:
            return make_response("Tile route error: %s - %s" % (str(e), traceback.format_exc()), 500)

    return http_server.serve_forever()

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
        if not tile:
            abort(404)

        decoder = avroregistry._get_decoder(tile_type)
        encoder = avroregistry._get_encoder(tile_type)

        ser = AvroSerializer(tile._2(), decoder, encoder)
        arr = ser.loads(tile._1())[0]['data']
        image = render_tile(arr)

        return respond_with_image(image)

    return make_tile_server(port, tile)

def catalog_multilayer_server(port, value_reader, layer_names, key_type, tile_type, avroregistry, render_tile):
    from geopyspark.avroserializer import AvroSerializer

    def tile(z, x, y):
        tiles = []
        decoder = avroregistry._get_decoder(tile_type)
        encoder = avroregistry._get_encoder(tile_type)

        for layer_name in layer_names:
            value = value_reader.readTile(key_type,
                                          layer_name,
                                          z,
                                          x,
                                          y,
                                          "")
            if not value:
                abort(404)

            ser = AvroSerializer(value._2(), decoder, encoder)
            tile = ser.loads(value._1())[0]['data']
            tiles.append(tile)

        image = render_tile(tiles)

        return respond_with_image(image)

    return make_tile_server(port, tile)

def png_layer_server(port, png):
    def tile(z, x, y):

        # fetch data
        try:
            img = png.lookup(x, y, z)
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
