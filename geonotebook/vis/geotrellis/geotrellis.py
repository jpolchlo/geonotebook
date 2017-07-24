import os
import requests
import threading
import time
import logging

from concurrent.futures import ThreadPoolExecutor
from tornado.httpclient import AsyncHTTPClient
from tornado import concurrent, ioloop
from tornado import gen

from datetime import datetime
from notebook.base.handlers import IPythonHandler
from geonotebook.wrappers.raster import TMSRasterData
from .server import (rdd_server,
                     catalog_layer_server,
                     catalog_multilayer_server)

from .render_methods import render_default_rdd

logger = logging.getLogger('geotrellis-tile-server')
logger.setLevel(10)
# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop/notebooks


class GTAsyncClient(object):
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(
                GTAsyncClient, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=12)
        self.io_loop = ioloop.IOLoop.current()


class GeoTrellisTileHandler(IPythonHandler):

    def initialize(self):
        self.client = GTAsyncClient()

    # This handler uses the order x/y/z for some reason.
    @gen.coroutine
    def get(self, port, x, y, zoom, **kwargs):
        client = AsyncHTTPClient()
        url = "http://localhost:%s/tile/%s/%s/%s.png" % (port, zoom, x, y)
        logger.debug("Handling %s" % (url))
        try:
            response = yield client.fetch(url, raise_error=False, follow_redirects=True)
            logger.debug("TILE REQUEST RETURNED WITH %s" % (response.code))
            if response.code == 200:
                png = response.body
                self.set_header('Content-Type', 'image/png')
                self.write(png)
                self.finish()
            else:
                logger.debug("TILE RESPONSE IS NOT OK!: %s - %s" % (str(response), str(response.body)))
                self.set_header('Content-Type', 'text/html')
                self.set_status(404)
                self.finish()
        except Exception as e:
            logger.debug("Error in {}/{}/{}: {}". format(zoom, x, y, str(e)))
            self.set_header('Content-Type', 'text/html')
            self.write(str(e))
            self.set_status(500)
            self.finish()

class GeoTrellis(object):

    def __init__(self, config, url):
        self.base_url = url

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        pattern = r'/user/[^/]+/geotrellis/([0-9]+)/([0-9]+)/([0-9]+)/([0-9]+)\.png.*'
        webapp.add_handlers(r'.*', [(pattern, GeoTrellisTileHandler)])

    def get_params(self, name, data, **kwargs):
        return {}

    def disgorge(self, name, **kwargs):
        inproc_server_states = kwargs.pop('inproc_server_states', None)
        if inproc_server_states is None:
            raise Exception(
                "GeoTrellis vis server requires kernel_id as kwarg to disgorge!")
        if 'geotrellis' in inproc_server_states:
            if name in inproc_server_states['geotrellis']['ports']:
                port = inproc_server_states['geotrellis']['ports'][name]
                user = os.environ['LOGNAME'] if 'LOGNAME' in os.environ else 'hadoop'
                url = "http://localhost:%s/shutdown" % port
                response = requests.get(url)
                status_code = response.status_code
                inproc_server_states['geotrellis']['ports'].pop(name, None)
                if name in inproc_server_states['geotrellis']['server']:
                    server = inproc_server_states['geotrellis']['server'][name]
                    server.unbind()
                    inproc_server_states['geotrellis']['server'].pop(name, None)
            return None
        return None

    def ingest(self, data, name, **kwargs):
        inproc_server_states = kwargs.pop('inproc_server_states', None)
        if inproc_server_states is None:
            raise Exception(
                "GeoTrellis vis server requires kernel_id as kwarg to ingest!")

        if not "geotrellis" in inproc_server_states:
            inproc_server_states["geotrellis"] = { "ports" : {} , "server" : {}}

        port_coordination = {'handshake': str(datetime.now()) + " " + str(os.getpid()) + " " + str(datetime.now()) + "\n"}

        # TODO: refactor this to different methods?
        if isinstance(data, TMSRasterData):
            tms = data.tms
            server = tms.server
            server.setHandshake(port_coordination['handshake'])
            server.bind("0.0.0.0")
            port_coordination['port'] = server.port()
            inproc_server_states['geotrellis']['server'][name] = server
            print('Added TMS server at host {}'.format(server.host()))
            print('Added TMS server at port {}'.format(server.port()))
        # elif isinstance(data, GeoTrellisCatalogLayerData):
        #     render_tile = kwargs.pop('render_tile', None)
        #     if render_tile is None:
        #         raise Exception("GeoTrellis Layers require render_tile function.")

        #     args = (port_coordination,
        #             data.value_reader,
        #             data.layer_name,
        #             data.key_type,
        #             render_tile)
        #     if isinstance(data.layer_name, list):
        #         t = threading.Thread(target=catalog_multilayer_server, args=args)
        #     else:
        #         t = threading.Thread(target=catalog_layer_server, args=args)
        #     t.start()
        else:
            raise Exception("GeoTrellis vis server cannot handle data of type %s" % (type(data)))

        keep_looping = True
        while keep_looping:
            if 'port' in port_coordination:
                port = port_coordination['port']
                if port > 0:
                    url = "http://localhost:%s/handshake" % port
                    response = requests.get(url)
                    if response.text == port_coordination['handshake']:
                        keep_looping = False
            time.sleep(0.68) # nearly an eternity

        inproc_server_states['geotrellis']['ports'][name] = port_coordination['port']

        user = os.environ['LOGNAME'] if 'LOGNAME' in os.environ else 'hadoop'
        base_url = "/user/%s/geotrellis" % user
        return "%s/%d" % (base_url, port_coordination['port'])
