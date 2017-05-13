import requests
import threading
import multiprocessing
import time

from notebook.base.handlers import IPythonHandler
from geonotebook.wrappers import (RddRasterData,
                                  GeoTrellisCatalogLayerData)
from random import randint
from .server import (rdd_server,
                     catalog_layer_server)

from .render_methods import render_default_rdd

# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellisTileHandler(IPythonHandler):

    def initialize(self):
        pass

    # This handler uses the order x/y/z for some reason.
    def get(self, port, x, y, zoom, **kwargs):
        url = "http://localhost:%s/tile/%s/%s/%s.png" % (port, zoom, x, y)
        print(url)
        try:
            response = requests.get(url)
            print("RETURNED WITH %s" % (response.status_code))
            if response.status_code == requests.codes.ok:
                png = response.content
                self.set_header('Content-Type', 'image/png')
                self.write(png)
                self.finish()
            else:
                print("NOT OK!: %s" % str(response))
                print("NOT OK!: %s" % str(response.content))
                self.set_header('Content-Type', 'text/html')
                self.set_status(404)
                self.finish()
        except Exception as e:
            self.set_header('Content-Type', 'text/html')
            self.write(str(e))
            self.set_status(500)
            self.finish()

class GeoTrellisShutdownHandler(IPythonHandler):

    def initialize(self):
        pass

    def get(self, port):
        url = "http://localhost:%s/shutdown" % port
        # try:
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            png = response.content
            self.set_header('Content-Type', 'image/png')
            self.write(png)
            self.finish()
        else:
            self.set_header('Content-Type', 'text/html')
            self.write(str(response.content))
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
        pattern = r'/user/[^/]+/geotrellis/([0-9]+)/shutdown*'
        webapp.add_handlers(r'.*', [(pattern, GeoTrellisShutdownHandler)])

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
                url = "http://localhost:8000/user/hadoop/geotrellis/%s/shutdown" % port
                response = requests.get(url)
                status_code = response.status_code
                inproc_server_states['geotrellis']['ports'].pop(name, None)
                return status_code
            return None
        return None

    def ingest(self, data, name, **kwargs):
        from geopyspark.geotrellis.rdd import RasterRDD, TiledRasterRDD
        from geopyspark.geotrellis.constants import ZOOM

        inproc_server_states = kwargs.pop('inproc_server_states', None)
        if inproc_server_states is None:
            raise Exception(
                "GeoTrellis vis server requires kernel_id as kwarg to ingest!")

        if not "geotrellis" in inproc_server_states:
            inproc_server_states["geotrellis"] = { "ports" : {} }

        server_port = randint(49152, 65535)
        while server_port in inproc_server_states['geotrellis']['ports'].values():
            server_port = randint(49152, 65535)

        # TODO: refactor this to different methods?
        if isinstance(data, RddRasterData):
            rdd = data.rdd
            if isinstance(rdd, RasterRDD):
                metadata = rdd.collect_metadata()
                laid_out = rdd.tile_to_layout(metadata)
                reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)
            elif isinstance(rdd, TiledRasterRDD):
                laid_out = rdd
                reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)
            else:
                raise Exception("RddRasterData data must be an RDD, found %s" % (type(data)))

            render_tile = kwargs.pop('render_tile', None)
            if render_tile is None:
                render_tile = render_default_rdd

            rdds = {}
            for layer_rdd in reprojected.pyramid(reprojected.zoom_level, 0):
                rdds[layer_rdd.zoom_level] = layer_rdd

            args = (server_port, rdds, render_tile)
            t = threading.Thread(target=rdd_server, args=args)
            t.start()
        elif isinstance(data, GeoTrellisCatalogLayerData):
            render_tile = kwargs.pop('render_tile', None)
            if render_tile is None:
                raise Exception("GeoTrellis Layers require render_tile function.")
            args = (server_port,
                    data.value_reader,
                    data.layer_name,
                    data.key_type,
                    data.tile_type,
                    data.avroregistry,
                    render_tile)
            p = multiprocessing.Process(target=catalog_layer_server, args=args)
            p.start()
        else:
            raise TypeError("Expected a RasterRDD, TiledRasterRDD, or PngRDD")

        t = threading.Thread(target=moop, args=(png, self.port))
        t.start()

        self.base_url = "http://localhost:8000/user/hadoop/geotrellis" # XXX
        return self.base_url + "/" + str(self.port) + "/" + name
