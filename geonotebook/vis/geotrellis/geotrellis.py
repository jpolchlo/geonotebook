import requests
import threading
import time

from notebook.base.handlers import IPythonHandler
from random import randint
from .server import moop


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellisHandler(IPythonHandler):

    def initialize(self):
        pass

    def get(self, port, layer_name, x, y, zoom, **kwargs):
        url = "http://localhost:%s/tile/%s/%s/%s/%s.png" % (port, layer_name, x, y, zoom)
        try:
            response = requests.get(url)
            if response.status_code == requests.codes.ok:
                png = response.content
                self.set_header('Content-Type', 'image/png')
                self.write(png)
                self.finish()
            else:
                self.set_header('Content-Type', 'text/html')
                self.set_status(404)
                self.finish()
        except:
            self.set_header('Content-Type', 'text/png')
            self.set_status(404)
            self.finish()

class GeoTrellis(object):

    def __init__(self, config, url):
        self.base_url = url
        self.pyramids = {}
        self.server_active = False
        self.port = randint(49152, 65535) # XXX

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        pattern = r'/user/[^/]+/geotrellis/([0-9]+)/([^/]+)/([0-9]+)/([0-9]+)/([0-9]+)\.png.*'
        webapp.add_handlers(r'.*', [(pattern, GeoTrellisHandler)])

    def get_params(self, name, data, **kwargs):
        return {}

    def disgorge(self, name):
        url = "http://localhost:%s/remove/%s" % (self.port, name)
        response = requests.get(url)
        status_code = response.status_code
        print(status_code)
        return status_code

    def ingest(self, data, name, **kwargs):
        from geopyspark.geotrellis.rdd import RasterRDD, TiledRasterRDD
        from geopyspark.geotrellis.render import PngRDD
        from geopyspark.geotrellis.constants import ZOOM

        rdd = data.rdd
        if isinstance(rdd, RasterRDD):
            metadata = rdd.collect_metadata()
            laid_out = rdd.tile_to_layout(metadata)
            # reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)
            png = PngRDD.makePyramid(laid_out, data.rampname)
        elif isinstance(rdd, TiledRasterRDD):
            laid_out = rdd
            # reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)
            png = PngRDD.makePyramid(laid_out, data.rampname)
        elif isinstance(rdd, PngRDD):
            png = rdd
        else:
            raise Exception

        rdds = {}
        for layer_rdd in reprojected.pyramid(reprojected.zoom_level, 0):
            rdds[layer_rdd.zoom_level] = layer_rdd
        # self.pyramids.update({name: rdds})
        self.pyramids[name] = rdds

        if self.server_active == False:
            t = threading.Thread(target=moop, args=(self.pyramids, self.port))
            t.start()
            self.server_active = True

        self.base_url = "http://localhost:8000/user/hadoop/geotrellis" # XXX
        return self.base_url + "/" + str(self.port) + "/" + name
