import requests
import threading
import time

from notebook.base.handlers import IPythonHandler
from .server import moop


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellisHandler(IPythonHandler):

    def initialize(self):
        pass

    def get(self, port, layer_name, x, y, zoom, **kwargs):
        url = "http://localhost:%s/%s/%s/%s/%s.png" % (port, layer_name, x, y, zoom)
        try:
            response = requests.get(url)
            if response.status_code == requests.codes.ok:
                png = response.content
                self.set_header('Content-Type', 'image/png')
                self.write(png)
                self.finish()
            else:
                self.set_header('Content-Type', 'text/png')
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

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        pattern = r'/user/[^/]+/geotrellis/([0-9]+)/([^/]+)/([0-9]+)/([0-9]+)/([0-9]+)\.png.*'
        webapp.add_handlers(r'.*', [(pattern, GeoTrellisHandler)])

    def get_params(self, name, data, **kwargs):
        return {}

    def ingest(self, data, name=None, **kwargs):
        from geopyspark.geotrellis.rdd import RasterRDD, TiledRasterRDD
        from geopyspark.geotrellis.constants import ZOOM

        if isinstance(data, RasterRDD):
            rdd = data
            metadata = rdd.collect_metadata()
            laid_out = rdd.tile_to_layout(metadata)
            reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)
        elif isinstance(data, TiledRasterRDD):
            laid_out = data
            reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

        layer_name = format(hash(name) + hash(str(kwargs)), 'x').replace("-", "Z")
        rdds = {}
        for layer_rdd in reprojected.pyramid(reprojected.zoom_level, 0):
            rdds[layer_rdd.zoom_level] = layer_rdd
        self.pyramids.update({layer_name: rdds})

        if self.server_active == False:
            t = threading.Thread(target=moop, args=(self.pyramids,))
            t.start()
            self.server_active = True

        self.base_url = "http://localhost:8000/user/hadoop/geotrellis" # XXX
        port = 8033
        return self.base_url + "/" + str(port) + "/" + layer_name
