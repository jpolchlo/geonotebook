import notebook
import os
import requests
import tornado.web

from notebook.base.handlers import IPythonHandler
from notebook.utils import url_path_join as ujoin
# from tornado import gen
# from tornado.web import URLSpec
import threading
import time

from .server import moop


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellisHandler(IPythonHandler):

    def initialize(self):
        pass

    def get(self, rdd, x, y, zoom, **kwargs):
        url = "http://localhost:%d/%s/%s/%s/%s.png" % (8033, rdd, zoom, x, y)
        try:
            response = requests.get(url)
            png = response.content
            self.set_header('Content-Type', 'image/png')
            self.write(png)
            self.finish()
        except:
            self.set_header('Content-Type', 'text/html')
            self.set_status(404)
            self.finish()

class GeoTrellis(object):

    def __init__(self, config, url):
        self.base_url = url

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        pattern = r'/user/[^/]+/geotrellis/([^/]+)/([0-9]+)/([0-9]+)/([0-9]+)\.png.*'
        webapp.add_handlers(r'.*', [(pattern, GeoTrellisHandler)])

    def get_params(self, name, data, **kwargs):
        return {}

    def ingest(self, data, max_zoom, name=None, **kwargs):
        # from geopyspark.geotrellis.catalog import write
        # from geopyspark.geotrellis.constants import SPATIAL, ZOOM
        # from geopyspark.geotrellis.geotiff_rdd import get

        # path = data.uri
        layer_name = format(hash(name) + hash(str(kwargs)), 'x').replace("-", "Z")

        # # GeoTrellis ingest
        # rdd = get(geopysc, SPATIAL, path, maxTileSize=256, numPartitions=64)
        # metadata = rdd.collect_metadata()

        # laid_out = rdd.tile_to_layout(metadata)
        # reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

        # pyramided = reprojected.pyramid(max_zoom, 0)

        # for layer_rdd in pyramided:
        #     write(self.catalog, layer_name, layer_rdd)

        t = threading.Thread(target=moop, args=(42,))
        t.start()

        return self.base_url + "/" + layer_name
