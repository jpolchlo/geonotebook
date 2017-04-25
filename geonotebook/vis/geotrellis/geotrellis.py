import notebook
import os
import tornado.web

from notebook.base.handlers import IPythonHandler
from notebook.utils import url_path_join as ujoin
from tornado import gen
from tornado.web import URLSpec


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellis(object):

    def __init__(self, config, catalog, url):
        self.base_url = url
        self.catalog = catalog

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        pass

    def get_params(self, name, data, **kwargs):
        return {}

    def ingest(self, data, geopysc, max_zoom, name=None, **kwargs):
        from geopyspark.geotrellis.catalog import write
        from geopyspark.geotrellis.constants import SPATIAL, ZOOM
        from geopyspark.geotrellis.geotiff_rdd import get

        path = data.uri
        layer_name = format(hash(name) + hash(str(kwargs)), 'x').replace("-", "Z")

        # GeoTrellis ingest
        rdd = get(geopysc, SPATIAL, path, maxTileSize=256, numPartitions=64)
        metadata = rdd.collect_metadata()

        laid_out = rdd.tile_to_layout(metadata)
        reprojected = rdd.reproject("EPSG:3857", sheme=ZOOM)

        pyramided = reprojected.pyramid(max_zoom, 0)

        for layer_rdd in pyramided:
            write(self.catalog, layer_name, layer_rdd)

        return self.base_url + "/" + layer_name
