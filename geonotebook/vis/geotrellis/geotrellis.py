import threading
import time

from .server import moop


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellis(object):

    def __init__(self, config, url):
        self.base_url = 'http://localhost:8033' # XXX tiler thread should report port
        self.pyramids = {}

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        pass

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

        t = threading.Thread(target=moop, args=(self.pyramids,))
        t.start()

        return self.base_url + "/" + layer_name
