import threading
import time

from .server import moop


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellis(object):

    def __init__(self, config, url):
        self.base_url = url
        self.base_url = 'http://localhost:8033'

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

        if isinstance(data, RasterRDD):
            rdd = data
            metadata = rdd.collect_metadata()
            laid_out = rdd.tile_to_layout(metadata)
            reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)
        elif isinstance(data, TiledRasterRDD):
            laid_out = data
            reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

        pyramid = {}
        pyramided = reprojected.pyramid(max_zoom, 0)

        layer_name = format(hash(name) + hash(str(kwargs)), 'x').replace("-", "Z")


        # for layer_rdd in pyramided:
        #     write(self.catalog, layer_name, layer_rdd)

        t = threading.Thread(target=moop, args=(42,))
        t.start()

        return self.base_url + "/" + layer_name
