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
        from geopyspark.geotrellis.constants import SPATIAL
        from geopyspark.geotrellis.geotiff_rdd import geotiff_rdd
        from geopyspark.geotrellis.tile_layer import collect_pyramid_metadata, tile_to_layout, pyramid, reproject

        path = data.uri
        layer_name = format(hash(name) + hash(str(kwargs)), 'x').replace("-", "Z")

        # GeoTrellis ingest
        rdd = geotiff_rdd(geopysc, SPATIAL, path, maxTileSize=256, numPartitions=64)
        reprojected = rdd #reproject(geopysc, rdd, "EPSG:3857")
        (_, metadata) = collect_pyramid_metadata(geopysc,
                                                 SPATIAL,
                                                 reprojected,
                                                 crs="EPSG:3857",
                                                 tile_size=256)
        laid_out = tile_to_layout(geopysc, SPATIAL, reprojected, metadata)
        pyramided = pyramid(geopysc,
                            SPATIAL,
                            laid_out,
                            metadata,
                            256,
                            max_zoom,
                            0)
        for zoom, layer_rdd, layer_metadata in pyramided:
            write(geopysc,
                  SPATIAL,
                  self.catalog,
                  layer_name,
                  zoom,
                  layer_rdd,
                  layer_metadata)

        return self.base_url + "/" + layer_name
