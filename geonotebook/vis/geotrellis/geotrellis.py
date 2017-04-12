import notebook
import os
import tornado.web

from notebook.base.handlers import IPythonHandler
from notebook.utils import url_path_join as ujoin
from tornado import gen
from tornado.web import URLSpec


# jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop

class GeoTrellisHandler(IPythonHandler):

    def initialize(self, fn):
        self.fn = fn

    @gen.coroutine
    def get(self, layer_name, _x, _y, _zoom, **kwargs):
        x = int(_x)
        y = int(_y)
        zoom = int(_zoom)

        self.set_header('Content-Type', 'image/png')
        png = self.fn(layer_name, x, y, zoom)
        self.write(png)
        self.finish()


class GeoTrellis(object):

    def __init__(self, config, catalog, SPARK_HOME):
        os.environ['SPARK_HOME'] = SPARK_HOME
        from pyspark import SparkContext
        from geopyspark.geopycontext import GeoPyContext

        sc = SparkContext(appName="Value Reader")
        self.geopysc = GeoPyContext(sc)
        self.config = config
        self.catalog = catalog

    def start_kernel(self, kernel):
        pass

    def shutdown_kernel(self, kernel):
        pass

    def initialize_webapp(self, config, webapp):
        import io

        from geopyspark.geotrellis.catalog import read_value
        from geopyspark.geotrellis.constants import SPATIAL
        from PIL import Image, ImageOps

        def fn(layer_name, x, y, zoom):

            def make_image(arr):
                return Image.fromarray(arr.astype('uint8')).resize((256,256), Image.NEAREST).convert('L')

            tile = read_value(self.geopysc, SPATIAL, self.catalog, layer_name, zoom, x, y)
            arr = tile['data']
            bands = max(arr.shape[0],3)
            arrs = [np.array(arr[x, :, :]).reshape(256, 256) for x in range(bands)]
            images = [make_image(arr) for arr in arrs]
            image = ImageOps.autocontrast(Image.merge('RGB', images))
            bio = io.BytesIO()
            image.save(bio, 'PNG')

            return bio.getvalue()

        pattern = r'/user/[^/]+/geotrellis/([^/]+)/([0-9]+)/([0-9]+)/([0-9]+)\.png.*'
        webapp.add_handlers(r'.*', [(pattern, GeoTrellisHandler, {'fn': fn})])

    def get_params(self, name, data, **kwargs):
        return {}

    def ingest(self, data, max_zoom, name=None, **kwargs):
        from geopyspark.geotrellis.catalog import write
        from geopyspark.geotrellis.constants import SPATIAL
        from geopyspark.geotrellis.geotiff_rdd import geotiff_rdd
        from geopyspark.geotrellis.tile_layer import collect_pyramid_metadata, tile_to_layout, pyramid, reproject

        path = data.uri
        layer_name = format(hash(name) + hash(str(kwargs)), 'x').replace("-", "Z")

        # GeoTrellis ingest
        rdd = geotiff_rdd(geopysc, SPATIAL, path, maxTileSize=256, numPartitions=64)
        reprojected = reproject(geopysc, rdd, "EPSG:3857")
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

        return "/user/jack/geotrellis/" + layer_name
