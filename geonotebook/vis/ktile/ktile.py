from collections import MutableMapping

import os
import tempfile

from notebook.utils import url_path_join as ujoin

import zmq
from zmq.eventloop import zmqstream
from tornado.ioloop import IOLoop
import pickle

import requests

import TileStache as ts
# NB:  this uses a 'private' API for parsing the Config layer dictionary
from TileStache.Config import _parseConfigLayer as parseConfigLayer

from geonotebook.utils import get_kernel_id
from geonotebook.vis.comm_protocol import *

from .handler import (KtileHandler,
                      KtileLayerHandler,
                      KtileTileHandler)


# Manage kernel_id => layer configuration section
# Note - when instantiated this is a notebook-wide class,
# it manages the configuration for all running geonotebook
# kernels. It lives inside the Tornado Webserver
class KtileConfigManager(MutableMapping):
    def __init__(self, default_cache, log=None):
        self.default_cache = default_cache
        self._configs = {}
        self.log = log

    def __getitem__(self, *args, **kwargs):
        return self._configs.__getitem__(*args, **kwargs)

    def __setitem__(self, _id, value):
        self._configs.__setitem__(_id, value)

    def __delitem__(self, *args, **kwargs):
        return self._configs.__delitem__(*args, **kwargs)

    def __iter__(self, *args, **kwargs):
        return self._configs.__iter__(*args, **kwargs)

    def __len__(self, *args, **kwargs):
        return self._configs.__len__(*args, **kwargs)

    def add_config(self, kernel_id, **kwargs):
        cache = kwargs.get("cache", self.default_cache)

        self._configs[kernel_id] = ts.parseConfig({
            "cache": cache,
            "layers": {}
        })

    def add_layer(self, kernel_id, layer_name, layer_dict, dirpath=''):
        # NB: dirpath is actually not used in _parseConfigLayer So dirpath
        # should have no effect regardless of its value.

        # Note: Needs error checking
        layer = parseConfigLayer(layer_dict, self._configs[kernel_id], dirpath)

        if layer_name not in self._configs[kernel_id].layers:
            self._configs[kernel_id].layers[layer_name] = layer

        try:
            layer.provider.generate_vrt()
        except AttributeError:
            pass

        return True


# Ktile vis_server,  this is not a persistent object
# It is brought into existence as a client to provide access
# to the KtileConfigManager through the Tornado webserver's
# REST API vi ingest/get_params. It is instantiated once inside
# the tornado app in order to call initialize_webapp.  This sets
# up the REST API that ingest/get_params communicate with. It also
# provides access points to start_kernel and shutdown_kernel for
# various initialization. NB: State CANNOT be shared across these
# different contexts!

class Ktile(object):
    def __init__(self, config, default_cache=None):
        self.config = config
        self.default_cache_section = default_cache

    @property
    def default_cache(self):
        return dict(self.config.items(self.default_cache_section))

    def start_kernel(self, kernel):
        kernel_id = get_kernel_id(kernel)
        msg = { 'action': 'register_kernel',
                'kernel_id': kernel_id
              }
        resp = webapp_comm_send(msg)

        if resp['success'] == True:
            kernel.log.info("Successfully registered kernel {}".format(kernel_id))
        else:
            raise RuntimeError("Unknown error during kernel registration" if 'err_msg' not in resp else resp['err_msg'])

    def shutdown_kernel(self, kernel):
        kernel_id = get_kernel_id(kernel)
        resp = webapp_comm_send({
            'action': 'delete_kernel',
            'kernel_id': kernel_id
        })

        if resp['success'] == True:
            kernel.log.info("Successfully deleted kernel {}".format(kernel_id))
        else:
            raise RuntimeError("Unknown error during kernel delete" if 'err_msg' not in resp else resp['err_msg'])

    # This function is called inside the tornado web app
    # from jupyter_load_server_extensions
    def initialize_webapp(self, config, nbapp):
        webapp = nbapp.web_app
        log = nbapp.log
        port = nbapp.port

        base_url = webapp.settings['base_url']
        log.info("Initializing web app with base url '{}'".format(base_url))

        webapp.ktile_config_manager = KtileConfigManager(
            self.default_cache,
            log=log
        )

        io_loop = IOLoop.current()
        ctx = zmq.Context()
        socket = ctx.socket(zmq.REP)
        selected_port = socket.bind_to_random_port("tcp://127.0.0.1", min_port=49152, max_port=65535, max_tries=100)

        user = os.environ['USER']
        log.info("Webapp communication channel opened on port {} for user {}".format(selected_port, user))
        with open("{}/{}".format(tempfile.gettempdir(), user), 'w') as f:
            f.write(str(selected_port))

        stream = zmqstream.ZMQStream(socket, io_loop)
        stream.on_recv(make_callback(stream, WebAppProtocol(nbapp)))

        webapp.add_handlers('.*$', [
            # kernel_name
            (ujoin(base_url, r'/ktile/([^/]*)'),
             KtileHandler,
             dict(ktile_config_manager=webapp.ktile_config_manager)),

            # kernel_name, layer_name
            (ujoin(base_url, r'/ktile/([^/]*)/([^/]*)'),
             KtileLayerHandler,
             dict(ktile_config_manager=webapp.ktile_config_manager)),

            # kernel_name, layer_name, x, y, z, extension
            (ujoin(base_url,
                   r'/ktile/([^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/\.]*)\.(.*)'),
             KtileTileHandler,
             dict(ktile_config_manager=webapp.ktile_config_manager)),

        ])
        log.info("Completed webapp registration for user {}".format(user))

    # get_params should take a generic list of parameters e.g. 'bands',
    # 'range', 'gamma' and convert these into a list of vis_server specific
    # parameters which will be passed along to the tile render handler in
    # add_layer. This is intended to allow the vis_server to include style
    # parameters and subsetting operations. select bands, set ranges
    # on a particular dataset etc.
    def get_params(self, name, data, **kwargs):
        # All paramater setup is handled on ingest
        return {}

    def _static_vrt_options(self, data, kwargs):
        options = {
            'vrt_path': kwargs['vrt_path'],
            'bands': data.band_indexes,
        }

        return options

    def _dynamic_vrt_options(self, data, kwargs):
        options = {
            'path': os.path.abspath(data.reader.path),
            'bands': data.band_indexes,

            'nodata': data.nodata,
            # TODO:  Needs to be moved into RasterData level API
            'raster_x_size': data.reader.width,
            'raster_y_size': data.reader.height,
            'transform': data.reader.dataset.profile['transform'],
            'dtype': data.reader.dataset.profile['dtype']
        }
        if 'map_srs' in kwargs:
            options['map_srs'] = kwargs['map_srs']

        return options

    def ingest(self, data, name=None, **kwargs):

        # Verify that a kernel_id is present otherwise we can't
        # post to the server extension to add the layer
        kernel_id = kwargs.pop('kernel_id', None)
        if kernel_id is None:
            raise Exception(
                "KTile vis server requires kernel_id as kwarg to ingest!")

        options = {
            'name': data.name if name is None else name
        }

        options.update(kwargs)

        # Note:
        # Check if the reader has defined a vrt_path
        #
        # This is mostly intended for the VRTReader so that it can communicate
        # that the VRT for reading data is also the VRT that should be used for
        # visualisation. Otherwise we wouild have to explicitly add a vrt_path
        # kwarg to the add_layer() call.
        #
        # A /different/ VRT can still be used for visualisation by passing
        # a path via vrt_path to add_layer.
        #
        # Finally, A dynamic VRT will ALWAYS be generated if vrt_path is
        # explicitly set to None via add_layer.
        if hasattr(data.reader, 'vrt_path'):
            if 'vrt_path' in kwargs and kwargs['vrt_path'] is None:
                # Explicitly set to None
                pass
            else:
                kwargs['vrt_path'] = data.reader.vrt_path

        # If we have a static VRT
        if 'vrt_path' in kwargs and kwargs['vrt_path'] is not None:
            options.update(self._static_vrt_options(data, kwargs))
        else:
            # We don't have a static VRT, set options for a dynamic VRT
            options.update(self._dynamic_vrt_options(data, kwargs))

        # Make the Request
        # port_request = webapp_comm_send({ 'action': 'request_port' })
        # base_url = 'http://127.0.0.1:{}/ktile/{}/{}'.format(port_request['port'], kernel_id, name)

        base_url_query = webapp_comm_send({ 'action': 'request_base_url' })
        base_url = '{}ktile/{}/{}'.format(base_url_query['base_url'], kernel_id, name)

        resp = webapp_comm_send({
            'action': 'add_layer',
            'kernel_id': kernel_id,
            'layer_name': name,
            'json': {
                "provider": {
                    "class": "geonotebook.vis.ktile.provider:MapnikPythonProvider",
                    "kwargs": options
                }
                # NB: Other KTile layer options could go here
                #     See: http://tilestache.org/doc/#layers
            }
        })

        if not resp['success']:
            raise RuntimeError("KTile.ingest() failed with error:\n\n{}".format(resp['err_msg']))

        return base_url
