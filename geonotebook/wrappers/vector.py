import collections

import fiona
import six

from .. import annotations


class VectorData(collections.Sequence):

    def __init__(self, path, **kwargs):
        # the layer attribute will be set once this instance is
        # added to a layer
        self.layer = None
        if isinstance(path, six.string_types):
            self.reader = fiona.open(path)
        else:
            self.reader = path

    @property
    def name(self):
        self.reader.name

    def __len__(self):
        return len(self.reader)

    def __getitem__(self, key):
        # fiona doesn't raise an error when accessing and invalid key
        if key < 0 or key >= len(self):
            raise IndexError()
        return self.reader[key]

    @property
    def geojson(self):
        """Return an object (geojson) representation."""
        features = list(self.reader)

        # Here, we add an id property to each feature which will
        # be used for styling on the client.
        for i, feature in enumerate(features):
            properties = feature.setdefault('properties', {})
            properties['_geonotebook_feature_id'] = i
        return {
            'type': 'FeatureCollection',
            'features': features
        }

    @property
    def points(self):
        """Return a generator of "Point" annotation objects."""
        for feature in self.reader:
            geometry = feature['geometry']
            if geometry['type'] == 'Point':
                coords = geometry['coordinates']
                yield annotations.Point(
                    coords, layer=self.layer, **feature['properties']
                )
            elif geometry['type'] == 'MultiPoint':
                for coords in geometry['coordinates']:
                    yield annotations.Point(
                        coords, layer=self.layer, **feature['properties']
                    )

    @property
    def polygons(self):
        """Return a generator of "Polygon" annotation objects."""
        for feature in self.reader:
            geometry = feature['geometry']
            if geometry['type'] == 'Polygon':
                coords = geometry['coordinates']
                yield annotations.Polygon(
                    coords[0], coords[1:],
                    layer=self.layer, **feature['properties']
                )
            elif geometry['type'] == 'MultiPolygon':
                for coords in geometry['coordinates']:
                    yield annotations.Polygon(
                        coords[0], coords[1:],
                        layer=self.layer, **feature['properties']
                    )

class GeoJsonData(object):

    def __init__(self, geojson, **kwargs):
        # the layer attribute will be set once this instance is
        # added to a layer
        self.layer = None
        self._geojson = geojson

    @property
    def name(self):
        "%s_%s" % ("GeoJSON", hash(str(self._geojson)))

    def __len__(self):
        if "features" in self._geojson:
            return len(self._geojson["features"])
        else:
            return 1

    def __getitem__(self, key):
        if "features" in self._geojson:
            if key < 0 or key >= len(self):
                raise IndexError()
            return self._geojson["features"][key]
        else:
            return self._geojson

    @property
    def geojson(self):
        """Return an object (geojson) representation."""
        return self._geojson
