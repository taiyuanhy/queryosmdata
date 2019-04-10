import collections
import xml.etree.ElementTree as ET

import fiona.crs
import geopandas as gpd
from pandas.io.common import urlopen, urlencode
import pandas as pd
from shapely.geometry import Point, LineString,Polygon,MultiPolygon
from six import string_types

OSMData = collections.namedtuple('OSMData', ('nodes', 'waynodes', 'waytags',
                                             'relmembers', 'reltags'))
_crs = fiona.crs.from_epsg(4326)

overpass_url_list = [
    'https://overpass.kumi.systems/api/interpreter',
    'https://lz4.overpass-api.de/api/interpreter',
    'https://z.overpass-api.de/api/interpreter',
    'http://overpass.openstreetmap.ie/api/interpreter',
]

# Tags to remove so we don't clobber the output. This list comes from
# osmtogeojson's index.js (https://github.com/tyrasd/osmtogeojson)
uninteresting_tags = set([
    "source",
    "source_ref",
    "source:ref",
    "history",
    "attribution",
    "created_by",
    "tiger:county",
    "tiger:tlid",
    "tiger:upload_uuid",
])


# http://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide
def query_osm(typ, bbox=None, recurse=None, tags='', raw=False,
              meta=False, operation='and', **kwargs):
    """
    Query the Overpass API to obtain OpenStreetMap data.

    See also:
    http://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide

    The OSM XML data is parsed into an intermediate set of DataFrames.
    By passing in 'render=False', this will return these DataFrames stored
    as the OSMData namedtuple. If render is True, then the DataFrames
    are built into their corresponding geometries.

    Parameters
    ----------
    typ : {'node', 'way', 'relation'}
        The type of OSM data to query
    bbox : (min lon, min lat, max lon, max lat) bounding box
        Optional bounding box to restrict the query. Unless the query
        is extremely restricted, you usually want to specify this.
        It can be retrieved from GeoPandas objects as 'df.total_bounds' or
        from Shapely objects as 'geom.bounds'
    recurse : {'up, 'down', 'uprel', 'downrel'}
        This is used to get more data than the original query. If 'typ' is
        'way', you'll usually want this set to 'down' which grabs all nodes
        of the matching ways
    tags : string or list of query strings
        See also the OverpassQL (referenced above) for more tag options
        Examples:
            tags='highway'
                Matches objects with a 'highway' tag
            tags='highway=motorway' <-- Matches ob
                Matches objects where the 'highway' tag is 'motorway'
            tags='name~[Mm]agazine'
                Match if the 'name' tag matches the regular expression

            Specify a list of tag requests to match all of them or the any of them
            tags=['highway', 'name~"^Magazine"']
                Match tags that have 'highway' and where 'name' starts
                with 'Magazine'

    raw : boolean, default False
        Return the raw XML data returned by the request
    render : boolean, default True
        Parse the output and return a final GeoDataFrame
    meta : boolean, default False
        Indicates whether to query the metadata with each OSM object. This
        includes the changeset, timestamp, uid, user, and version.
    operation: {'and', 'or'}, default 'and'
        the operation of query conditions
        'and' :  return a list of tag requests to match all of them
        'or' :  return a list of tag requests to match any of them

    way_type:{'Line','Polygon'} (optional)
        when typ equals 'way'
        'Line' : the type of geometry in geodataframe is LineString
        'Polygon' : the type of geometry in geodataframe is Polygon
    Returns
    -------
    df - GeoDataFrame
    Note that there's probably a bit more filtering required to get the
    exact desired data. For example if you only want ways, you may want
    to grab only the linestrings like:
        >>> df = df[df.type == 'LineString']

    """
    url = _build_url(typ, operation,bbox, recurse, tags, meta)

    # TODO: Raise on non-200 (or 400-599)
    with urlopen(url) as response:
        content = response.read()

    if raw:
        return content
    return read_osm(content, **kwargs)



def _build_url(typ,operation, bbox=None, recurse=None, tags='', meta=False):
    recurse_map = {
        'up': '<',
        'uprel': '<<',
        'down': '>',
        'downrel': '>>',
    }
    if recurse is None:
        recursestr = ''
    else:
        try:
            recursestr = recurse_map[recurse]
        except KeyError:
            raise ValueError("Unrecognized recurse value '{}'. "
                             "Must be one of: {}."
                             .format(recurse, ', '.join(recurse_map.keys())))

    # Allow tags to be a single string
    if isinstance(tags, string_types) and tags:
        tags = [tags]
    queries = ''.join('[{}]'.format(t) for t in tags)

    # Overpass QL takes the bounding box as
    # (min latitude, min longitude, max latitude, max longitude)
    if bbox is None:
        bboxstr = ''
    else:
        # bboxstr = "({})".format(
        # ','.join(str(b) for b in (bbox[1], bbox[0], bbox[3], bbox[2])))
        bboxstr = '(poly:"{}")'.format(
            ' '.join('{c[1]} {c[0]}'.format(c=c) for c in bbox.exterior.coords))

    if meta:
        metastr = 'meta'
    else:
        metastr = ''
    if operation == 'and':
        query = '({typ}{bbox}{queries};{recurse};);out {meta};'.format(
            typ=typ, bbox=bboxstr, queries=queries, recurse=recursestr, meta=metastr)
    elif operation == 'or':
        query = '('
        for temp_query in tags:
            temp_query = '['+temp_query+']'
            query += '{typ}{bbox}{queries};{recurse};'.format(
                typ=typ, bbox=bboxstr, queries=temp_query, recurse=recursestr)
        query += ');out '+metastr+';'
    url = ''.join([overpass_url_list[0],'?',
                   urlencode({'data': query})])
    # print(query)
    return url


def read_osm(content, render=True, **kwargs):
    """
    Parse OSM XML data and store as several DataFrames. Optionally "render"
    the DataFrames to GeoDataFrames.

    """
    doc = ET.fromstring(content)
    data = gpd.GeoDataFrame()
    nodes = read_nodes(doc)
    if not nodes.empty:
        waynodes, waytags = read_ways(doc)
        relmembers, reltags = read_relations(doc)

        data = OSMData(nodes, waynodes, waytags, relmembers, reltags)

        if render:
            data = render_to_gdf(data, **kwargs)

    return data


def read_nodes(doc):
    #   Example:
    #   <node id="1705717514" lat="42.3630798" lon="-71.0997601">
    #       <tag k="crossing" v="zebra"/>
    #       <tag k="highway" v="crossing"/>
    #       <tag k="source" v="Bing"/>
    #   </node>
    nodes = [_element_to_dict(xmlnode) for xmlnode in doc.findall('node')]
    nodes = _dict_to_dataframe(nodes)
    if not nodes.empty:
        nodes['lon'] = nodes['lon'].astype(float)
        nodes['lat'] = nodes['lat'].astype(float)
    return nodes


def _element_to_dict(element):
    d = element.attrib.copy()
    for t in element.findall('tag'):
        k = t.attrib['k']
        if k not in uninteresting_tags:
            d[k] = t.attrib['v']

    return d


def _dict_to_dataframe(d):
    df = pd.DataFrame.from_dict(d)
    if 'timestamp' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    return df


def read_ways(doc):
    #   Example:
    #   <way id="8614593">
    #       <nd ref="61326730"/>
    #       <nd ref="61326036"/>
    #       <nd ref="61321194"/>
    #       <tag k="attribution" v="Office of Geographic and Environmental Information (MassGIS)"/>
    #       <tag k="condition" v="fair"/>
    #       <tag k="created_by" v="JOSM"/>
    #       <tag k="highway" v="residential"/>
    #       <tag k="lanes" v="2"/>
    #       <tag k="massgis:way_id" v="171099"/>
    #       <tag k="name" v="Centre Street"/>
    #       <tag k="source" v="massgis_import_v0.1_20071008165629"/>
    #       <tag k="width" v="13.4"/>
    #   </way>
    waytags = []
    waynodes = []
    for xmlway in doc.findall('way'):
        wayid = xmlway.attrib['id']
        for i, xmlnd in enumerate(xmlway.findall('nd')):
            d = xmlnd.attrib.copy()
            d['id'] = wayid
            d['index'] = i
            waynodes.append(d)

        tags = _element_to_dict(xmlway)
        waytags.append(tags)

    waynodes = _dict_to_dataframe(waynodes)
    waytags = _dict_to_dataframe(waytags)

    return waynodes, waytags


def read_relations(doc):
    # Example:
    #   <relation id="1933745">
    #     <member type="way" ref="134055159" role="outer"/>
    #     <member type="way" ref="260533047" role="outer"/>
    #     <member type="way" ref="142867799" role="outer"/>
    #     <member type="way" ref="134063352" role="outer"/>
    #     <member type="way" ref="142803038" role="outer"/>
    #     <member type="way" ref="134056144" role="outer"/>
    #     <member type="way" ref="134056141" role="outer"/>
    #     <tag k="admin_level" v="8"/>
    #     <tag k="boundary" v="administrative"/>
    #     <tag k="name" v="Cambridge"/>
    #     <tag k="type" v="boundary"/>
    #     <tag k="wikipedia" v="en:Cambridge, Massachusetts"/>
    #   </relation>
    reltags = []
    relmembers = []
    for xmlrel in doc.findall('relation'):
        relid = xmlrel.attrib['id']
        for i, xmlmember in enumerate(xmlrel.findall('member')):
            d = xmlmember.attrib.copy()
            d['id'] = relid
            d['index'] = i
            relmembers.append(d)

        tags = _element_to_dict(xmlrel)
        reltags.append(tags)

    relmembers = _dict_to_dataframe(relmembers)
    reltags = _dict_to_dataframe(reltags)

    return relmembers, reltags

# if data contains way data ,the way_type decides the geometry type of way data in geodataframe is LineSting or Polygon
def render_to_gdf(osmdata, drop_untagged=True,way_type = 'Line'):
    nodes = render_nodes(osmdata.nodes, drop_untagged)
    ways = None
    if way_type == 'Line':
        ways = render_ways(osmdata.nodes, osmdata.waynodes, osmdata.waytags)
    if way_type == 'Polygon':
        ways = render_polygons(osmdata.nodes, osmdata.waynodes, osmdata.waytags)
    if ways is not None:
        # We should get append working
        nodes = nodes.append(ways).set_geometry('geometry', crs=_crs)
    return nodes


def render_nodes(nodes, drop_untagged=True):
    # Drop nodes that have no tags, convert lon/lat to points
    if drop_untagged:
        nodes = nodes.dropna(subset=nodes.columns.drop(['id', 'lon', 'lat']),
                             how='all')
    points = [Point(x['lon'], x['lat']) for i, x in nodes.iterrows()]
    nodes = nodes.drop(['lon', 'lat'], axis=1)
    nodes = nodes.set_geometry(points, crs=_crs)

    return nodes

# render linestring
def render_ways(nodes, waynodes, waytags):
    if waynodes is None or waynodes.empty:
        return None

    node_points = nodes[['id', 'lon', 'lat']]

    def wayline(df):
        df = df.sort_index(by='index')[['lon', 'lat']]
        return LineString(df.values)
    # Group the ways and create a LineString for each one.  way_lines is a
    # Series where the index is the way id and the value is the LineString.
    # Merge it with the waytags to get a single GeoDataFrame of ways
    waynodes = waynodes.merge(node_points, left_on='ref', right_on='id',
                              suffixes=('', '_nodes'))
    way_lines = waynodes.groupby('id').apply(wayline)
    ways = waytags.set_index('id').set_geometry(way_lines, crs=_crs)
    ways.reset_index(inplace=True)

    return ways
# render polygon
def render_polygons(nodes, polygonnodes, waytags):
    if polygonnodes is None or polygonnodes.empty:
        return None

    node_points = nodes[['id', 'lon', 'lat']]

    def waypolygon(df):
        df = df.sort_index(by='index')[['lon', 'lat']]
        return Polygon(df.values)
    # Group the ways and create a LineString for each one.  way_lines is a
    # Series where the index is the way id and the value is the LineString.
    # Merge it with the waytags to get a single GeoDataFrame of ways
    polygonnodes = polygonnodes.merge(node_points, left_on='ref', right_on='id',
                              suffixes=('', '_nodes'))
    way_polygons = polygonnodes.groupby('id').apply(waypolygon)
    polygons = waytags.set_index('id').set_geometry(way_polygons, crs=_crs)
    polygons.reset_index(inplace=True)

    return polygons