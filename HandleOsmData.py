# coding: utf-8
import geopandas_osm as osm
import math
from shapely.geometry import Polygon
# import matplotlib.pyplot as plt
import json
import time
import os
from flask import Flask
from flask import request
import traceback
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'osm downloader is running on port 5060'

@app.route('/hello')
def hello():
    return 'osm downloader is running on port 5060'

@app.route('/processData')
def processData():
    boundary_param = request.args['bbox']
    boundary = boundary_param.split(',')
    extent = dict()
    extent['min_lat'] = float(boundary[1])
    extent['min_lon'] = float(boundary[0])
    extent['max_lat'] = float(boundary[3])
    extent['max_lon'] = float(boundary[2])
    print('extent:' + json.dumps(extent))
    featureTypeList = ['motorway', 'primary', 'secondary', 'smallRoad', 'building', 'water', 'green', 'station',
                      'restaurant', 'bank']
    # featureTypeList = ['building']
    config_all = dict()
    try:
        for featureType in featureTypeList:
            config = getOSMData(extent, featureType)
            if len(config[featureType]) > 0:
                config_all = dict(config_all, **config)
        print(json.dumps(config_all))
        return json.dumps({'success': 1, 'data': config_all})
    except Exception, e:
        traceback.print_exc()
        return json.dumps({'success': 0, 'message': str(e)})
outputPath = 'e:\\osmdownloader'
defaultHeight = 15

get_now_milli_time = lambda: int(time.time() * 1000)

def getOSMData(extent, feature_type):
    print('feature_type:' + feature_type + ' start......')
    boundary = Polygon([(extent['min_lon'], extent['min_lat']), (extent['min_lon'], extent['max_lat']),
                        (extent['max_lon'], extent['max_lat']), (extent['max_lon'], extent['min_lat'])])
    result = None
    if feature_type == 'motorway':
        df = osm.query_osm('way', boundary, recurse='down', tags=['highway=motorway', 'highway=motorway_link'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway')
    elif feature_type == 'primary':
        df = osm.query_osm('way', boundary, recurse='down',
                           tags=['highway=primary', 'highway=primary_link', 'highway=trunk', 'highway=trunk_link'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway')
    elif feature_type == 'secondary':
        df = osm.query_osm('way', boundary, recurse='down',
                           tags=['highway=secondary', 'highway=secondary_link', 'highway=tertiary',
                                 'highway=tertiary_link'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway')
    elif feature_type == 'smallRoad':
        df = osm.query_osm('way', boundary, recurse='down', tags=['highway=residential'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway')
    elif feature_type == 'building':
        df = osm.query_osm('way', boundary, recurse='down', tags='building', way_type='Polygon')
        columns = df.columns.values.tolist()
        filter_list = ['geometry', 'building']
        if 'height' in columns:
            filter_list.append('height')
        if 'building:levels' in columns:
            filter_list.append('building:levels')
        if 'layer' in columns:
            filter_list.append('layer')
        result = df
        if not df.empty:
            result = df[df.type == 'Polygon'][filter_list]
            result = handleBuildingData(result)
    elif feature_type == 'green':
        df = osm.query_osm('way', boundary, recurse='down',
                           tags=['natural=wood', 'natural=tree', 'natural=scrub', 'natrual=grassland',
                                 'leisure=golf_course', 'leisure=park'], operation='or', way_type='Polygon')
        result = df
        if not df.empty:
            result = df[df.type == 'Polygon'][['geometry']]
            result['natural'] = 'green'
            result = mergeGeoDataFrameByField(result, 'natural')
    elif feature_type == 'water':
        df = osm.query_osm('way', boundary, recurse='down', tags='natural=water', way_type='Polygon')
        result = df
        if not df.empty:
            result = df[df.type == 'Polygon'][['geometry','natural']]
            result = mergeGeoDataFrameByField(result, 'natural')
    elif feature_type == 'station':
        df = osm.query_osm('node', boundary, recurse='down', tags='railway=station')
        result = df
        if not df.empty:
            result = df[df.type == 'Point'][['geometry', 'railway']]
    elif feature_type == 'bank':
        df = osm.query_osm('node', boundary, recurse='down', tags=['amenity=atm', 'amenity=bank'], operation='or')
        result = df
        if not df.empty:
            result = df[df.type == 'Point'][['geometry', 'amenity']]
    elif feature_type == 'restaurant':
        df = osm.query_osm('node', boundary, recurse='down',
                           tags=['amenity=restaurant', 'amenity=cafe', 'amenity=bar', 'amenity=pub'], operation='or')
        result = df
        if not df.empty:
            result = df[df.type == 'Point'][['geometry', 'amenity']]
    elif feature_type == 'education':
        df = osm.query_osm('node', boundary, recurse='down',
                           tags=['amenity=college', 'amenity=school', 'amenity=university', 'amenity=kindergarten'],
                           operation='or')
        result = df
        if not df.empty:
            result = df[df.type == 'Point'][['geometry', 'amenity']]
    config_dict = {feature_type: ''}
    if result is not None:
        if not result.empty:
            # print(result.head())
            # print(result.to_json())
            output_filename = feature_type + '_' + str(get_now_milli_time()) + '.geojson'
            output_filepath = outputPath + os.sep + output_filename
            config_dict[feature_type] = output_filepath
            if not os.path.exists(outputPath):
                os.makedirs(outputPath)
            result.to_file(output_filepath, driver='GeoJSON')
    print('feature_type:' + feature_type + ' end,featureCount is '+str(len(result)))
    return config_dict
    # result.plot()
    # plt.show()


def mergeGeoDataFrameByField(geodataframe, field):
    result = geodataframe.dissolve(by=field)
    result[field] = result.index.values
    return result

def handleBuildingData(geodataframe):
    columns = geodataframe.columns.values
    underground_list = []
    height_list = []
    for i,v in geodataframe.iterrows():
        if 'layer' in columns:
            if v['layer'] is not None:
                layer = float(v['layer'])
                if layer < 0:
                    underground_list.append(i)
        height = defaultHeight
        if 'height' in columns:
            if v['height'] is not None:
                temp_height = float(v['height'])
                if math.isnan(temp_height) is False:
                    height = temp_height
                    height_list.append(height)
                    continue
        elif 'building:levels' in columns:
            if v['building:levels'] is not None:
                temp_levels = float(v['building:levels'])
                if math.isnan(temp_levels) is False:
                    height = 3 * temp_levels
                    height_list.append(height)
                    continue
        v['height'] = height
        height_list.append(height)
    geodataframe['height'] = height_list
    geodataframe = geodataframe.drop(index = underground_list)
    # print(geodataframe.to_json())
    return geodataframe

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5060)