# coding: utf-8
import geopandas_osm as osm
import math
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.geometry import MultiLineString
from shapely.ops import cascaded_union
# import matplotlib.pyplot as plt
import json
import time
import os
from flask import Flask
from flask import request
import traceback
import urllib2
import logging
from concurrent.futures import ThreadPoolExecutor

outputPath = 'e:\\osmdownloader'
defaultHeight = 15
callback_address = 'http://192.168.10.21:8888/downloadData/'
featureTypeList = ['motorway', 'primary', 'secondary', 'smallRoad', 'building', 'water', 'green', 'station','restaurant', 'bank']

executor = ThreadPoolExecutor(max_workers=4)
app = Flask(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def hello_world():
    return 'osm downloader is running on port 5060'

@app.route('/hello')
def hello():
    return 'osm downloader is running on port 5060'

@app.route('/processData')
def processData():
    try:
        boundary_param = request.args['bbox']
        task_code = request.args['code']
        boundary = boundary_param.split(',')
        extent = dict()
        extent['min_lat'] = float(boundary[1])
        extent['min_lon'] = float(boundary[0])
        extent['max_lat'] = float(boundary[3])
        extent['max_lon'] = float(boundary[2])
        logger.info('extent:' + json.dumps(extent))
        # 异步执行任务
        executor.submit(task_run,task_code,extent)
        return json.dumps({"code":task_code,"success":1})
    except Exception, e:
        traceback.print_exc()
        return json.dumps({"message":str(e),"success":0})

"""下载OSM数据的接口
    Args:
        bbox: 数据范围
        ordercode:订单编号
    Returns:
        dict 返回信息
        dict.success:1 成功 0 失败
        dict.ordercode:订单编号
        dict.result: 如果成功返回数据信息 如果失败显示错误信息
"""
def task_run(task_code,extent):
    config_all = dict()
    percent = 0
    interval = 100 / len(featureTypeList)

    complete_url = callback_address + 'complete?code=' + task_code + '&success='
    try:
        for feature_type in featureTypeList:
            config = getOSMData(extent, feature_type)
            percent += interval
            if percent > 100:
                percent = 100
            # 更新任务状态
            update_process_url = callback_address + 'updatePercent?code=' + task_code + '&percent='+str(percent)
            logger.info('update state:'+update_process_url)
            http_request = urllib2.Request(update_process_url)
            urllib2.urlopen(http_request)
            logger.info('update finished:'+update_process_url)
            if len(config[feature_type]) > 0:
                config_all = dict(config_all, **config)
        logger.info(json.dumps(config_all))
        #完成后更新任务状态
        complete_url += '1'
        logger.info('task complete :' + complete_url)
        data = json.dumps(config_all).encode('utf-8')
        http_request = urllib2.Request(complete_url, data=data, headers={'Content-Type': 'application/json'})
        urllib2.urlopen(http_request)
        logger.info('task complete finished: '+complete_url)
    except Exception, e:
        traceback.print_exc()
        #完成后更新任务状态
        complete_url += '0'
        data = urllib2.urlencode({'errorMessage': str(e)})
        http_request = urllib2.Request(complete_url, data=data)
        urllib2.urlopen(http_request)

# 获取当前时间
get_now_milli_time = lambda: int(time.time() * 1000)

"""获取OSM数据
    Args:
        bbox: 数据范围
        feature_type: 数据类型
        ordercode:订单编号
    Returns:
        dict 返回结果
        dict.feature_type:数据存放路径
"""
def getOSMData(extent, feature_type):
    logger.info('feature_type:' + feature_type + ' start......')
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
    # elif feature_type == 'education':
    #     df = osm.query_osm('node', boundary, recurse='down',
    #                        tags=['amenity=college', 'amenity=school', 'amenity=university', 'amenity=kindergarten'],
    #                        operation='or')
    #     result = df
    #     if not df.empty:
    #         result = df[df.type == 'Point'][['geometry', 'amenity']]
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
            result.to_file(output_filepath,'GeoJSON')
            # geojson_str = result.to_json()
            # output = open(output_filepath, 'w')
            # output.write(geojson_str)
            # output.close()
    logger.info('feature_type:' + feature_type + ' end,featureCount is '+str(len(result)))
    return config_dict
    # result.plot()
    # plt.show()


def mergeGeoDataFrameByField(geodataframe, field):  
    result = gpd.GeoDataFrame()  
    geometry_list = geodataframe['geometry'].tolist()
#     merge all geometries into one multiGeos
    multigeos = cascaded_union(geometry_list)
    result['geometry'] = gpd.GeoSeries(multigeos)
    result[field] = [field]
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
                if isNum2(v['building:levels']):
                    temp_levels = float(v['building:levels'])
                    height = 3 * temp_levels
                    height_list.append(height)
                    continue
        v['height'] = height
        height_list.append(height)
    geodataframe['height'] = height_list
    geodataframe = geodataframe.drop(index = underground_list)
    # print(geodataframe.to_json())
    return geodataframe
# 判断是否为浮点数
def isNum2(value):
    try:
        x = float(value) #此处更改想判断的类型
    except TypeError:
        return False
    except ValueError:
        return False
    except Exception as e:
        return False
    else:
        if math.isnan(x):
            return False
        return True

if __name__ == '__main__':
    logger.info('server started')
    app.run(host="0.0.0.0", port=5060)