# coding: utf-8
import geopandas_osm as osm
import math
import geopandas as gpd
from shapely.geometry import Polygon
import re
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
import logging.handlers
from concurrent.futures import ThreadPoolExecutor
import MergeBuilding
import log_handler
import ssl
#全局取消证书验证
ssl._create_default_https_context = ssl._create_unverified_context

outputPath = 'e:\\osmdownloader'
# outputPath = '/uinnova/citybuilder/osm/osmdata'
defaultHeight = 15
callback_address = 'http://127.0.0.1:8888/projectTemplate/'
featureTypeList =['motorway', 'primary', 'secondary', 'smallRoad', 'building', 'water', 'green', 'station','restaurant', 'bank']
# featureTypeList = ['building']
mergeCount = 1000
timeout = 2*60
executor = ThreadPoolExecutor(max_workers=4)
app = Flask(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                    level=logging.INFO)
logger = app.logger

@app.route('/')
def hello_world():
    logger.info('test')
    return 'osm downloader is running on port 5060'

@app.route('/hello')
def hello():
    logger.info('hello')
    return 'osm downloader is running on port 5060'

@app.route('/processData')
def processData():
    try:
        boundary_param = request.args['bbox']
        headers = dict(request.headers)
        if headers.has_key("Openid"):
            openid = headers["Openid"]
        else:
            openid = 'oLX7p04daC2OdoZCbP6VihD_0XCo'
        task_code = request.args['code']
        boundary = boundary_param.split(',')
        extent = dict()
        extent['min_lat'] = float(boundary[1])
        extent['min_lon'] = float(boundary[0])
        extent['max_lat'] = float(boundary[3])
        extent['max_lon'] = float(boundary[2])
        logger.info('extent:' + json.dumps(extent))
        # 异步执行任务
        executor.submit(task_run,task_code,extent,openid)
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
def task_run(task_code,extent,openid):
    config_all = dict()
    percent = 0
    interval = 100 / len(featureTypeList)

    complete_url = callback_address + 'complete?code=' + task_code + '&success='
    try:
        # raise RuntimeError('testError')
        for feature_type in featureTypeList:
            config = getOSMData(extent, feature_type)
            # 如果下载过程中出现错误,则返回失败
            if config.has_key('err'):
                handleError(complete_url, config['err'], openid)
                return
            percent += interval
            if percent > 100:
                percent = 100
            # 更新任务状态
            update_process_url = callback_address + 'updatePercent?code=' + task_code + '&percent='+str(percent)
            logger.info('update state:'+update_process_url)
            http_request = urllib2.Request(update_process_url,headers={'OpenId':openid})
            urllib2.urlopen(http_request)
            logger.info('update finished:'+update_process_url)
            if len(config[feature_type]) > 0:
                config_all = dict(config_all, **config)
        logger.info(json.dumps(config_all))
        #完成后更新任务状态
        complete_url += '1'
        logger.info('task complete :' + complete_url)
        data = json.dumps(config_all).encode('utf-8')
        http_request = urllib2.Request(complete_url, data=data, headers={'Content-Type': 'application/json','OpenId':openid})
        urllib2.urlopen(http_request)
        logger.info('task complete finished: '+complete_url)
    except Exception, e:
        traceback.print_exc()
        #完成后更新任务状态
        handleError(complete_url,e,openid)

def handleError(complete_url,e,openid):
    # traceback.print_exc()
    # 完成后更新任务状态
    complete_url += '0'
    logger.info('task failed message :' + str(e))
    logger.info('task failed url :' + complete_url)
    http_request = urllib2.Request(complete_url, data=str(e),
                                   headers={'Content-Type': 'application/text', 'OpenId': openid})
    try:
        urllib2.urlopen(http_request)
    except Exception, e2:
        traceback.print_exc()
    logger.info('task failed finished :' + complete_url)

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
            result = mergeGeoDataFrameByField(result, 'highway',feature_type)
    elif feature_type == 'primary':
        df = osm.query_osm('way', boundary, recurse='down',
                           tags=['highway=primary', 'highway=primary_link', 'highway=trunk', 'highway=trunk_link'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway',feature_type)
    elif feature_type == 'secondary':
        df = osm.query_osm('way', boundary, recurse='down',
                           tags=['highway=secondary', 'highway=secondary_link', 'highway=tertiary',
                                 'highway=tertiary_link'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway',feature_type)
    elif feature_type == 'smallRoad':
        df = osm.query_osm('way', boundary, recurse='down', tags=['highway=residential'],
                           operation='or', way_type='Line')
        result = df
        if not df.empty:
            result = df[df.type == 'LineString'][['highway', 'geometry']]
            result = mergeGeoDataFrameByField(result, 'highway',feature_type)
    elif feature_type == 'building':
        df = osm.query_osm('way', boundary, recurse='down', tags='building', way_type='Polygon',timeout=timeout)
        if type(df).__name__ == 'dict':
            print(json.dumps(df))
            return df
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
            result = mergeGeoDataFrameByField(result, 'natural',feature_type)
    elif feature_type == 'water':
        df = osm.query_osm('way', boundary, recurse='down', tags='natural=water', way_type='Polygon')
        result = df
        if not df.empty:
            result = df[df.type == 'Polygon'][['geometry','natural']]
            result = mergeGeoDataFrameByField(result, 'natural',feature_type)
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

def geometry_is_valid(geometry):
    return geometry.is_valid

"""根据某一个字段对数据进行合并
geodataframe 待合并的geodataframe
field 根据此字段合并
value 合并后field字段对应的属性值
"""
def mergeGeoDataFrameByField(geodataframe, field ,value=None):
    result = gpd.GeoDataFrame()  
    geometry_list = geodataframe['geometry'].tolist()
    #过滤掉有错误的geometry
    geometry_list = filter(geometry_is_valid,geometry_list)
#     merge all geometries into one multiGeos
    multigeos = cascaded_union(geometry_list)
    result['geometry'] = gpd.GeoSeries(multigeos)
    if value is None :
        value = field
    result[field] = [value]
    return result

def handleBuildingData(geodataframe):
    columns = geodataframe.columns.values
    underground_list = []
    height_list = []
    # 未合并的建筑数据输出到硬盘
    # output = open(outputPath+os.sep+'building_unmerged_'+str(get_now_milli_time()) + '.geojson', 'w')
    # output.write(geodataframe.to_json())
    # output.close()
    for i,v in geodataframe.iterrows():
        if 'layer' in columns:
            if isNum(v['layer']):
                layer = float(v['layer'])
                if layer < 0:
                    underground_list.append(i)
        height = defaultHeight
        if 'height' in columns:
            if isNum(v['height']):
                height = float(v['height'])
                height_list.append(height)
                continue
        if 'building:levels' in columns:
            if isNum(v['building:levels']):
                # print(v['building:levels'])
                temp_levels = float(v['building:levels'])
                height = 3 * temp_levels
                height_list.append(height)
                continue
        v['height'] = height
        height_list.append(height)
    geodataframe['height'] = height_list
    #删除地下建筑
    geodataframe = geodataframe.drop(index = underground_list)
    #如果超过一定数量,进行合并
    if len(geodataframe) > mergeCount:
        logger.info('start merging building......')
        geodataframe = MergeBuilding.mergeGeoDataFrameBuilding(geodataframe,'height')
        logger.info('merge building success......')
    # print(geodataframe.to_json())
    return geodataframe
# 判断是否为浮点数
def isNum(obj):
    try:
        if obj is None:
            return False
        if type(obj).__name__ == 'str' or type(obj).__name__ == 'unicode':
            float(obj)
        elif type(obj).__name__ == 'float':
            # 因为使用float有一个例外是nan
            if math.isnan(obj):
                return False
        return True
    except Exception:
        return False

def testDownload(extent):
    for feature in featureTypeList:
        getOSMData(extent,feature)


if __name__ != '__main__':
    log_handler.set_logger(logger)
    logger.info('server started by gunicorn')

if __name__ == '__main__':
#     print(111)
    log_handler.set_logger(logger)
    logger.info('server started')
    app.run(host="0.0.0.0", port=5060)

    # print(isNum('\u5f6d\u6811\u6797\u7f16\u8f91\u7684\u6d4b\u8bd5\u6570\u636e'))
    #116.31757748306245,39.9045864219624,116.43467545165014,39.99434794896722"
    # ext = '116.35512709290839,39.90387410973224,116.36681878444415,39.91284148570492'
    # ext = ext.split(',')
    # extent = {"max_lon": float(ext[2]), "min_lat": float(ext[1]), "min_lon": float(ext[0]),"max_lat": float(ext[3])}
#     extent = {"max_lon":116.31757748306245, "min_lat":  39.9045864219624, "min_lon":116.35467545165014, "max_lat": 39.92434794896722}
#     testDownload(extent)
    # handleError(callback_address + 'complete?code=9&success=','error','zzzzzzzz')

    # gdf = gpd.GeoDataFrame.from_file('E:\\osmdownloader\\building_unmerged_1555313776171.geojson')
    # handleBuildingData(gdf)