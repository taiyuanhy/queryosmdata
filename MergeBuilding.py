# coding: utf-8
import json
import fiona
from shapely.geometry.multipolygon import MultiPolygon
from shapely.ops import cascaded_union
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

def mergeGeoDataFrameBuilding(buildings_geodataframe,fieldName):
    buildings_geodataframe['height'] = pd.to_numeric(buildings_geodataframe[fieldName])
    # print(buildings.head())
    maxHeight = buildings_geodataframe.loc[:, "height"].max()
    minHeight = buildings_geodataframe.loc[:, "height"].min()
    count = 10
    height_list = []
    new_buildings_geometry = []
    new_buildings_geoseries = []
    new_buildings_geodataframe = gpd.GeoDataFrame()
    interval = float(maxHeight - minHeight) / count
    for k in range(count):
        new_buildings_geometry.append([])
    # print(height_list)
    for i, v in buildings_geodataframe.iterrows():
        building = v['geometry']
        if building is not None:
            if building.is_valid:
                temp_height = v['height']
                for k in range(count):
                    if k < count - 1:
                        if temp_height >= minHeight + k * interval and temp_height < minHeight + (k + 1) * interval:
                            new_buildings_geometry[k].append(building)
                            break
                    elif k == count - 1:
                        if temp_height >= minHeight + k * interval and temp_height <= minHeight + (k + 1) * interval:
                            new_buildings_geometry[k].append(building)
                            break
        # print(gpd.GeoSeries(new_buildings_geometry[0]))
        # for k in range(count):
        #     new_buildings_geodataframe.append(gpd.GeoSeries(new_buildings_geometry[k]))
    for k in range(count):
        if len(new_buildings_geometry[k]) > 0:
            height_list.append(int(minHeight + (k + 0.5) * interval))
            new_buildings_combined = cascaded_union(new_buildings_geometry[k])
            if new_buildings_combined.geom_type == 'Polygon':
                new_buildings_combined = MultiPolygon([new_buildings_combined])
            new_buildings_geoseries.append(new_buildings_combined)
    new_buildings_geodataframe['geometry'] = new_buildings_geoseries
    new_buildings_geodataframe['height'] = height_list
    new_buildings_geodataframe.set_geometry('geometry')
    return new_buildings_geodataframe


def mergeBuilding(path,fieldName):
    buildings = gpd.read_file(path)
    new_buildings_geodataframe = mergeGeoDataFrameBuilding(buildings,fieldName)
    outpath = path.replace(".geojson", "_new.geojson")
    new_buildings_geodataframe.to_file(outpath,'GeoJSON')

def divideFile(path):
    max_count = 15000
    buildings = gpd.read_file(path)
    file_count = len(buildings) / max_count
    for i in range(file_count):
        cursor = i*max_count
        outpath = path.replace(".geojson", "_part"+str(i+1)+".geojson")
        print([cursor,cursor+max_count])
        print(outpath)
        if(i!=file_count-1):
            new_buildings_geodataframe = buildings[cursor:cursor+max_count]
        else:
            new_buildings_geodataframe = buildings[cursor:len(buildings)+1]
        outstr = new_buildings_geodataframe.to_json()
        output = open(outpath, 'w')
        output.write(outstr)
        output.close()

if __name__ == '__main__':
    path = 'e:\\harbin_building.geojson'
    # divideFile(path)
    mergeBuilding(path,'floor')