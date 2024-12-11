import geopandas as gpd
from osgeo import gdal, ogr

table_dict = {
    0: 'intergranular_lane',
    1: 'uniform_granule',
    2: 'granule_with_dot',
    3: 'granule_with_lane',
    4: 'complex_granule',
}

og_table_dict = {
    0: 'og_intergranular_lane',
    1: 'og_uniform_granule',
    2: 'og_granule_with_dot',
    3: 'og_granule_with_lane',
    4: 'og_complex_granule',
}

def array_to_raster(array):
    rows, cols = array.shape
    driver = gdal.GetDriverByName('MEM')
    raster = driver.Create('', cols, rows, 1, gdal.GDT_Int32)
    raster.GetRasterBand(1).WriteArray(array)
    return raster


def raster_to_polygons(raster):
    src_band = raster.GetRasterBand(1)
    
    driver = ogr.GetDriverByName('MEMORY')
    datasource = driver.CreateDataSource('temp')
    srs = ogr.osr.SpatialReference()
    srs.ImportFromEPSG(4326)      
    layer = datasource.CreateLayer('polygons', srs=srs, geom_type=ogr.wkbPolygon)
    
    field = ogr.FieldDefn("Value", ogr.OFTInteger)
    layer.CreateField(field)

    gdal.Polygonize(src_band, None, layer, 0, [], callback=None)
    
    polygons = []
    for feature in layer:
        geom = feature.GetGeometryRef()
        value = feature.GetField("Value")
        polygons.append({"geometry": geom.ExportToWkt(), "value": value})
    
    return polygons


def array_to_gdf(array):
    raster = array_to_raster(array)
    polygons = raster_to_polygons(raster)

    gdf = gpd.GeoDataFrame.from_records(polygons, columns=["geometry", "value"])
    gdf['geometry'] = gdf['geometry'].apply(lambda x: ogr.CreateGeometryFromWkt(x))
    
    return gdf