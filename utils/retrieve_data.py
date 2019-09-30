import os
import requests
import pandas as pd
import json
from geojson import Feature, MultiPolygon, FeatureCollection, Point
from shapely.wkb import loads
from shapely.ops import transform
import pyproj
from functools import partial
from ding0.core import NetworkDing0
from egoio.tools import db
from sqlalchemy.orm import sessionmaker


def retrieve_mv_grid_polygon(subst_id, version='v0.4.5'):

	# prepare query
	oep_url= 'http://oep.iks.cs.ovgu.de/'
	schema = "grid"
	table = "ego_dp_mv_griddistrict"
	where_version = 'where=version=' + version
	where_ids = '&where=subst_id=' + str(subst_id)
	
	# retrieve data and reformat geo data
	get_data = requests.get(
		oep_url+'/api/v0/schema/'+schema+'/tables/'+table+'/rows/?{version}{where_ids}'.format(version=where_version, where_ids=where_ids)
		)
	data = get_data.json()[0]
	geom = loads(data['geom'], hex=True)

	projection = partial(
	                pyproj.transform,
	                pyproj.Proj(init='epsg:3035'),  # source coordinate system
	                pyproj.Proj(init='epsg:4326'))  # destination coordinate system

	data['coordinates'] = [tuple(list(transform(projection, g).exterior.coords) for g in geom.geoms)]

	feature_collection = to_geojson([data], geom_type='MultiPolygon')

	return feature_collection


def to_geojson(data, geom_type):
	"""Convert JSON to GeoJSON"""

	collection = []

	for dat in data:
		if geom_type == 'MultiPolygon':
			coordinates = MultiPolygon(dat['coordinates'])
		elif geom_type == 'Point':
			coordinates = Point(dat['coordinates'])
		else:
			raise NotImplementedError()


		properties = {key: value for key, value in dat.items() 
			if key not in ['geom', 'coordinates', 'geom_type']}
		feature_coordinates = Feature(geometry=coordinates, properties=properties)

		collection.append(feature_coordinates)

	feature_collection = FeatureCollection(collection)

	return feature_collection



def generate_ding0_data(subst_id):

	engine = db.connection(readonly=True)
	session = sessionmaker(bind=engine)()

	nd = NetworkDing0(name='network')

	# run DING0 on selected MV Grid District
	nd.run_ding0(session=session,mv_grid_districts_no=[subst_id])

	return nd


def create_project_folder(folderpath):

	os.makedirs(folderpath, exist_ok=True)
	# os.makedirs(os.path.join(folderpath, 'data'), exist_ok=True)


def create_data_folder(folder='data'):

	if os.path.exists(folder):
	    shutil.rmtree(folder)
	os.makedirs(folder)


def geom_to_coords(geom):

	coordinates_shp = loads(geom, hex=True)
	coordinates = [coordinates_shp.x, coordinates_shp.y]

	return coordinates


def reformat_ding0_grid_data(file):

	data = pd.read_csv(file)

	geo_referenced_data = data.loc[~data['geom'].isna(), 'geom']
	geo_referenced_data = pd.DataFrame(geo_referenced_data.apply(geom_to_coords).rename('coordinates'), index=geo_referenced_data.index)
	geo_referenced_data['lat'] = geo_referenced_data['coordinates'].apply(lambda x: x[0])
	geo_referenced_data['lon'] = geo_referenced_data['coordinates'].apply(lambda x: x[1])
	
	data = data.join(geo_referenced_data, how='inner')

	data = data.fillna('NaN').to_dict(orient='records')

	return data


if __name__ == '__main__':

	# setup
	mv_grid_district = 2659
	project_folder = os.path.join(os.path.expanduser('~'), 'projects', 'ding0_visualization_v1')
	data_folder = 'data'

	# # create project and data folder
	create_project_folder(project_folder)
	create_data_folder()

	# # retrieve mv grid district polygon
	mv_grid_district_polygon = retrieve_mv_grid_polygon(mv_grid_district)
	with open(os.path.join(data_folder, 'mv_grid_district_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(mv_grid_district_polygon, outfile)

	# # generate ding0 data
	ding0_data = generate_ding0_data(mv_grid_district)
	ding0_data.to_csv(os.path.join(data_folder, 'ding0'))

	# reformat ding0 data
	ding0_data_reformated = reformat_ding0_grid_data(
		os.path.join(data_folder, 'ding0', str(mv_grid_district), 'buses_{}.csv'.format(mv_grid_district))
		)
	# ding0_data_reformated.to_csv(os.path.join(data_folder, 'ding0', str(mv_grid_district), 'buses_{}.csv'.format(mv_grid_district)))
	ding0_data_geojson = to_geojson(ding0_data_reformated, geom_type='Point')
	with open(os.path.join(data_folder, 'ding0', str(mv_grid_district), 'mv_visualization_data_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(ding0_data_geojson, outfile)