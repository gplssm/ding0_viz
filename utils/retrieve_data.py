import os
import requests
import pandas as pd
import json
from geojson import Feature, MultiPolygon, FeatureCollection
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

	# Convert to geojson
	coordinates = MultiPolygon(data['coordinates'])

	properties = {key: value for key, value in data.items() 
		if key not in ['geom', 'coordinates', 'geom_type']}
	feature_coordinates = Feature(geometry=coordinates, properties=properties)

	feature_collection = FeatureCollection([feature_coordinates])

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
	os.makedirs(os.path.join(folderpath, 'data'), exist_ok=True)


if __name__ == '__main__':

	# setup
	mv_grid_district = 2659
	project_folder = os.path.join(os.path.expanduser('~'), 'projects', 'ding0_visualization_v1')
	data_folder = os.path.join(project_folder, 'data')

	# create project folder
	create_project_folder(project_folder)

	# retrieve mv grid district polygon
	mv_grid_district_polygon = retrieve_mv_grid_polygon(mv_grid_district)
	with open(os.path.join(data_folder, 'mv_grid_district_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(mv_grid_district_polygon, outfile)

	# generate ding0 data
	ding0_data = generate_ding0_data(mv_grid_district)
	ding0_data.to_csv(os.path.join(data_folder, 'ding0'))