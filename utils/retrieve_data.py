import os
import requests
import pandas as pd
import json
from geojson import Feature, MultiPolygon, FeatureCollection, Point, LineString
from shapely.wkb import loads
from shapely.ops import transform
import pyproj
from functools import partial
from ding0.core import NetworkDing0
from egoio.tools import db
from sqlalchemy.orm import sessionmaker
from utils.process_data import to_geojson, display_names, display_roundings
import yaml
import argparse


def retrieve_mv_grid_polygon(subst_id, geojson_path, version='v0.4.5'):

	os.makedirs(os.path.join(geojson_path, str(subst_id)), exist_ok=True)

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

	for k in data.keys():
		if k in display_names.keys():
			data[display_names[k]] = data.pop(k)

	projection = partial(
	                pyproj.transform,
	                pyproj.Proj(init='epsg:3035'),  # source coordinate system
	                pyproj.Proj(init='epsg:4326'))  # destination coordinate system

	data['coordinates'] = [tuple(list(transform(projection, g).exterior.coords) for g in geom.geoms)]

	for k, v in data.items():
		if k in display_roundings.keys() and v is not None:
			data[k] = round(float(v), display_roundings[k])

	feature_collection = to_geojson([data], geom_type='MultiPolygon')

	with open(os.path.join(geojson_path, str(subst_id), 'mv_grid_district_{}.geojson'.format(subst_id)), 'w') as outfile:
	    json.dump(feature_collection, outfile)



def generate_ding0_data(grid_id, save_path):

	engine = db.connection(readonly=True)
	session = sessionmaker(bind=engine)()

	nd = NetworkDing0(name='network')

	# run DING0 on selected MV Grid District
	nd.run_ding0(session=session,mv_grid_districts_no=[grid_id])

	nd.to_csv(save_path)


def create_data_folder(data_path):

	os.makedirs(data_path, exist_ok=True)


def to_list_of_ints(grid_id):

	if grid_id:
		assume_list = grid_id.split(",")
		assume_range = grid_id.split("..")

		if len(assume_list) > 1:
			grid_id_list = [int(_) for _ in assume_list]
		elif len(assume_range) > 1:
			grid_id_list = list(range(int(assume_range[0]), int(assume_range[1]) + 1))
		else:
			grid_id_list = [int(grid_id)]


		return grid_id_list
	else:
		return []


def read_config_yaml(conf_file):

	conf_settings = yaml.load(open(conf_file), Loader=yaml.SafeLoader)
	if isinstance(conf_settings['grid_id'], str):
		conf_settings['grid_id'] = [int(i) for i in conf_settings['grid_id'].split("..")]
	elif isinstance(conf_settings['grid_id'], int):
		conf_settings['grid_id'] = [conf_settings['grid_id']]

	return conf_settings




if __name__ == '__main__':

	# Parse command-line input
	parser = argparse.ArgumentParser(
		description='Retrieve data for visualization',
		formatter_class=argparse.RawTextHelpFormatter,
		epilog="Alternatively, you can provide all required input by a config file.\n" \
		"Use the argument `conf` to include a customs config file")
	parser.add_argument('--grid_id', type=str, help='ID of the grid. Following input formats are valid\n' \
		'\t--grid_id=645 (single grid)\n' \
		'\t--grid_id=645,655 (list of grid IDs)\n'
		'\t--grid_id=645..655 (range of grid IDs)\n'
		'Must be either given by command-line or by config file.',
		default=str())
	parser.add_argument('--csv_data_path', type=str, help="Path to save ding0 grid data in CSV format")
	parser.add_argument('--geojson_data_path', type=str, help="Path to save processed grid data in GeoJSON format")
	parser.add_argument('--conf', type=read_config_yaml, help="Config file in YAML format", default=dict())
	args = parser.parse_args()
	args.grid_id = to_list_of_ints(args.grid_id)

	# Read-in cmd-line args and custom config file args
	settings_custom_config = {k: v for k,v in vars(args)["conf"].items() if k != "exclude"}
	settings_cmd = vars(args)

	# Load config file
	settings_default_conf = read_config_yaml("_config.yml")
	
	# Merge three settings dicts with the following overwrite order
	# 1. CMD args
	# 2. Custom config file args
	# 3. Default config file args
	settings = {k: v for k, v in settings_default_conf.items() if v is not None and k != "exclude"}
	
	for k, v in settings_custom_config.items():
		if v:
			settings.update({k: v})

	for k, v in settings_cmd.items():
		if v and k != 'conf':
			settings.update({k: v})

	# create project and data folder
	create_data_folder(settings['csv_data_path'])
	create_data_folder(settings['geojson_data_path'])

	for g in settings['grid_id']:
		# retrieve mv grid district polygon
		retrieve_mv_grid_polygon(g, settings['geojson_data_path'])

		# generate ding0 data
		ding0_data = generate_ding0_data(g, settings['csv_data_path'])
