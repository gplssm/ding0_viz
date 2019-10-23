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


display_names = {
  "p_nom": "Nominal power in kW",
  "s_nom": "Nominal apparent power in kVA",
  "bus": "Bus",
  "bus0": "Bus 0",
  "bus1": "Bus 1",
  "mv_grid_id": "MV grid id",
  "lv_grid_id": "LV grid id",
  "v_nom": "Nominal voltage in kV",
  "lat": "Latitude",
  "lon": "Longitude",
  "control": "Type of control",
  "type": "Technology",
  "subtype": "Specific technology",
  "weather_cell_id": "Weather cell id",
  "length": "Length in km",
  "num_parallel": "Parallel lines",
  "subst_id": "Substation id",
  "zensus_sum": "Population",
  "area_ha": "Area in km²",
  "consumption": "Annual consumption in MWh",
  "dea_capacity": "Generation capacity in kW",
  "mv_dea_capacity": "MV generation capacity in kW",
  "lv_dea_capacity": "LV generation capacity kW",
}

display_roundings = {
	"Annual consumption in MWh": 0,
	"Nominal apparent power in kVA": 0,
	"Nominal power in kV": 0,
	"Area in km²": 0,
	"Generation capacity in kW": 0,
	"MV generation capacity in kW": 0,
	"LV generation capacity in kW": 0,
	"x": 5,
	"r": 5,
	"Length in km": 3,
	"Latitude": 6,
	"Longitude": 6,
	}


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

	return feature_collection



def generate_ding0_data(grid_id, save_path):

	engine = db.connection(readonly=True)
	session = sessionmaker(bind=engine)()

	nd = NetworkDing0(name='network')

	# run DING0 on selected MV Grid District
	nd.run_ding0(session=session,mv_grid_districts_no=[grid_id])

	nd.to_csv(save_path)


def create_data_folder(data_path):

	os.makedirs(data_path, exist_ok=True)



if __name__ == '__main__':

	# setup
	import yaml
	y = yaml.load(open("_config.yml"), Loader=yaml.SafeLoader)
	mv_grid_district = y['mv_grid_district_id']
	csv_data_folder = os.path.join('data', 'csv')
	geojson_data_folder = os.path.join('data', 'geojson')

	# create project and data folder
	create_data_folder(csv_data_folder)

	# retrieve mv grid district polygon
	mv_grid_district_polygon = retrieve_mv_grid_polygon(mv_grid_district)
	with open(os.path.join(geojson_data_folder, 'mv_grid_district_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(mv_grid_district_polygon, outfile)

	# generate ding0 data
	ding0_data = generate_ding0_data(mv_grid_district, csv_data_folder)

	# Argparse API
	# grid_is(s) - accepted types: scalar, list, range, file
	# --use-existing-csv='csv-folder' (requires `--data-path`)
	# --data-path
