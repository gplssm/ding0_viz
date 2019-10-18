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


def to_geojson(data, geom_type):
	"""Convert JSON to GeoJSON"""

	collection = []

	for dat in data:
		if geom_type == 'MultiPolygon':
			coordinates = MultiPolygon(dat['coordinates'])
		elif geom_type == 'Point':
			coordinates = Point(dat['coordinates'])
		elif geom_type == 'LineString':
			coordinates = LineString(dat['coordinates'])
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

	os.makedirs(folder, exist_ok=True)


def geom_to_coords(geom):

	coordinates_shp = loads(geom, hex=True)
	coordinates = [coordinates_shp.x, coordinates_shp.y]

	return coordinates


def reformat_ding0_grid_data(bus_file, transformer_file, generators_file, lines_file):

	buses = pd.read_csv(bus_file)
	transformers = pd.read_csv(transformer_file)
	# hvmv_transformers = transformers[transformers['s_nom'] > 1000]
	# mvlv_transformers = transformers[transformers['s_nom'] <= 1000]
	lines = pd.read_csv(lines_file)
	generators = pd.read_csv(generators_file)

	geo_referenced_buses = buses.loc[~buses['geom'].isna(), 'geom']
	geo_referenced_buses = pd.DataFrame(geo_referenced_buses.apply(geom_to_coords).rename('coordinates'), index=geo_referenced_buses.index)
	geo_referenced_buses['lat'] = geo_referenced_buses['coordinates'].apply(lambda x: x[0])
	geo_referenced_buses['lon'] = geo_referenced_buses['coordinates'].apply(lambda x: x[1])
	
	buses = (buses.join(geo_referenced_buses, how='inner')).set_index('name')

	transformers_df = transformers.join(buses, on='bus0', how='inner').rename(
		columns=display_names).round(display_roundings).fillna('NaN')
	transformers_dict = transformers_df.to_dict(orient='records')

	lines_df_0 = lines.join(buses, on='bus0', how='inner').rename(columns={'coordinates': 'coordinates_0'}).set_index('name')
	lines_df_1 = lines.join(buses, on='bus1', how='inner').rename(columns={'coordinates': 'coordinates_1'}).set_index('name')
	lines_df = pd.concat([lines_df_0, lines_df_1], axis=1, sort=True).dropna(subset=['coordinates_0', 'coordinates_1'])
	lines_df['coordinates'] = [[tuple(row['coordinates_0']), tuple(row['coordinates_1'])] for it, row in lines_df.iterrows()]
	lines_df = lines_df[lines_df.columns[~lines_df.columns.str.endswith('_0')]]
	lines_df = lines_df.reset_index()

	lines_df_processed = lines_df.loc[:,~lines_df.columns.duplicated()]	
	lines_dict = lines_df_processed.fillna('NaN').rename(
		columns=display_names).round(display_roundings).to_dict(orient='records')


	generators_df = generators.join(buses, on='bus', how='inner').fillna('NaN').rename(
		columns=display_names).round(display_roundings)
	generators_dict = (generators_df[generators_df['Nominal voltage in kV'] < 110]).to_dict(orient='records')

	return transformers_dict, generators_dict, lines_dict


def list_available_grid_data(path):



    dirs = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]

    with open(os.path.join(data_folder, 'available_grid_data.txt'), 'w') as f:
	    f.write("gridids\n")
	    for item in dirs:
	        f.write("%s\n" % item)




if __name__ == '__main__':

	# setup
	import yaml
	y = yaml.load(open("_config.yml"), Loader=yaml.SafeLoader)
	mv_grid_district = y['mv_grid_district_id']
	data_folder = 'data'

	# # create project and data folder
	create_data_folder()

	# # retrieve mv grid district polygon
	mv_grid_district_polygon = retrieve_mv_grid_polygon(mv_grid_district)
	with open(os.path.join(data_folder, 'mv_grid_district_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(mv_grid_district_polygon, outfile)

	# # generate ding0 data
	ding0_data = generate_ding0_data(mv_grid_district)
	ding0_data.to_csv(os.path.join(data_folder, 'ding0'))

	# reformat ding0 data and save
	ding0_node_data_reformated, \
	ding0_generator_data_reformated, \
	ding0_line_data_reformated = reformat_ding0_grid_data(
		os.path.join(data_folder, 'ding0', str(mv_grid_district), 'buses_{}.csv'.format(mv_grid_district)),
		os.path.join(data_folder, 'ding0', str(mv_grid_district), 'transformers_{}.csv'.format(mv_grid_district)),
		os.path.join(data_folder, 'ding0', str(mv_grid_district), 'generators_{}.csv'.format(mv_grid_district)),
		os.path.join(data_folder, 'ding0', str(mv_grid_district), 'lines_{}.csv'.format(mv_grid_district))
		)
	ding0_node_data_geojson = to_geojson(ding0_node_data_reformated, geom_type='Point')
	ding0_generator_data_geojson = to_geojson(ding0_generator_data_reformated, geom_type='Point')
	ding0_line_data_geojson = to_geojson(ding0_line_data_reformated, geom_type='LineString')
	with open(os.path.join(data_folder, 'ding0', str(mv_grid_district), 'mv_visualization_node_data_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(ding0_node_data_geojson, outfile)
	with open(os.path.join(data_folder, 'ding0', str(mv_grid_district), 'mv_visualization_generator_data_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(ding0_generator_data_geojson, outfile)
	with open(os.path.join(data_folder, 'ding0', str(mv_grid_district), 'mv_visualization_line_data_{}.geojson'.format(mv_grid_district)), 'w') as outfile:
	    json.dump(ding0_line_data_geojson, outfile)

	# Write list of available grid data
	list_available_grid_data(os.path.join(data_folder, 'ding0'))
