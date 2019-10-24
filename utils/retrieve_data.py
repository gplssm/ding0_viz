import os
from ding0.core import NetworkDing0
from utils.process_data import read_config_yaml, to_list_of_ints
from egoio.tools import db
from sqlalchemy.orm import sessionmaker
import argparse


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
		# generate ding0 data
		ding0_data = generate_ding0_data(g, settings['csv_data_path'])
