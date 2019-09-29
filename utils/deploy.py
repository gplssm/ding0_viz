import os
from shutil import copyfile


def deploy(target):
	file = 'index.html'
	copyfile(os.path.join(os.getcwd(), file), os.path.join(target, file))

if __name__ == '__main__':
	project_folder = os.path.join(os.path.expanduser('~'), 'projects', 'ding0_visualization_v1')

	deploy(project_folder)
