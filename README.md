# Ding0 visualization

Visualize synthetic distribution grid data generated by [ding0](https://dingo.readthedocs.io) with [D3](https://d3js.org).

## Installation

Create a virtual environment for python

```
conda create -n dingo_visualization python=3 
conda activate dingo_visualization
pip install -r python_requirements.txt
```
and you're ready to retrieve and process data.

For generating and deploying the site with Jekyll, you need to have [Ruby](https://jekyllrb.com/docs/installation/) installed.
To install it, run 

```
sudo apt install ruby-bundler
```

To install Ruby dependencies, run

```
bundle
```

and packages specified in `Gemfile` are installed.

In addition install jekyll (might require sudo)

```
gem install jekyll bundler
```

### Trouble shooting

During the installation of ruby dependencies (`bundle`) you might encounter the following error

```
An error occurred while installing eventmachine (1.2.7), and Bundler cannot
continue.
Make sure that `gem install eventmachine -v '1.2.7'` succeeds before bundling.
```

Install the following

```
sudo apt-get install ruby-dev
```


## Retrieve data

Use the command-line interface, for example

```
python utils/retrieve_data.py --grid_id 632 --csv_data_path /path/to/save/data
```

You can also the `_config.yml` file or a custom config file. See the help with `python utils/retrieve_data.py`.

Order of input processing (top overwrites bottom)

1. Command-line arguments
2. Custom conifg file
3. Default config file `_config.yml`
Deploy and serve
----------------

Run

```
./DEPOY
```

to build site (actually just copying), serve the site at [localhost:4000](http://localhost:4000), and immediately open it up in your browser.



License
=======

MIT License

Copyright (c) 2019 gplssm

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
