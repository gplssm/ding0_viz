var color = {"hvmv": "#00b89c", "mvlv": "#008db7", "line": "#9c9c9c", "generator": "#2be555", "load": "#e52b55", "switch": "#020202"}
var props_selection = [
  "Annual consumption in kWh", 
  "Nominal power in kW",
  "Nominal apparent power in kVA",
  "Bus",
  "Bus 0",
  "Bus 1",
  "Nominal voltage in kV",
  "Latitude",
  "Longitude",
  "Type of control",
  "Technology",
  "Specific technology",
  "Weather cell id",
  "Length in km",
  "Parallel lines",
  "Sector",
  "Peak load in kW",
  "Type"];

// Add an SVG element to Leaflet’s overlay pane
var svg = d3.select(map.getPanes().overlayPane).append("svg")
var g = svg.append("g").attr("class", "leaflet-zoom-hide");

var map_lines = svg.append("g").attr("id", "lines");
var map_loads = svg.append("g").attr("id", "loads");
var map_generators = svg.append("g").attr("id", "generators");
var map_points = svg.append("g").attr("id", "transformers");
var map_switches = svg.append("g").attr("id", "switches");

var transform = d3.geoTransform({point: projectPoint}),
path = d3.geoPath().projection(transform);

// Create infoboxes
var Info = d3.select("#info")
  .append("div")
  .attr("class", "p")
  .style("visibility", "hidden");

var district = d3.select("#district")
  .append("div")
  .attr("class", "p")
  .style("visibility", "visible");


function grid_info_box(grid_id) {
d3.json("data/geojson/" + grid_id + "/mv_grid_district_" + grid_id + ".geojson", function(d) {
  
  var grid_description_table = sidebarTable(d.features[0].properties);
    district
      .html("<h3>Medium-voltage grid district " + grid_id + "</h3>" + grid_description_table);
});
};


function plot_points_and_lines(gridid) {
d3.json("data/geojson/" + gridid + "/mv_visualization_line_data_" + gridid + ".geojson", plot_lines)   
d3.json("data/geojson/" + gridid + "/mv_visualization_transformer_data_" + gridid + ".geojson", plot_transformers)
d3.json("data/geojson/" + gridid + "/mv_visualization_generator_data_" + gridid + ".geojson", function(generators_data){
  plot_points(generators_data.features, color["generator"], "generators", 8);
  plot_points(generators_data.features, color["generator"], "generators", 8);})
d3.json("data/geojson/" + gridid + "/mv_visualization_load_data_" + gridid + ".geojson", function(loads_data){
  plot_points(loads_data.features, color["load"], "loads", 8);
  plot_points(loads_data.features, color["load"], "loads", 8);})
d3.json("data/geojson/" + gridid + "/mv_visualization_switch_data_" + gridid + ".geojson", function(switches_data){
  plot_points(switches_data.features, color["switch"], "switches", 4.5);
  plot_points(switches_data.features, color["switch"], "switches", 4.5);
})
};

function plot_transformers(node_data) {

  // // Filter data
  hvmv_trafos = node_data.features.filter( function(d){return d.properties["Nominal voltage in kV"] == 110} )
  mvlv_trafos = node_data.features.filter( function(d){return d.properties["Nominal voltage in kV"] == 0.4} )

  plot_points(hvmv_trafos, color["hvmv"], "transformers", 10);
  plot_points(mvlv_trafos, color["mvlv"], "transformers", 8);

  // For some stupid reason I have to plot it twice :-/
  plot_points(hvmv_trafos, color["hvmv"], "transformers", 10);
  plot_points(mvlv_trafos, color["mvlv"], "transformers", 8);    
};

function updateSVG(data) {

      var bounds = path.bounds({
        type: "FeatureCollection",
        features: data
      });

      var offset = 500;
      var topLeft = bounds[0] + offset;
      var bottomRight = [bounds[1][0] + offset, bounds[1][1] + offset];

      svg.attr("width", bottomRight[0] - topLeft[0])
        .attr("height", bottomRight[1] - topLeft[1])
        .style("left", topLeft[0] + "px")
        .style("top", topLeft[1] + "px");

      // Alternative SVG positioning: take bounds from div #map
      // console.log(bounds);
      // map_height = console.log(d3.select("#map").style('width'))
      // map_width = console.log(d3.select("#map").style('height'))
      // svg.attr("width", 465)
      //   .attr("height", 980)
      //   .style("left", 0 + "px")
      //   .style("top", 0 + "px")
    };

function plot_lines(lines_data) {
  updateSVG(lines_data.features);
  edges = map_lines.selectAll("path")
                .data(lines_data.features)
                .enter()
                .append("path")
                .style("pointer-events", "all")
                .attr("d", function(d){
                  return path(d)})
                .attr("class", "lines")
                .style("fill", color["line"])
    .style("stroke", color["line"])
    .style("stroke-width", 7)
    .on("mouseover", onmouseover_lines)
    .on("mouseout", onmouseout_lines);

    map.on("moveend", updateLines);

    function updateLines(e) {

      edges.attr("d", function(d){
                  return path(d)});
    }
}

function plot_points(subset, color, selection, r) {
      path.pointRadius(r)
      // updateSVG(subset);
      var points_svg = svg.selectAll("g#" + selection).selectAll("path")
      .data(subset, function(d) {
        return d.geometry.coordinates;
      })
      .style("pointer-events", "all")
      .style("stroke-opacity", ".5")
      .style("stroke", color)
      .attr("fill", color)
      .on("mouseover", onmouseover_points)
      .on('mouseout', onmouseout_points);  

    points_svg.enter().append("path");
    points_svg.attr("d", path).attr("class", selection);

    map.on("moveend", updatePointsInternal);

    function updatePointsInternal(e) {
      path.pointRadius(r)
      points_svg.attr("d", path);
    }
  }



function projectPoint(x, y) {
    var point = map.latLngToLayerPoint(new L.LatLng(y, x));
    this.stream.point(point.x, point.y);
  };

function onmouseover_points(d, i) {
        const table_data = d.properties;
        const selected_data = Object.keys(table_data)
          .filter(key => props_selection.includes(key))
          .reduce((obj, key) => {
            obj[key] = table_data[key];
            return obj;
          }, {});
        
        var table_str = sidebarTable(selected_data);

        Info.style("visibility", "visible")
        .html("<h5>" + table_data["name"] + "</h5>" + table_str);
        d3.select(this)
          .transition()
          .duration(200)
          .style("fill-opacity", ".7")
          .style("stroke-width", 3);
      };

function onmouseover_lines(d, i) {

      const table_data = d.properties;
        const selected_data = Object.keys(table_data)
          .filter(key => props_selection.includes(key))
          .reduce((obj, key) => {
            obj[key] = table_data[key];
            return obj;
          }, {});
      var table_str = sidebarTable(selected_data);

      Info.style("visibility", "visible")
      .html(("<h5>" + table_data["index"] + "</h5>" + table_str));
      d3.select(this)
        .transition()
        .duration(200)
        .style("stroke-width", "9")
        .style("stroke-opacity", ".7");
    };

function onmouseout_points(d, i) {
      Info.style('visibility', 'hidden');
      d3.select(this)
        .transition()
        .duration(200)
        .style("fill-opacity", "1")
        .style("stroke-width", "0")
      Info.style('visibility', 'hidden');
    };

function onmouseout_lines(d, i) {
    Info.style('visibility', 'hidden');
    d3.select(this)
      .transition()
      .duration(200)
      .style("stroke-opacity", "1")
      .style("stroke-width", "7");
  };
