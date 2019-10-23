var mv_grid_district_id = "{{ site.mv_grid_district_id }}"
var color = {"hvmv": "#00b89c", "mvlv": "#008db7", "line": "#9c9c9c", "generator": "#2be555"}


// Add an SVG element to Leaflet’s overlay pane
var svg = d3.select(map.getPanes().overlayPane).append("svg")
var g = svg.append("g").attr("class", "leaflet-zoom-hide");

var map_lines = svg.append("g").attr("id", "lines");
var map_generators = svg.append("g").attr("id", "generators");
var map_points = svg.append("g").attr("id", "transformers");

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


function grid_info_box(mv_grid_district_id) {
d3.json("data/geojson/" + mv_grid_district_id + "/mv_grid_district_" + mv_grid_district_id + ".geojson", function(d) {
  props = d.features[0].properties;
  delete props["area_share"];
  delete props["consumption_per_area"];
  delete props["dea_cnt"];
  delete props["free_area"];
  delete props["gem_clean"];
  delete props["group"];
  delete props["la_area"];
  delete props["la_count"];
  delete props["lv_dea_cnt"];
  delete props["population_density"];
  delete props["subst_sum"];
  delete props["type1"];
  delete props["type1_cnt"];
  delete props["type2"];
  delete props["type2_cnt"];
  delete props["type3"];
  delete props["type3_cnt"];
  delete props["version"];
  delete props["zensus_count"];
  delete props["zensus_density"];
  delete props["gem"];
  delete props["mv_dea_cnt"];
  var grid_description_table = sidebarTable(props);
    district
      .html("<h3>Medium-voltage grid district " + mv_grid_district_id + "</h3>" + grid_description_table);
});
};


function plot_points_and_lines(gridid) {
d3.json("data/geojson/" + gridid + "/mv_visualization_line_data_" + gridid + ".geojson", plot_lines)   
d3.json("data/geojson/" + gridid + "/mv_visualization_transformer_data_" + gridid + ".geojson", plot_transformers)
d3.json("data/geojson/" + gridid + "/mv_visualization_generator_data_" + gridid + ".geojson", function(generators_data){
  plot_points(generators_data.features, color["generator"], "generators");
  plot_points(generators_data.features, color["generator"], "generators");
})};

function plot_transformers(node_data) {

  // // Filter data
  hvmv_trafos = node_data.features.filter( function(d){return d.properties["Nominal voltage in kV"] == 110} )
  mvlv_trafos = node_data.features.filter( function(d){return d.properties["Nominal voltage in kV"] == 400} )

  plot_points(hvmv_trafos, color["hvmv"], "transformers");
  plot_points(mvlv_trafos, color["mvlv"], "transformers");

  // For some stupid reason I have to plot it twice :-/
  plot_points(hvmv_trafos, color["hvmv"], "transformers");
  plot_points(mvlv_trafos, color["mvlv"], "transformers");    
};

function updateSVG(data) {
      path.pointRadius(10);

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

function plot_points(subset, color, selection) {

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

      points_svg.attr("d", path);
    }
  }



function projectPoint(x, y) {
    var point = map.latLngToLayerPoint(new L.LatLng(y, x));
    this.stream.point(point.x, point.y);
  };

function onmouseover_points(d, i) {
        var table_data = d.properties;      
        delete table_data["name"];
        delete table_data["in_building"];
        
        var table_str = sidebarTable(table_data);

        Info.style("visibility", "visible")
        .html("<h5>" + d.properties.name + "</h5>" + table_str);
        d3.select(this)
          .transition()
          .duration(200)
          .style("fill-opacity", ".7")
          .style("stroke-width", 3);
      };

function onmouseover_lines(d, i) {

      var table_data = d.properties;
      const index = d.properties.index;
      delete table_data["lv_grid_id"];
      delete table_data["coordinates_1"];
      delete table_data["index"];
      delete table_data["in_building"];
      var table_str = sidebarTable(table_data);

      Info.style("visibility", "visible")
      .html(("<h5>" + index + "</h5>" + table_str));
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
