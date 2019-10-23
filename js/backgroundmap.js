// Create background map with Leaftlet
var map = L.map('map').setView([47, 2], 5);
var grid_info;
function backgroundmap (mv_grid_district_id) {

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd'
  }).addTo(map);

  // use mv grid district bounds to reset background map
  var polygon = "data/geojson/" + mv_grid_district_id + "/mv_grid_district_" + mv_grid_district_id + ".geojson"
  d3.json(polygon, read_shape);  
}

function setMapBounds (map, shape) {
  var b = d3.geoBounds(shape);
    var bounds = [[b[0][1], b[0][0]], [b[1][1], b[1][0]]]

    // for debugging
    // L.rectangle(bounds, {color: "#ff7800", weight: 1}).addTo(map);
    map.fitBounds(bounds);
}
  
L.control.scale().addTo(map);

function read_shape(shape) {
  // console.log(shape);
  setMapBounds(map, shape);
}
