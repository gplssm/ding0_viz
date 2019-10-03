// Create background map with Leaftlet
function backgroundmap (mv_grid_district_id) {
  var map = L.map('map').setView([47, 2], 5);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd'
  }).addTo(map);

  // use mv grid district bounds to reset background map
  polygon = "data/mv_grid_district_" + mv_grid_district_id + ".geojson"
  d3.json(polygon, function(geoShape) {

    var b = d3.geoBounds(geoShape);
    var bounds = [[b[0][1], b[0][0]], [b[1][1], b[1][0]]]

    // for debugging
    // L.rectangle(bounds, {color: "#ff7800", weight: 1}).addTo(map);
    map.fitBounds(bounds);
  });
  return map
}