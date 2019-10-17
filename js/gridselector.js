var gridids = [];

d3.csv("data/available_grid_data.txt", function(csv) {
	var sel = document.getElementById('gridselector');
	var fragment = document.createDocumentFragment();

	csv.map(function(d){
        gridids.push(+d.gridids);
    })

	gridids.forEach(function(gridid, index) {
	    var opt = document.createElement('option');
	    opt.innerHTML = gridid;
	    opt.value = gridid;
	    fragment.appendChild(opt);
	});
	sel.appendChild(fragment);
});