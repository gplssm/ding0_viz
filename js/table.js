function sidebarTable(data) {

	var table_str = "<table>";
	for (var property1 in data) {
		table_str += "<tr>"
	  table_str += "<td>" + property1 + "</td><td>"  + data[property1] + "</td>";
		table_str += "</tr>"
	}
	table_str += "</table>"
	
	return table_str;
};

