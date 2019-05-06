/**
 * 
 */
function MySqlInputs(){
	var w = document.getElementById("mysqlurl").value;
	var x = document.getElementById("mysqldb").value;
	var y = document.getElementById("mysqlusername").value;
	var z = document.getElementById("mysqlpassword").value;
	w = "URL: " + w;
	x = "Database: " + x ;
	y = "Username: " + y;
	var outstr = w + "\n" + x + "\n" + y
	alert(outstr);
}