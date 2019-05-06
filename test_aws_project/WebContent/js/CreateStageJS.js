/**
 * 
 */
function CreateStage(){
	var w = document.getElementById("s3bucketname").value;
	var x = document.getElementById("s3keyname").value;
	var y = document.getElementById("s3region").value;
	w = "Bucket Name: " + w;
	x = "Key Name: " + x ;
	y = "Region: " + y;
	var outstr = w + "\n" + x + "\n" + y
	alert(outstr);
}
