/**
 * 
 */
function BeginUpload(){
	
	$.ajax({url: "/test_aws_project/BeginUpload", type: POST, async: false, 
        success: function(result){
            // Fill the grid dynamically using result
        	alert("success");
        },
        error: function(data) {
            alert('woops!');
        } 
});
		alert("Beginning Upload!");

	
}