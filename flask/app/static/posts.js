$(document).ready(function() {
	var userId = window.location.search.split("=")[1];
	var url = "/api/" + userId;
	getApi(url, populate_posts);
});

function getApi(url, callback) {
	$.get(url)
    	.done(function (data) {
        	callback(JSON.parse(data));
    	})
    	.fail(function(jqXHR, textStatus, errorThrown) {
        	console.log(jqXHR.responseText);
	});
}

function populate_posts(response) {
	// populate user information
	$('#userName').text(response['user'].name)
	$('#userId').text(response['user'].id)
	$('#location').text(response['user'].location)
	$('#reputation').text(response['user'].reputation)
	$('#upVotes').text(response['user'].upVotes)
	$('#downVotes').text(response['user'].downVotes)
	$('#views').text(response['user'].views)

	// populate posts
	var p1 = response[0]
	var p2 = response[1]
	var p3 = response[2]
	var p4 = response[3]
	var p5 = response[4]

	$("#collapseOne .card-body").html(
		"<span class=\"badge badge-info\">Android</span> <span class=\"badge badge-info\">Java</span> \
		<br><a style=\"margin-top: 10px;\" \
		href=https://stackoverflow.com/questions/" + p1.id + " id=\"collapseOneBtn\" \
		 target=\"_blank\" class=\"btn btn-link btn-sm\">" + p1.title + "</button>"
	)

	$("#collapseTwo .card-body").html(
		"<span class=\"badge badge-info\">php</span> \
		<br><a style=\"margin-top: 10px;\" \
		href=https://stackoverflow.com/questions/" + p2.id + " id=\"collapseOneBtn\" \
		 target=\"_blank\" class=\"btn btn-link btn-sm\">" + p2.title + "</button>"
	)

	$("#collapseThree .card-body").html(
		"<span class=\"badge badge-info\">android</span> \
		<br><a style=\"margin-top: 10px;\" \
		href=https://stackoverflow.com/questions/" + p3.id + " id=\"collapseOneBtn\" \
		 target=\"_blank\" class=\"btn btn-link btn-sm\">" + p3.title + "</button>"
	)

	$("#collapseFour .card-body").html(
		"<span class=\"badge badge-info\">android</span> \
		<br><a style=\"margin-top: 10px;\" \
		href=https://stackoverflow.com/questions/" + p4.id + " id=\"collapseOneBtn\" \
		 target=\"_blank\" class=\"btn btn-link btn-sm\">" + p4.title + "</button>"
	)

	$("#collapseFive .card-body").html(
		"<span class=\"badge badge-info\">android</span> \
		<br><a style=\"margin-top: 10px;\" \
		href=https://stackoverflow.com/questions/" + p5.id + " id=\"collapseOneBtn\" \
		 target=\"_blank\" class=\"btn btn-link btn-sm\">" + p5.title + "</button>"
	)
}

