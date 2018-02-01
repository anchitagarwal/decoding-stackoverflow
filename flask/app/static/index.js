// function to load posts page for the user
function load_posts_page() {
	var userId = $('#user').attr('value')
	if (userId != "Choose...") {
		var present_url = window.location.pathname;
    	var rest = present_url.substring(0, present_url.lastIndexOf("/") + 1);
    	var new_url = rest + 'posts' + '?userId=' + userId;
        window.location.href = new_url;
	}
}
