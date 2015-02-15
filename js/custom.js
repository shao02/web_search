$(function(){
    $('#query').typeahead({
        items:7,
        source: function (i, process) {
            var first_char=i.charAt(0);
            if (first_char != '"') {
                var a = $.getJSON(
                    '/js/lookup_'+first_char+'_.json',
                    { query: i },
                    function (data) {
                        return process(data);
                    }
                );
            }
        }
    });
    $('#query').keyup($.debounce(500, ajax_lookup));
});
var req = null;
function ajax_lookup(event) {
    query = $('#query').val();
    console.log("send ajax request for " + query);
    if (req != null) req.abort();
    req = $.ajax("/search?format=results&query=" + query)
                .done(function(msg){
                    $("#search_results").replaceWith(msg);
                });
}
function search_func(value)
{

}