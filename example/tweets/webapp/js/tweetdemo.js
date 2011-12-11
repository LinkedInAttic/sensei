var host="localhost";
var port=8080;

var queryString = {"query" : ""};
var senseiReq = {"query":{"query_string":queryString}};

senseiReq.fetchStored = true;

senseiReq.sort = [{"time":"desc"},"_score"];

senseiReq.selections = [];
senseiReq.facets = {};

senseiReq.facets.time={};

senseiReq.facets.tweeter={};

senseiReq.facets.hashtags={};


var senseiClient = new SenseiClient(host,port)

function handleSelected(name,facetVal){
	console.log(name+','+facetVal.value);
}

function renderFacet(name,facet){
	var node = $("#"+name);
	if (node != null){
		node.empty();
		for (var i=0;i<facet.length;++i){
			var html;

			html = '<input type="checkbox"> '+facet[i].value+' ('+facet[i].count+')</input>';
			node.append(html);
			var obj = node.children().last().get(0);
			obj._name = name;
			obj._facetVal = facet[i];
			if (facet[i].selected){
				obj.checked ="checked";
			}
			node.children().last().click(function(e){
				handleSelected(this._name,this._facetVal);
			});

			node.append('<br/>');
		}
	}
}

function renderHits(hits){
	$('#results').empty();
	for (var i=0;i<hits.length;++i){
		var html = '<div class="row">';
    	var hit = hits[i];

		html += '<div class="span3">'+hit._uid+"</div>"

		html += '<div class="span3">'+hit.score+"</div>"

        var date = new Date(hit.time);
		html += '<div class="span3">'+date+"</div>"

    	var srcdata = eval('('+hit.srcdata+')');

        var tweet = srcdata.tweet;
        var user = tweet.user;
        var imgUrl = user.profile_image_url_https;
        var text = tweet.text;


        var tweeter = srcdata.tweeter;

        html += '<div class="span3"><a href="'+imgUrl+'"><img src="'+imgUrl+'"/></a></div>';

        html += '<div class="span6">'+tweeter+'<br/>'+text+'</div>';
		html += '</div>';
    	$('#results').append(html);
    	console.log(srcdata.tweet);
    }
}

function renderPage(senseiResult){
	console.log(senseiResult.numhits)

	$("#numhits").empty();
	$("#numhits").append(senseiResult.numhits);


	$("#totaldocs").empty();
	$("#totaldocs").append(senseiResult.totaldocs);

	var facets = senseiResult.facets;

	for (var name in facets){
		renderFacet(name,facets[name])
	}

    renderHits(senseiResult.hits);
    
}

function doSearch(){
	senseiClient.doQuery(senseiReq,renderPage);
}


function updateTextQuery(){
	var q = $('#qbox').val();
	queryString['query'] = q;
	doSearch();
}

function resetAll(){
	$('#qbox').val("");
    queryString['query'] = "";
    senseiReq.selections = [];
    doSearch();
}
