var host="localhost";
var port=8080;

var tweeterSel={"values":[]};
var hashtagSel={"values":[]};
var timeRangeSel={"values":[]};

var selmap = {"timeRange":timeRangeSel,"tweeter":tweeterSel,"hashtags":hashtagSel};

var valmap = {"timeRange":{"000000100":"Last Minute","000010000":"Last Hour","001000000":"Last Day"}}

var senseiReq = {};

senseiReq.fetchStored = true;

senseiReq.sort = [{"time":"desc"},"_score"];

senseiReq.selections = [
{
  "terms":{
    "timeRange":timeRangeSel
  }
},
{
  "terms":{
    "tweeter":tweeterSel
  }
},
{
  "terms":{
    "hashtags":hashtagSel
  }
}
];

senseiReq.facets = {};

senseiReq.facets.timeRange={"expand":true};

senseiReq.facets.tweeter={"expand":true};

senseiReq.facets.hashtags={"expand":true};


setSenseiQueryString(senseiReq,"");

var repVal = function(arr,s){
  var found =false;
  for (var i=0;i<arr.length;++i){
    if (s==arr[i]){
        arr.splice(i,1);
        found = true;
        break;
    }
  }
  if (!found){
    arr.push(s);
  }
}

function handleSelected(name,facetVal){
	var sel = selmap[name];
	var valArray = sel["values"];
	repVal(valArray,facetVal.value);
	doSearch();
}

function clearSelection(name){
  console.log("clear: "+name);
  var sel = selmap[name];
  var valArray = sel["values"];
  valArray.length=0;
  doSearch();
}

function renderHits(hits){
	$('#results').empty();
	for (var i=0;i<hits.length;++i){
		var html = '<div class="row">';
    	var hit = hits[i];

		html += '<div class="span3">'+hit._uid+"</div>"

		html += '<div class="span3">'+hit._score+"</div>"

    var date = new Date(hit.time*1000);
		html += '<div class="span3">'+date+'</div>'

    var srcdata = extractSrcData(hit);
    
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

	
	$("#time").empty();
	$("#time").append(senseiResult.time/1000);

	var facets = senseiResult.facets;

	for (var name in facets){
		renderFacet(name,facets[name],handleSelected,clearSelection,valmap);
	}

  renderHits(senseiResult.hits);
    
}

function doSearch(){
  console.log("req: "+senseiReq);
	executeSenseiReq(host,port,senseiReq,renderPage);
}


function updateTextQuery(){
	var q = $('#qbox').val();
	setSenseiQueryString(senseiReq,q);
	doSearch();
}

function resetAll(){
	$('#qbox').val("");
  senseiReq.selections = [];
  setSenseiQueryString(senseiReq,"");
  for (var sel in selmap){
    selection["values"].length = 0;
  }

  doSearch();
}
