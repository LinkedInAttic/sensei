var host="localhost";
var port=8080;

var queryString = {"query" : ""};
var senseiReq = {"query":{"query_string":queryString}};

senseiReq.selections = [];
senseiReq.facets = {};

senseiReq.facets.color={
	
};

senseiReq.facets.category={};

senseiReq.facets.year={};

senseiReq.facets.price={};


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

function renderPage(senseiResult){
	console.log(senseiResult.numhits)

	var facets = senseiResult.facets;

	for (var name in facets){
		renderFacet(name,facets[name])
	}

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
    queryString['query'] = "";
    senseiReq.selections = [];
}
