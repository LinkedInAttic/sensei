var host="localhost";
var port=8080;


var colorSel={"values":[]};
var categorySel={"values":[]};
var priceSel={"values":[]};
var yearSel={"values":[]};
var mileageSel={"values":[]};
var tagsSel={"values":[]};
var makemodelSel={"value":""};
var citySel={"value":""};

var selmap = {"color":colorSel,"category":categorySel,"price":priceSel,"year":yearSel,"mileage":mileageSel,"tags":tagsSel,"makemodel":makemodelSel,"city":citySel};

var senseiReq = {"query":{"query_string":queryString}};

var fieldSort = null;

senseiReq.fetchStored = true;

senseiReq.sort = ["_score"];

senseiReq.selections = [
{
  "terms":{
    "color":colorSel
  }
},
{
  "terms":{
    "category":categorySel
  }
},
{
  "terms":{
    "price":priceSel
  }
},
{
  "terms":{
    "year":yearSel
  }
},
{
  "terms":{
    "mileage":mileageSel
  }
},
{
  "path":{
    "makemodel":makemodelSel
  }
},
{
  "path":{
    "city":citySel
  }
},
{
  "terms":{
    "tags":tagsSel
  }
}
];

var queryString = {"query" : ""};

senseiReq.facets = {};

senseiReq.facets.color={"expand":true};

senseiReq.facets.category={"expand":true};

senseiReq.facets.year={"expand":true};

senseiReq.facets.price={"expand":true};

senseiReq.facets.mileage={"expand":true};

senseiReq.facets.tags={"expand":true};

senseiReq.facets.city={};
senseiReq.facets.makemodel={};

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
	console.log(name+','+facetVal.value);
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

function renderPath(field, objs) {
  if (!objs)
    return;

  var headContainer = $('#'+field+' .ROWHEAD span').empty();

  var div = $('#'+field).get(0);
  var vals = [];
  var sel = selmap[field];
  if (sel["value"].length != 0) {
    var val = sel["value"];
    var _vals = val.split('/');
    for (var i=0; i<_vals.length; ++i) {
      if (_vals[i] != '') {
        vals.push([_vals[i]]);
        var v = [];
        for (var j=0; j<vals.length; ++j) {
          v.push(vals[j][0]);
        }
        vals[vals.length-1].push(v.join('/'));
      }
    }
  }
  vals = [['All', '']].concat(vals);
  for (var i=0; i<vals.length; ++i) {
    headContainer.append('<a />').find('a:last')
      .text(vals[i][0])
      .click(function (e) {
        sel["value"] = this.value;
        doSearch();
      }).get(0).value = vals[i][1];
    headContainer.append('/');
  }

  var container = $('#'+field+' table table tr').empty();
  for (var i=0; i<objs.length; ++i) {
    var obj = objs[i];
    if (obj.selected)
      continue;

    var _v = obj.value.split('/');
    var v = _v.pop();
    while (v=='' && _v.length)
      v = _v.pop();
    container.append('<td />').find('td:last')
      .append('<a />').find('a:last')
      .text(v+' ('+obj.count+')')
      .click(function (e) {
        sel["value"] = this.value;
        doSearch();
      }).get(0).value = obj.value;
  }
}

function renderFacet(name,facet){
	var node = $("#"+name);
	if (node != null){
		node.empty();
		
		node.append('<input type="checkbox"> All</input>');
		var allObj = node.children().last().get(0);
		allObj._name = name;

		var sel = selmap[name];
	    var valArray = sel["values"];
	    if (valArray.length==0){
	    	allObj.checked="checked";
	    }

		node.children().last().click(function(e){
				clearSelection(this._name);
		});
		node.append('<br/>');

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
		var html = '<tr>';
    	var hit = hits[i];

    	var score = hit._score;
    	var id = hit._uid;
    	var color = hit.color[0];
    	var category = hit.category[0];
    	var year = hit.year[0];
    	var price = hit.price[0];
    	var mileage = hit.mileage[0];

		html += '<td>'+id+'</td>';
		html += '<td>'+color+'</td>';
		html += '<td>'+category+'</td>';
		html += '<td>'+parseFloat(year)+'</td>';
		html += '<td>'+parseFloat(price)+'</td>';
		html += '<td>'+parseFloat(mileage)+'</td>';
		html += '<td>'+score+'</td>';

		html += '</tr>';
    $('#results').append(html);
  }
}

function renderPage(senseiResult){
	console.log(senseiResult.numhits);


	$("#numhits").empty();
	$("#numhits").append(senseiResult.numhits);


	$("#totaldocs").empty();
	$("#totaldocs").append(senseiResult.totaldocs);


	var facets = senseiResult.facets;

	for (var name in facets){
	  if (name=='makemodel' || name=='city'){
	    renderPath(name,facets[name]);
	  }
	  else{
		  renderFacet(name,facets[name]);
	  }
	}

    renderHits(senseiResult.hits);
}

function doSearch(){
	executeSenseiReq(host,port,senseiReq,renderPage);
}

function updateTextQuery(){
	var q = $('#qbox').val();
	setSenseiQueryString(senseiReq,q);
	doSearch();
}

function resetAll(){
	$('#qbox').val("");
    for (var sel in selmap){
    	var selection = selmap[sel];
    	selection["values"].length = 0;
    }
    setSenseiQueryString(senseiReq,"");
	doSearch();
}

function toggleSort(name){
	console.log('sort:'+name)
	if (fieldSort==null || fieldSort[name]==null){
		fieldSort = {};
		fieldSort[name] = 'desc';
		senseiReq.sort=[fieldSort,"_score"]
	}
	else{
		var dir = fieldSort[name];
		if ('desc'==dir){
			fieldSort[name]='asc';
		}
		else{
			fieldSort[name]='desc';
		}
	}
	doSearch();
}
