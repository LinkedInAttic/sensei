function executeSenseiReq(host,port,req,callback){
  var url = "http://"+host+":"+port+"/sensei";
  $.post(url,JSON.stringify(req),callback);
}

function extractSrcData(senseiHit){
  return eval('('+senseiHit._srcdata+')');
}

function setSenseiQueryString(req,queryString){
  req["query"]={"query_string":{"query":queryString}};
  console.log("q:"+req);
}

function renderFacet(name,facet,selectionHandle,clearSelection,valmap){
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

			var dispVal = facet[i].value;
			if (valmap!=null){
				var convertedFacet = valmap[name];
				if (convertedFacet!=null){
					var convertedVal = convertedFacet[dispVal];
					if (convertedVal!=null){
					  dispVal = convertedVal;
				    }
				}
			}

			html = '<input type="checkbox"> '+dispVal+' ('+facet[i].count+')</input>';
			node.append(html);
			var obj = node.children().last().get(0);
			obj._name = name;
			obj._facetVal = facet[i];
			if (facet[i].selected){
				obj.checked ="checked";
			}
			node.children().last().click(function(e){
				selectionHandle(this._name,this._facetVal);
			});

			node.append('<br/>');
		}
	}
}
