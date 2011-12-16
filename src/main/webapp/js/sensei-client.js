// Is a variable is defined.
function isDefined(object, variable){
	return (typeof(eval(object)[variable]) == 'undefined')? false : true;
};

// String trim function.
String.prototype.trim = function() { return this.replace(/^\s+|\s+$/g, ''); };

// SenseiFacet(name, expand=true, minHits=1, maxCounts=10, orderBy=HITS)
var SenseiFacet = function () {
	if (arguments.length == 0) return null;

	this.expand = true;
	this.minHits = 1;
	this.maxCounts = 10;
	this.orderBy = this.OrderBy.HITS;

	this.name = arguments[0];
	if (arguments.length > 1)
		this.expand = arguments[1];
	if (arguments.length > 2)
		this.minHits = arguments[2];
	if (arguments.length > 3)
		this.maxCounts = arguments[3];
	if (arguments.length > 4)
		this.orderBy = arguments[4];
};

SenseiFacet.prototype = {
	OrderBy: {
		HITS: "hits",
		VALUE: "val"
	}
};

var SenseiProperty = function (key, val) {
	this.key = key;
	this.val = val;
};

// SenseiSelection(name, values="", excludes="", operation=OR)
var SenseiSelection = function () {
	if (arguments.length == 0) return null;

	this.values = "";
	this.excludes = "";
	this.operation = this.Operation.OR;
	this.properties = [];

	this.name = arguments[0];
	if (arguments.length > 1)
		this.values = arguments[1];
	if (arguments.length > 2)
		this.excludes = arguments[2];
	if (arguments.length > 3)
		this.operation = arguments[3];
};

SenseiSelection.prototype = {
	Operation: {
		OR: "or",
		AND: "and"
	},

	addProperty: function (key, val) {
		for (var i=0; i<this.properties.length; ++i) {
			if (this.properties[i].key == key) {
				this.properties[i].val = val;
				return true;
			}
		}
		this.properties.push(new SenseiProperty(key, val));
		return true;
	},

	removeProperty: function (key) {
		for (var i=0; i<this.properties.length; ++i) {
			if (this.properties[i].key == key) {
				this.properties.splice(i, 1);
				return true;
			}
		}
		return false;
	}
};

// SenseiSort(field, dir=DESC)
var SenseiSort = function () {
	if (arguments.length == 0) return null;

	this.dir = this.DIR.DESC;

	this.field = arguments[0];
	if (arguments.length > 1)
		this.dir = arguments[1];
};

SenseiSort.prototype = {
	DIR: {
		ASC: "asc",
		DESC: "desc"
	}
};

// SenseiClient(query="", offset=0, length=10, explain=false, fetch=false, routeParam, groupBy, maxPerGroup)
var SenseiClient = function () {
	this._facets = [];
	this._selections = [];
	this._sorts = [];
  this._initParams = [];

	this.query = "";
	this.offset = 0;
	this.length = 10;
	this.explain = false;
	this.fetch = false;
	this.routeParam = "";
	this.groupBy = "";
	this.maxPerGroup = 0;

	if (arguments.length > 0)
		this.query = arguments[0];
	if (arguments.length > 1)
		this.offset = arguments[1];
	if (arguments.length > 2)
		this.length = arguments[2];
	if (arguments.length > 3)
		this.explain = arguments[3];
	if (arguments.length > 4)
		this.fetch = arguments[4];
	if (arguments.length > 5)
		this.routeParam = arguments[5];
	if (arguments.length > 6)
		this.groupBy = arguments[6];
	if (arguments.length > 7)
		this.maxPerGroup = arguments[7];
};

SenseiClient.prototype = {
	addInitParam: function (initParam) {
		if (!initParam) return false;

		for (var i=0; i<this._initParams.length; ++i) {
			if (initParam.name == this._initParams[i].name) {
				this._initParams.splice(i, 1, initParam);
				return true;
			}
		}
		this._initParams.push(initParam);

		return true;
	},

	removeInitParam: function (name) {
		for (var i=0; i<this._initParams.length; ++i) {
			if (name == this._initParams[i].name) {
				this._initParams.splice(i, 1);
				return true;
			}
		}
		return false;
	},

	clearInitParams: function () {
		this._initParams = [];
	},

    addFacet: function (facet) {
        if (!facet) return false;

        for (var i=0; i<this._facets.length; ++i) {
            if (facet.name == this._facets[i].name) {
                this._facets.splice(i, 1, facet);
                return true;
            }
        }
        this._facets.push(facet);

        return true;
    },

    removeFacet: function (name) {
        for (var i=0; i<this._facets.length; ++i) {
            if (name == this._facets[i].name) {
                this._facets.splice(i, 1);
                return true;
            }
        }
        return false;
    },

    clearFacets: function () {
        this._facets = [];
    },

	addSelection: function (sel) {
		if (!sel) return false;

		for (var i=0; i<this._selections.length; ++i) {
			if (sel == this._selections[i]) {
				return true;
			}
		}
		this._selections.push(sel);
		return true;
	},

	removeSelection: function (sel) {
		for (var i=0; i<this._selections.length; ++i) {
			if (sel == this._selections[i]) {
				this._selections.splice(i, 1);
				return true;
			}
		}
		return false;
	},

	clearSelections: function () {
		this._selections = [];
	},

	addSort: function (sort) {
		if (!sort) return false;

		for (var i=0; i<this._sorts.length; ++i) {
			if (sort.field == this._sorts[i].field) {
				this._sorts.splice(i, 1, sort);
				return true;
			}
		}
		this._sorts.push(sort);

		return true;
	},

	removeSort: function (field) {
		for (var i=0; i<this._sorts.length; ++i) {
			if (field == this._sorts[i].field) {
				this._sorts.splice(i, 1);
				return true;
			}
		}
		return false;
	},

	clearSorts: function () {
		this._sorts = [];
	},

	buildQuery: function () {
		var qs = {
			q: this.query,
			start: this.offset,
			rows: this.length,
			routeparam: this.routeParam,
			groupby: this.groupBy,
			maxpergroup: this.maxPerGroup
		};
		if (this.explain)
			qs['showexplain'] = true;
		if (this.fetch)
			qs['fetchstored'] = true;

        for (var i = 0; i < this._initParams.length; i++) {
            var inputParam = this._initParams[i];
            qs["dyn."+inputParam.name+".type"] = inputParam.type;
            qs["dyn."+inputParam.name+".val"] = inputParam.vals;
        }
		for (var i=0; i<this._facets.length; ++i) {
			var facet = this._facets[i];
			qs["facet."+facet.name+".expand"] = facet.expand;
			qs["facet."+facet.name+".minhit"] = facet.minHits;
			qs["facet."+facet.name+".max"] = facet.maxCounts;
			qs["facet."+facet.name+".order"] = facet.orderBy;
		}
		for (var i=0; i<this._selections.length; ++i) {
			var sel = this._selections[i];
			qs["select."+sel.name+".val"] = sel.values;
			qs["select."+sel.name+".not"] = sel.excludes;
			qs["select."+sel.name+".op"] = sel.operation;
			var props = [];
			for (var j=0; j<sel.properties.length; j++) {
				props.push(""+sel.properties[j].key+":"+sel.properties[j].val);
			}
			props = props.join(',');
			if (props != '')
				qs["select."+sel.name+".prop"] = props;
		}
		var sl = [];
		for (var i=0; i<this._sorts.length; ++i) {
			var sort = this._sorts[i];
			if (sort.field == "relevance") {
				sl.push(sort.field);
			}
			else {
				sl.push(sort.field+":"+sort.dir);
			}
		}
		qs["sort"] = sl.join(',');

		return qs;
	},

	getSysInfo: function (callback) {
		$.getJSON("sensei/sysinfo", null, callback);
	},

	request: function (callback) {
		$.getJSON("sensei", this.buildQuery(), callback);
	}
};



function buildSelectionReqString(selectionNode,prefix,paramNames){
	var reqString = "";
	var selName = null;
	var firsttime=true;
	for(var i=0; i<selectionNode.childNodes.length; i++){
		var node = selectionNode.childNodes[i];
		var nodeName = node.name;
		if (nodeName == null ) continue;
		if ("name" == nodeName){
			selName = node.value;
		}
		else{
			if (!selName) continue;
			if (nodeName in paramNames){
				if (!firsttime) {
				  reqString+="&";
				}
				
				var val;
				if (node.type == 'checkbox'){
					if (node.value=='on'){
						val = 'true';
					}
					else{
						val = 'false';
					}
				}
				else{
					val = node.value;
				}
				reqString += prefix+"."+selName+"."+paramNames[nodeName]+"="+val;
				firsttime=false;
			}
	    }
	}
	
	return reqString;
}



function buildSortString(){
	var reqString = "sort=";
	var firsttime=true;
	var sortNodes = document.getElementsByName('sort');
	if (sortNodes!=null){
	  for (var i=0;i<sortNodes.length;++i){
		
		var reqSubString = null;
		var field = null;
		var dir = null;
		
		for(var k=0; k<sortNodes[i].childNodes.length; k++){
			var node = sortNodes[i].childNodes[k];
			var nodeName = node.name;
			if (nodeName == null ) continue;
			
			if ("field" == nodeName){
				field = node.value;
			}
			else if ("dir"==nodeName){
				dir = node.value;
			}
		}
		
		if (field!=null){
			if ("relevance" == field){
				reqSubString=field;
			}
			else{
				reqSubString=field+":"+dir;
			}
		}
		
		if (reqSubString!=null){
			if (!firsttime){
				reqString+=",";
			}
			else{
				firsttime=false;
			}
			reqString+=reqSubString;
		}
	  }
    }
	return reqString;
}



function buildreqString(){
	document.getElementById('buildReqButton').disable=true;
	
	var qstring = document.getElementById('query').value;
	
	var start = document.getElementById('start').value;
	var rows = document.getElementById('rows').value;
	var routeparam = document.getElementById('routeparam').value;
	
	var explain = document.getElementById('explain').checked;
	
	var fetchStore = document.getElementById('fetchstore').checked;
	var groupBy = document.getElementById('groupBy').value;
	var maxPerGroup = document.getElementById('maxpergroup').value;
	
  var reqString="q=" + qstring +"&start=" + start + "&rows=" + rows + "&routeparam=" + routeparam;
  var queryParamNames = $('#queryParams').find('.name-input');
  var queryParamValues = $('#queryParams').find('.value-input');
  var qparams = [];
  queryParamNames.each(function(index, obj) {
    var jobj = $(obj);
    if(jobj.val()) {
      qparams.push(jobj.val() + ":" + $(queryParamValues.get(index)).val());
    }
  });

  if (qparams.length > 0) {
    reqString += ("&qparam=" + encodeURIComponent(qparams.join(',')));
  }

	if (explain){
		reqString += "&showexplain=true";
	}
	
	if (fetchStore){
		reqString += "&fetchstored=true";
	}

  if (groupBy) {
		reqString += "&groupby=" + encodeURIComponent(groupBy);
  }
  if (maxPerGroup != '') {
    reqString += "&maxpergroup=" + encodeURIComponent(maxPerGroup);
  }
	
	var selectionNodes = document.getElementsByName('selection');
	var params = {not:"not",val:"val",op:"op"};
	for (var i=0;i<selectionNodes.length;++i){
		reqString+="&"+buildSelectionReqString(selectionNodes[i],"select",params);	
	}
	
	var facetNodes = document.getElementsByName('facet');
	var facetparams = {expand:"expand",max:"max",minhit:"minhit",order:"order"};
	for (var i=0;i<facetNodes.length;++i){
		reqString+="&"+buildSelectionReqString(facetNodes[i],"facet",facetparams);	
	}

    var initParams = document.getElementsByName('inputParam');
    params = {vals:"vals", type:'type'};
    for (var i = 0; i < initParams.length;i++) {
        var facetName = null;
        for(var j=0; j<initParams[i].childNodes.length; j++){
            var node = initParams[i].childNodes[j];
            var nodeName = node.name;
            if (nodeName == null ) continue;
            if ("facetName" == nodeName){
                facetName = node.value;
            }
        }
        if (facetName == null) continue;
        reqString += "&" + buildSelectionReqString(initParams[i], 'dyn.'+facetName, params);
    }

	reqString += "&"+buildSortString();
	
	document.getElementById('reqtext').value=reqString;
	document.getElementById('buildReqButton').disable=false;
}

function trim_leading_comments(str)
{
    // very basic. doesn't support /* ... */
    str = str.replace(/^(\s*\/\/[^\n]*\n)+/, '');
    str = str.replace(/^\s+/, '');
    return str;
}



function runQuery(){
	document.getElementById('runquery').disable=true;
	var reqString = document.getElementById('reqtext').value;
	$.get("sensei?"+reqString,renderResult);
	document.getElementById('runquery').disable=false;
}

(function($){$.toJSON=function(o)
{if(typeof(JSON)=='object'&&JSON.stringify)
return JSON.stringify(o);var type=typeof(o);if(o===null)
return"null";if(type=="undefined")
return undefined;if(type=="number"||type=="boolean")
return o+"";if(type=="string")
return $.quoteString(o);if(type=='object')
{if(typeof o.toJSON=="function")
return $.toJSON(o.toJSON());if(o.constructor===Date)
{var month=o.getUTCMonth()+1;if(month<10)month='0'+month;var day=o.getUTCDate();if(day<10)day='0'+day;var year=o.getUTCFullYear();var hours=o.getUTCHours();if(hours<10)hours='0'+hours;var minutes=o.getUTCMinutes();if(minutes<10)minutes='0'+minutes;var seconds=o.getUTCSeconds();if(seconds<10)seconds='0'+seconds;var milli=o.getUTCMilliseconds();if(milli<100)milli='0'+milli;if(milli<10)milli='0'+milli;return'"'+year+'-'+month+'-'+day+'T'+
hours+':'+minutes+':'+seconds+'.'+milli+'Z"';}
if(o.constructor===Array)
{var ret=[];for(var i=0;i<o.length;i++)
ret.push($.toJSON(o[i])||"null");return"["+ret.join(",")+"]";}
var pairs=[];for(var k in o){var name;var type=typeof k;if(type=="number")
name='"'+k+'"';else if(type=="string")
name=$.quoteString(k);else
continue;if(typeof o[k]=="function")
continue;var val=$.toJSON(o[k]);pairs.push(name+":"+val);}
return"{"+pairs.join(", ")+"}";}};$.evalJSON=function(src)
{if(typeof(JSON)=='object'&&JSON.parse)
return JSON.parse(src);return eval("("+src+")");};$.secureEvalJSON=function(src)
{if(typeof(JSON)=='object'&&JSON.parse)
return JSON.parse(src);var filtered=src;filtered=filtered.replace(/\\["\\\/bfnrtu]/g,'@');filtered=filtered.replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g,']');filtered=filtered.replace(/(?:^|:|,)(?:\s*\[)+/g,'');if(/^[\],:{}\s]*$/.test(filtered))
return eval("("+src+")");else
throw new SyntaxError("Error parsing JSON, source is not valid.");};$.quoteString=function(string)
{if(_escapeable.test(string))
{return'"'+string.replace(_escapeable,function(a)
{var c=_meta[a];if(typeof c==='string')return c;c=a.charCodeAt();return'\\u00'+Math.floor(c/16).toString(16)+(c%16).toString(16);})+'"';}
return'"'+string+'"';};var _escapeable=/["\\\x00-\x1f\x7f-\x9f]/g;var _meta={'\b':'\\b','\t':'\\t','\n':'\\n','\f':'\\f','\r':'\\r','"':'\\"','\\':'\\\\'};})(jQuery);
