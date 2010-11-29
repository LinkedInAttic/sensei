function removeAllChildren(elem){
	if ( elem.hasChildNodes() ){
	    while (elem.childNodes.length >= 1 )
	    {
	        elem.removeChild( elem.firstChild );       
	    } 
	}
}

function removeSort(sortNode){
	var sortElement = document.getElementById("sorts");
	sortElement.removeChild(sortNode);
}

function removeSelection(selectionNode){
	var selElement = document.getElementById("selections");
	selElement.removeChild(selectionNode);
}

function removeFacet(facetNode){
	var facetElement = document.getElementById("facets");
	facetElement.removeChild(facetNode);
}

function addFacet(){
	var facetElement = document.getElementById("facets");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','facet');
	facetElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('name: '));
	var nameTextNode = document.createElement('input');
	nameTextNode.setAttribute('type','text');
	nameTextNode.setAttribute('name','name');
	divNode.appendChild(nameTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('expand: '));
	var expandCheckNode = document.createElement('input');
	expandCheckNode.setAttribute('type','checkbox');
	expandCheckNode.setAttribute('name','expand');
	divNode.appendChild(expandCheckNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('min hits: '));
	var minHitsTextNode = document.createElement('input');
	minHitsTextNode.setAttribute('type','text');
	minHitsTextNode.setAttribute('name','minhit');
	minHitsTextNode.setAttribute('value','1');
	divNode.appendChild(minHitsTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('max counts: '));
	var maxCountsTextNode = document.createElement('input');
	maxCountsTextNode.setAttribute('type','text');
	maxCountsTextNode.setAttribute('name','max');
	maxCountsTextNode.setAttribute('value','10');
	divNode.appendChild(maxCountsTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('order by: '));
	var dropDownOrderNode = document.createElement('select');
	dropDownOrderNode.setAttribute('name','order');
	var opt1 =  document.createElement('option');
	opt1.innerHTML = 'hits';
	dropDownOrderNode.appendChild(opt1);
	var opt2 =  document.createElement('option');
	opt2.innerHTML = 'val';
	dropDownOrderNode.appendChild(opt2);
	divNode.appendChild(dropDownOrderNode);
	divNode.appendChild(document.createElement('br'));
	
	var removeButton = document.createElement('input');
	removeButton.setAttribute('type','button');
	removeButton.setAttribute('value','remove facet');
	removeButton.setAttribute('onclick','removeFacet(this.parentNode)');
	divNode.appendChild(removeButton);
}

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
				reqString += prefix+"."+selName+"."+nodeName+"="+val;  	
				firsttime=false;
			}
	    }
	}
	
	return reqString;
}

function addSelection(){
	var selElement = document.getElementById("selections");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','selection');
	selElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('name: '));
	var nameTextNode = document.createElement('input');
	nameTextNode.setAttribute('type','text');
	nameTextNode.setAttribute('name','name');
	divNode.appendChild(nameTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('values: '));
	var valTextNode = document.createElement('input');
	valTextNode.setAttribute('type','text');
	valTextNode.setAttribute('name','val');
	divNode.appendChild(valTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('excludes: '));
	var notTextNode = document.createElement('input');
	notTextNode.setAttribute('type','text');
	notTextNode.setAttribute('name','not');
	divNode.appendChild(notTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('operation: '));
	var dropDownSelNode = document.createElement('select');
	dropDownSelNode.setAttribute('name','op');
	var opt1 =  document.createElement('option');
	opt1.innerHTML = 'or';
	dropDownSelNode.appendChild(opt1);
	var opt2 =  document.createElement('option');
	opt2.innerHTML = 'and';
	dropDownSelNode.appendChild(opt2);
	divNode.appendChild(dropDownSelNode);
	divNode.appendChild(document.createElement('br'));
	
	var removeButton = document.createElement('input');
	removeButton.setAttribute('type','button');
	removeButton.setAttribute('value','remove selection');
	removeButton.setAttribute('onclick','removeSelection(this.parentNode)');
	divNode.appendChild(removeButton);
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

function addSort(){
	var sortElement = document.getElementById("sorts");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','sort');
	sortElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('sort: '));
	var fieldTextNode = document.createElement('input');
	fieldTextNode.setAttribute('type','text');
	fieldTextNode.setAttribute('name','field');
	divNode.appendChild(fieldTextNode);
	
	var dropDownSelNode = document.createElement('select');
	dropDownSelNode.setAttribute('name','dir');
	var opt1 =  document.createElement('option');
	opt1.innerHTML = 'desc';
	dropDownSelNode.appendChild(opt1);
	var opt2 =  document.createElement('option');
	opt2.innerHTML = 'asc';
	dropDownSelNode.appendChild(opt2);
	divNode.appendChild(dropDownSelNode);
	divNode.appendChild(document.createElement('br'));
	
	var removeButton = document.createElement('input');
	removeButton.setAttribute('type','button');
	removeButton.setAttribute('value','remove sort');
	removeButton.setAttribute('onclick','removeSort(this.parentNode)');
	divNode.appendChild(removeButton);
}

function clearSorts(){
	var sortElement = document.getElementById("sorts");
	removeAllChildren(sortElement);
}

function clearSelections(){
	var selElement = document.getElementById("selections");
	removeAllChildren(selElement);
}

function clearFacets(){
	var facetElement = document.getElementById("facets");
	removeAllChildren(facetElement);
}


function buildreqString(){
	document.getElementById('buildReqButton').disable=true;
	
	var qstring = document.getElementById('query').value;
	
	var start = document.getElementById('start').value;
	var rows = document.getElementById('rows').value;
	
	var explain = document.getElementById('explain').checked;
	
	var fetchStore = document.getElementById('fetchstore').checked;
	
    var reqString="q=" + qstring +"&start=" + start + "&rows=" + rows;

	if (explain){
		reqString += "&showexplain=true";
	}
	
	if (fetchStore){
		reqString += "&fetchstored=true";
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

function renderResult(content){
	var js_source = content.replace(/^\s+/, '');
	var indent_size = 4;
    var indent_char = ' ';
    var preserve_newlines = true;
    var keep_array_indentation = false;
    var braces_on_own_line = true;

	document.getElementById('content').value =
    js_beautify(js_source, {
        indent_size: indent_size,
        indent_char: indent_char,
        preserve_newlines:preserve_newlines,
        braces_on_own_line: braces_on_own_line,
        keep_array_indentation:keep_array_indentation,
        space_after_anon_function:true});
}

function runQuery(){
	document.getElementById('runquery').disable=true;
	var reqString = document.getElementById('reqtext').value;
	$.get("sensei?"+reqString,renderResult);
	document.getElementById('runquery').disable=false;
}
