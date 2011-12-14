var senseiReq = {};



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

function removeInitParam(node){
	var el = document.getElementById("dyn");
	el.removeChild(node);
}


function addQueryParam() {
  var qp = $('#queryParams');
  qp.append($.mustache($('#query-param-tmpl').html(), {}));
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
	nameTextNode.setAttribute('value',$('#facetsFacets').val());
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

function addInitParam(){
	var el = document.getElementById("dyn");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','inputParam');
	el.appendChild(divNode);

	divNode.appendChild(document.createTextNode('facet name: '));
	var nameTextNode = document.createElement('input');
	nameTextNode.setAttribute('type','text');
	nameTextNode.setAttribute('name','facetName');
	divNode.appendChild(nameTextNode);
	divNode.appendChild(document.createElement('br'));

    divNode.appendChild(document.createTextNode('param name: '));
    nameTextNode = document.createElement('input');
    nameTextNode.setAttribute('type','text');
    nameTextNode.setAttribute('name','name');
    divNode.appendChild(nameTextNode);
    divNode.appendChild(document.createElement('br'));

    divNode.appendChild(document.createTextNode('type: '));
    el = document.createElement('select');
    el.setAttribute('name', 'type');

    var option = document.createElement('option');
    option.setAttribute('value', 'boolean');
    option.appendChild(document.createTextNode("Boolean"));
    el.appendChild(option);

    option = document.createElement('option');
    option.setAttribute('value', 'string');
    option.appendChild(document.createTextNode("String"));
    el.appendChild(option);

    option = document.createElement('option');
    option.setAttribute('value', 'int');
    option.appendChild(document.createTextNode("Int"));
    el.appendChild(option);

    option = document.createElement('option');
    option.setAttribute('value', 'bytearray');
    option.appendChild(document.createTextNode("ByteArray [UTF8]"));
    el.appendChild(option);

    option = document.createElement('option');
    option.setAttribute('value', 'long');
    option.appendChild(document.createTextNode("Long"));
    el.appendChild(option);

    option = document.createElement('option');
    option.setAttribute('value', 'double');
    option.appendChild(document.createTextNode("Double"));
    el.appendChild(option);

    divNode.appendChild(el);
    divNode.appendChild(document.createElement('br'));

	divNode.appendChild(document.createTextNode('value(s): '));
	var node = document.createElement('input');
	node.setAttribute('type','text');
	node.setAttribute('name','vals');
	node.setAttribute('value','');
	divNode.appendChild(node);
	divNode.appendChild(document.createElement('br'));

	var removeButton = document.createElement('input');
	removeButton.setAttribute('type','button');
	removeButton.setAttribute('value','remove init param');
	removeButton.setAttribute('onclick','removeInitParam(this.parentNode)');
	divNode.appendChild(removeButton);
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
	nameTextNode.setAttribute('value',$('#selFacets').val());
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

function addSort(){
	var sortElement = document.getElementById("sorts");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','sort');
	sortElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('sort: '));
	var fieldTextNode = document.createElement('input');
	fieldTextNode.setAttribute('type','text');
	fieldTextNode.setAttribute('name','field');
	fieldTextNode.setAttribute('value',$('#sortFacets').val());
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

function clearInputParams(){
	var el = document.getElementById("dyn");
	removeAllChildren(el);
}
