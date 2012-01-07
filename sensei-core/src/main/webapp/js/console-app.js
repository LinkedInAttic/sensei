function _void (){}
// Cookie set and get functions.
function now(){return (new Date).getTime();}

function setCookie(name, value, expires, path, domain)
{
    if(!expires) expires = -1;
    if(!path) path = "/";
    var d = "" + name + "=" + value;

    var e;
    if (expires < 0) {
        e = "";
    }
    else if (expires == 0) {
        var f = new Date(1970, 1, 1);
        e = ";expires=" + f.toUTCString();
    }
    else {
        var f = new Date(now() + expires * 1000);
        e = ";expires=" + f.toUTCString();
    }
    var dm;
    if(!domain){
        dm = "";
    }
    else{
        dm = ";domain=" + domain;
    }

    document.cookie = name + "=" + value + ";path=" + path + e + dm;
};

function getCookie(a)
{
    var b = String(document.cookie);
    var c = b.indexOf(a + "=");

    if (c != -1) {
        var d = b.indexOf(";", c);
        d = d == -1 ? b.length : d;
        c = c + a.length + 1;
        if(b.charAt(c)=='"'&&b.charAt(d-1)=='"'){
            c+=1;d-=1;
        }
        if(c>=d)
            return "";
        return b.substring(c, d);
    }

    return "";
};

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

function addFacet(){
	var facetElement = document.getElementById("facets");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','facet');
	facetElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('name: '));
	var nameTextNode = document.createElement('input');
  nameTextNode.setAttribute('style','width:150px;margin-bottom:10px');
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
  minHitsTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	minHitsTextNode.setAttribute('type','text');
	minHitsTextNode.setAttribute('name','minhit');
	minHitsTextNode.setAttribute('value','1');
	divNode.appendChild(minHitsTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('max counts: '));
	var maxCountsTextNode = document.createElement('input');
  maxCountsTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	maxCountsTextNode.setAttribute('type','text');
	maxCountsTextNode.setAttribute('name','max');
	maxCountsTextNode.setAttribute('value','10');
	divNode.appendChild(maxCountsTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('order by: '));
	var dropDownOrderNode = document.createElement('select');
  dropDownOrderNode.setAttribute('style','width:150px;margin-bottom:10px');
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
	removeButton.setAttribute('value','remove');
	removeButton.setAttribute('class','btn error');
	removeButton.setAttribute('onclick','removeFacet(this.parentNode)');
  
	divNode.appendChild(removeButton);

  var jDivNode = $(divNode);
  jDivNode.find('input[type="text"]').keyup(function (e) {
    buildQuery();
  });

  jDivNode.find('input[type="checkbox"]').change(function (e) {
    buildQuery();
  });

  jDivNode.find('select').change(function (e) {
    buildQuery();
  });

  $(removeButton).click(function (e) {
    buildQuery();
  });
}

function addQueryParam() {
  var qp = $('#queryParams');
  qp.append($.mustache($('#query-param-tmpl').html(), {}));
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
  nameTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	nameTextNode.setAttribute('value',$('#initParamsFacets').val());
	divNode.appendChild(nameTextNode);
	divNode.appendChild(document.createElement('br'));

    divNode.appendChild(document.createTextNode('param name: '));
    nameTextNode = document.createElement('input');
    nameTextNode.setAttribute('style','width:150px;margin-bottom:10px');
    nameTextNode.setAttribute('type','text');
    nameTextNode.setAttribute('name','name');
    divNode.appendChild(nameTextNode);
    divNode.appendChild(document.createElement('br'));

    divNode.appendChild(document.createTextNode('type: '));
    el = document.createElement('select');
    el.setAttribute('name', 'type');
    
    el.setAttribute('style','width:150px;margin-bottom:10px');

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
  node.setAttribute('style','width:100px;margin-bottom:10px');
	divNode.appendChild(node);
	divNode.appendChild(document.createElement('br'));

	var removeButton = document.createElement('input');
	removeButton.setAttribute('type','button');
	removeButton.setAttribute('value','removes');
  removeButton.setAttribute('class','btn error');
	removeButton.setAttribute('onclick','removeInitParam(this.parentNode)');
	divNode.appendChild(removeButton);

  var jDivNode = $(divNode);
  jDivNode.find('input[type="text"]').keyup(function (e) {
    buildQuery();
  });

  jDivNode.find('input[type="checkbox"]').change(function (e) {
    buildQuery();
  });

  jDivNode.find('select').change(function (e) {
    buildQuery();
  });

  $(removeButton).click(function (e) {
    buildQuery();
  });
}

function addSelection(){
	var selElement = document.getElementById("selections");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','selection');
	selElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('name: '));
	var nameTextNode = document.createElement('input');
  nameTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	nameTextNode.setAttribute('type','text');
	nameTextNode.setAttribute('name','name');
	nameTextNode.setAttribute('value',$('#selFacets').val());
	divNode.appendChild(nameTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('values: '));
	var valTextNode = document.createElement('input');
  valTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	valTextNode.setAttribute('type','text');
	valTextNode.setAttribute('name','val');
	divNode.appendChild(valTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('excludes: '));
	var notTextNode = document.createElement('input');
  notTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	notTextNode.setAttribute('type','text');
	notTextNode.setAttribute('name','not');
	divNode.appendChild(notTextNode);
	divNode.appendChild(document.createElement('br'));
	
	divNode.appendChild(document.createTextNode('operation: '));
	var dropDownSelNode = document.createElement('select');
  dropDownSelNode.setAttribute('style','width:150px;margin-bottom:10px');
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
	removeButton.setAttribute('value','remove');
	removeButton.setAttribute('onclick','removeSelection(this.parentNode)');
  removeButton.setAttribute('class','btn error');
	divNode.appendChild(removeButton);

  var jDivNode = $(divNode);
  jDivNode.find('input[type="text"]').keyup(function (e) {
    buildQuery();
  });

  jDivNode.find('input[type="checkbox"]').change(function (e) {
    buildQuery();
  });

  jDivNode.find('select').change(function (e) {
    buildQuery();
  });

  $(removeButton).click(function (e) {
    buildQuery();
  });
}

function addSort(){
	var sortElement = document.getElementById("sorts");
	var divNode = document.createElement('div');
	divNode.setAttribute('name','sort');
	sortElement.appendChild(divNode);
	
	divNode.appendChild(document.createTextNode('sort: '));
	var fieldTextNode = document.createElement('input');
  fieldTextNode.setAttribute('style','width:150px;margin-bottom:10px');
	fieldTextNode.setAttribute('type','text');
	fieldTextNode.setAttribute('name','field');
	fieldTextNode.setAttribute('value',$('#sortFacets').val());
	divNode.appendChild(fieldTextNode);
	
	var dropDownSelNode = document.createElement('select');
	dropDownSelNode.setAttribute('name','dir');
  dropDownSelNode.setAttribute('style','width:150px;margin-bottom:10px');
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
	removeButton.setAttribute('value','remove');
	removeButton.setAttribute('onclick','removeSort(this.parentNode)');
  removeButton.setAttribute('class','btn error');
	divNode.appendChild(removeButton);

  var jDivNode = $(divNode);
  jDivNode.find('input[type="text"]').keyup(function (e) {
    buildQuery();
  });

  jDivNode.find('input[type="checkbox"]').change(function (e) {
    buildQuery();
  });

  jDivNode.find('select').change(function (e) {
    buildQuery();
  });

  $(removeButton).click(function (e) {
    buildQuery();
  });
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

var default_js_beautify_settings = {
  indent_size               : 4,
  indent_char               : ' ',
  preserve_newlines         : true,
  braces_on_own_line        : true,
  keep_array_indentation    : false,
  space_after_anon_function : true
};

function runQuery(){
  if (reqTextMirror == null)
    return;

  var jobj = $('#runquery');
  jobj.attr('disabled', 'disabled');
  $.post("sensei", reqTextMirror.getValue(), function (text) {
    contentMirror.setValue(js_beautify(text, default_js_beautify_settings));
  }, 'text')
  .complete(function(){
    jobj.removeAttr('disabled');
  });
}

function buildQuery(){
  var req = {};

  req.query = {
    query_string : {
      query : $('#query').val()
    }
  };

  req.from = parseInt($('#start').val());
  if (isNaN(req.from))
    req.from = 0;
  req.size = parseInt($('#rows').val());
  if (isNaN(req.size))
    req.size = 10;

  routeParam = $('#routeparam').val();
  if (routeParam != null && routeParam.length != 0)
    req.routeParam = routeParam;

  req.explain = $('#explain').prop('checked');
  req.fetchStored = $('#fetchstore').prop('checked');

  if ($('#fetchTermVector').prop('checked')) {
    var val = $('#tvFields').val();
    if (val != null) {
      var tvFields = val.split(/,/);
      req.termVectors = [];
      $.each(tvFields, function (i, v) {
        v = v.trim();
        if (v.length != 0)
          req.termVectors.push(v);
      });
    }
  }

	var groupBy = $('#groupBy').val();
	var maxPerGroup = parseInt($('#maxpergroup').val());
  if (groupBy != null && groupBy.length != 0) {
    if (isNaN(maxPerGroup))
      maxPerGroup = 0;

    req.groupBy = {
      columns : [groupBy],
      top     : maxPerGroup
    };
  }

  var sort = [];
  $('[name="sort"]').each(function (i, v) {
    var jobj = $(v);
    var field = jobj.find('[name="field"]').val();
    if (field == '_score')
      sort.push(field);
    else {
      var o = {};
      o[field] = jobj.find('[name="dir"]').val();
      sort.push(o);
    }
  });
  if (sort.length != 0)
    req.sort = sort;

  var selections = [];
  $('[name="selection"]').each(function (i, v) {
    var jobj = $(v);
    var field = jobj.find('[name="name"]').val();
    if (field != null && field.length != 0) {
      var o = {terms : {}};
      var oo = o.terms[field] = {
        values   : [],
        excludes : [],
        operator : jobj.find('[name="op"]').val()
      };

      var values   = jobj.find('[name="val"]').val();
      if (values != null) {
        $.each(values.split(/,/), function (ii, vv) {
          vv = vv.trim();
          if (vv.length != 0)
            oo.values.push(vv);
        });
      }
      var excludes = jobj.find('[name="not"]').val();
      if (excludes != null) {
        $.each(excludes.split(/,/), function (ii, vv) {
          vv = vv.trim();
          if (vv.length != 0)
            oo.excludes.push(vv);
        });
      }

      selections.push(o);
    }
  });
  if (selections.length != 0)
    req.selections = selections;

  var facets = {};
  $('[name="facet"]').each(function (i, v) {
    var jobj = $(v);
    var field = jobj.find('[name="name"]').val();
    if (field != null && field.length != 0) {
      var o = facets[field] = {
        max      : parseInt(jobj.find('[name="max"]').val()),
        minCount : parseInt(jobj.find('[name="minhit"]').val()),
        expand   : jobj.find('[name="expand"]').prop('checked'),
        order    : jobj.find('[name="order"]').val()
      };
      if (isNaN(o.max))
        o.max = 10;
      if (isNaN(o.minCount))
        o.minCount = 1;
    }
  });
  for (var key in facets) {
    req.facets = facets;
    break;
  }

  var facetInit = {};
  $('[name="inputParam"]').each(function (i, v) {
    var jobj = $(v);
    var field = jobj.find('[name="facetName"]').val();
    if (field != null && field.length != 0) {
      var o = facetInit[field];
      if (o == null)
        o = facetInit[field] = {};

      var param = jobj.find('[name="name"]').val();
      if (param != null && param.length != 0) {
        var oo = o[param] = {
          values : [],
          type   : jobj.find('[name="type"]').val()
        };

        var values = jobj.find('[name="vals"]').val();
        if (values != null) {
          $.each(values.split(/,/), function (ii, vv) {
            vv = vv.trim();
            if (vv.length != 0)
              oo.values.push(vv);
          });
        }
      }
    }
  });
  for (var key in facetInit) {
    req.facetInit = facetInit;
    break;
  }

  reqTextMirror.setValue(js_beautify($.toJSON(req), default_js_beautify_settings));
}

