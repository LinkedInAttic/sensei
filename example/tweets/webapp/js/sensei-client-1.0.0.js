function executeSenseiReq(host,port,req,callback){
  var url = "http://"+host+":"+port+"/sensei";
  $.post(url,JSON.stringify(req),callback);
}

function extractSrcData(senseiHit){
  return eval('('+senseiHit.srcdata+')');
}

function setSenseiQueryString(req,queryString){
  req["query"]={"query_string":{"query":queryString}};
  console.log("q:"+req);
}
