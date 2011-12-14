function SenseiClient(host,port){
  this.url = "http://"+host+":"+port+"/sensei";
  this.req = {};
  this.doQuery = function(req,callback){
  	$.post(this.url,JSON.stringify(req),callback);
  }
}
