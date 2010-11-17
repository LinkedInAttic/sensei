require 'sinatra'
require 'sinatra/reloader'
require 'java'
require 'jars/lucene-core.jar'
require 'jars/log4j.jar'
require 'json'
require File.dirname(__FILE__) + '/lib/sensei-client'

@@logger = org.apache.log4j.Logger.getLogger("Sinatra Server")

@@logger.info("starting cluster client...")

searchClient = SenseiClient.new

  get '/search' do
    
    Document = org.apache.lucene.document.Document
    doc = Document.new()
    @@logger.info("testing log4j")
    
    result = searchClient.search(params)
    "#{result}"
  end
  
  #shutdown do
  #  @@logger.info("shutting down cluster client...")
  #  @searchClient.shutdown
  #end

