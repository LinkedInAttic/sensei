require 'rubygems'
require 'sinatra'
require 'searcher'

set :run, false # disable built-in sinatra web server
set :environment, :development
set :base_url, 'http://xxtrial' # custom application option
run Sinatra::Application
