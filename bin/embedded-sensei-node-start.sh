#!/bin/bash	
########################################################################
#
# This script starts up an EmbeddedSenseiServer which is configured
# by the context file: node-conf/sensei-embed.spring
#
# It takes up to 3 arguments which have to be given in order: 
#
# - data index directory, defaults to data/cardata
# - node id, defaults to 1970
# - port, defaults to 1970
#
# Real world configurations would probably use spring completely or
# some properties file to configure these values.
#
# @author Brian Hammond
#
########################################################################

export USAGE="usage: start-embedded-sensei-node.sh [index_directory [id [port]]]"
export SENSEI_DIR="$( cd $( dirname $( which ${0} ) )/.. ; pwd )"

export CONTEXT="file://${SENSEI_DIR}/node-conf/sensei-embed.spring"

export CLASSPATH=$( find ${SENSEI_DIR}/lib ${SENSEI_DIR}/target/lib ${SENSEI_DIR}/ext -name "*.jar" 2>/dev/null | xargs | tr ' '  : )
export LOGS_DIR="${SENSEI_DIR}/logs"
export MAIN="com.sensei.search.util.SpringLoader"

export DEFAULT_IDX="${SENSEI_DIR}/data/cardata/"
export DEFAULT_ID="1970"
export DEFAULT_PORT="1970"

##
 #
 # echo out the first argument
 #
 ##
_value_or_default() {
	echo ${1}
}

##
 #
 # return 1 if any of the arguments look like a help flag
 #
 ##
_check_args() {
	local arg
	local result=0
	for arg in ${*} ; do
		if [ "-h" == "${arg}" ] || [ "--help" == "${arg}" ] ; then
			result=1
			break
		fi
	done
	return ${result}
}

##
 #
 # print a usage message
 #
 ##
_usage() {
	echo "${USAGE}"
}

##
 #
 # parse the arguments and start up the embedded node
 #
 ##
_main() {
	local idx=$( _value_or_default ${1} ${DEFAULT_IDX} ) ; shift
	local id=$( _value_or_default ${1} ${DEFAULT_ID} ) ; shift
	local port=$( _value_or_default ${1} ${DEFAULT_PORT} ) ; shift

	_check_args ${idx} ${id} ${port}
	if [ 0 = ${?} ] ; then
		java \
		-classpath ${CLASSPATH} \
		-Didx.dir=${idx}        \
		-Dnode.id=${id}         \
		-Dnode.port=${port}     \
		-Dlog.home=${LOGS_DIR}  \
		${MAIN} ${CONTEXT}
	else
		_usage
	fi
}

_main ${*}
########################################################################
# EOF
########################################################################
