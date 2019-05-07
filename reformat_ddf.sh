#!/bin/bash

display_usage() {
  echo
  echo "Usage: $0"
  echo
  echo " <DDF file>    File of DDF table (semicolon separated and w/o header)"
  echo
}

argument="$1"

if [[ -z $argument ]] ; then
  raise_error "Expected argument to be present"
  display_usage
else
  sed 's/;/\t/g' $argument | awk '{print $1 "\t" $2}'
fi
