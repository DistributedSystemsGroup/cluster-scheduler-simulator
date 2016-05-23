#!/usr/bin/env bash
set -e

SWARM_IP=''
REGISTRY=''
VERSION=''
PUSH=0

function print_help {
    echo "Usage: $0 [-r registry] [-v version] [-p] [-sp Swarm_IP] <repository>"
    echo
    echo "Will build Docker images names [registry]/<repository>/<image name>:version"
    echo "If -p is specified, docker push will be called at the end of the build"
    echo "If -sp is specified, swarm pull will be called at the end of the build"
    exit
}

while getopts ":hr:v:p:sp" opt; do
    case ${opt} in
        \?|h)
          print_help
          ;;
        r)
          REGISTRY=$OPTARG/
          ;;
        v)
          VERSION=:$OPTARG
          ;;
        p)
          PUSH=1
          ;;
        sp)
          SWARM_PULL=1
          ;;
    esac
done
shift $((OPTIND-1))

if [ -z $1 ]; then
    print_help
fi

REPOSITORY=$1

for d in `find . -mindepth 1 -maxdepth 1 -type d -printf '%f '`; do
  pushd ${d}
  docker build -t ${REGISTRY}${REPOSITORY}/${d}${VERSION} .
  if [ ${PUSH} = 1 ]; then
    docker push ${REGISTRY}${REPOSITORY}/${d}${VERSION}
  fi
  if [ ${SWARM_PULL} = 1 ]; then
    docker -H ${SWARM_IP} pull ${REGISTRY}${REPOSITORY}/${d}${VERSION}
  fi
  popd
done
