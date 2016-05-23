# Docker Images

This folder contains Docker Images.

It is possible to build and pull (on both Docker and Swarm) them by running:

    $ bash build_push_pull_all.sh
    
    $ bash build_push_pull_all.sh -h
    Usage: build_push_pull_all.sh [-r registry] [-v version] [-p] [-sp Swarm_IP] <repository>
    
    Will build Docker images names [registry]/<repository>/<image name>:version
    If -p is specified, docker push will be called at the end of the build
    If -sp is specified, swarm pull will be called at the end of the build
    
Example:

    $ bash build_push_pull_all.sh -r 192.168.45.252:5000 -p -sp 192.168.45.252:2380 repository