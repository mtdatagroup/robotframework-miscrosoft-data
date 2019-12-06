#!/bin/bash

PROGRAM_NAME=${0}
LOCAL_DIR=${PWD}

# Change below to update Tags and Container name
IMAGE_NAME=data-x-tf-image
IMAGE_TAG=$(cat VERSION)
IMAGE_NAME_AND_TAG=${IMAGE_NAME}:${IMAGE_TAG}
CONTAINER_NAME=data-x-tf-container

NO_CACHE_DOCKER_BUILD_OPTS="--rm --no-cache"
DOCKER_FILE=Dockerfile

FINAL_IMAGE_PATH=${IMAGE_NAME}:${IMAGE_TAG}

function usage()
{
   echo "${PROGRAM_NAME} [ -b | -c | -i | -p | -t ]"
   echo ""
   echo " -b = build docker image"
   echo " -c = build docker image - use no-cache"
   echo " -i = interactive bash shell"
   echo " -p = docker system prune"
   echo " -t = run the container"
   exit 1
}

function docker_system_prune()
{
   docker system prune
}

function build_image()
{
   echo "Building image ..."
   docker build -f ${DOCKER_FILE} -t ${IMAGE_NAME_AND_TAG} .
}

function build_image_no_cache()
{
   echo "Building image ..."
   docker build ${NO_CACHE_DOCKER_BUILD_OPTS} -f ${DOCKER_FILE} -t ${IMAGE_NAME_AND_TAG} .
}

function run_interactive()
{
   echo "Running interactive session ..."

   docker run --rm -it \
      --entrypoint "/bin/bash" \
      --name ${CONTAINER_NAME}-INTERACTIVE \
      -v ${LOCAL_DIR}/external:/usr/src/external \
      -v ${LOCAL_DIR}/lib:/usr/src/app/lib \
      -v ${LOCAL_DIR}/tests:/usr/src/app/tests \
      ${FINAL_IMAGE_PATH} 
}

function run_container()
{
   echo "Running container ..."

   docker run --rm -it \
      --name ${CONTAINER_NAME}-INTERACTIVE \
      -v ${LOCAL_DIR}/external:/usr/src/external \
      -v ${LOCAL_DIR}/lib:/usr/src/app/lib \
      -v ${LOCAL_DIR}/tests:/usr/src/app/tests \
      ${FINAL_IMAGE_PATH} 
}

###############
#    main()   #
###############

build=0
build_no_cache=0
interactive=0
container=0
prune=0

while getopts "bicpt" opt; do
  case ${opt} in
   i )
      interactive=1
      ;;
   \? )
      usage
      ;;
   b )
      build=1
      ;;
   c )
      build_no_cache=1
      ;;
   p )
      prune=1
      ;;
   t )
      container=1
      ;;
  esac
done

if [[ ${build} -eq 1 ]] ; then
   build_image
fi

if [[ ${build_no_cache} -eq 1 ]] ; then
   build_image_no_cache
fi

if [[ ${interactive} -eq 1 ]] ; then
   run_interactive
fi

if [[ ${container} -eq 1 ]] ; then
   run_container
fi

if [[ ${prune} -eq 1 ]] ; then
   docker_system_prune
fi
