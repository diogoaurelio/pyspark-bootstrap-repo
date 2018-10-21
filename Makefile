######################################################################################
#### 					Spark Structured-Streaming with local setup               ####
######################################################################################

.ONESHELL:
SHELL := /bin/bash

CUR_DIR = $(PWD)

###### Please adjust the following params according to your needs: ######

DOCKER_NET = spark
AWS_REGION = eu-west-1
AWS_PROFILE = spark_mock_env
S3_CONTAINER_NAME=s3_mock
S3_PORT=8000


##############################   Docker API #############################

define stop_docker
    docker stop $(1) \
    && docker rm -f $(1) \
    && echo "Stopped $(1) docker container"
endef

define create_aws_profile
	if grep -q $(1) ~/.aws/credentials; \
	then \
		echo "Fake $(1) profile already exists"; \
	else \
		echo "[$(1)]" >> ~/.aws/credentials && \
		echo "aws_access_key_id = $(2)" >> ~/.aws/credentials && \
		echo "aws_secret_access_key = $(3)" >> ~/.aws/credentials && \
		echo "[profile $(1)]" >> ~/.aws/config && \
		echo "region = $(4)" >> ~/.aws/config && \
		echo "output = json" >> ~/.aws/config; \
	fi
endef

docker-create-network:
	docker network create $(DOCKER_NET)

s3-stop:
	$(call stop_docker, $(S3_CONTAINER_NAME))

s3-start:
	@echo "Starting $(S3_CONTAINER_NAME) container" \
		&& docker run -d --name $(S3_CONTAINER_NAME) \
		-p ${S3_PORT}:8000 \
		-e SCALITY_ACCESS_KEY_ID=$AWS_MOCK_KEY \
        -e SCALITY_SECRET_ACCESS_KEY=$AWS_MOCK_SECRET \
		--net=$(DOCKER_NET) \
		scality/s3server \
		&& echo "Started $(S3_CONTAINER_NAME) docker container, listing all running containers:" \
		&& docker ps


#########################################################################


############################ Global settings ############################

create-aws-mock-profile:
	$(call create_aws_profile,$(AWS_PROFILE),MY_ACCESS_KEY,MY_SECRET_KEY,$(AWS_REGION))

init: docker-create-network create-aws-mock-profile
	echo "Initialized Spark Mock Docker environment"

init-err:
	make docker-create-network || echo "" && make create-aws-mock-profile || echo ""

start: init-err s3-start
	echo "Finished local docker setup, ready to start Spark Application. To stop S3 docker container: `make s3-stop`"


#########################################################################