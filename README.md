# Pyspark-bootstrap-repo

Boiler-plate code to start you Python Spark project

- [Introduction](#introduction)
- [Python requirements](#Python-requirements)
    - [Setup Anaconda](#Setup-Anaconda)
    - [Setup local conda environment](#Setup-local-conda-environment)
- [Unit testing](#Unit-testing)
    - [Packaging pre-requisites](#Packaging-pre-requisites)
    - [Running the tests](#Running-the-tests)
- [Running locally](#Running-locally)
    - [Step 1) (Optional) Start the Docker containers](#Step-1)-(Optional)-Start-the-Docker-containers)
    - [Step 2) (Optional) Download supporting jars](#Step-2)-(Optional)-Download-supporting-jars)
    - [Step 3) (Optional) Setup local Environmental variables](#Step-3)-(Optional)-Setup-local-Environmental-variables)
    - [Step 4) Run the Spark Application](#Step-4)-Run-the-Spark-Application)

# Introduction

This repo contains boiler-plate code for kickstarting any Spark repository based on Python with unit tests prepared. Moreover, it also adds setup of local docker containers to test locally integration with AWS cloud resources, such as AWS S3 buckets.
Bellow you can find more instructions how to setup locally your Python environment to develop for this repository.

# Python requirements

- Setup Anaconda. 
- Setup conda environment
- Setup local env

### Setup Anaconda
To download Anaconda package manager, go to: <i>https://www.continuum.io/downloads</i>.

After installing locally the conda environment, proceed to setup this project environment.


### Setup local conda environment

For dependency management we are using conda-requirements.txt and requirements.txt. Feel free to add your own packages in those files.
 
Please "cd" into the current reposotory and build your conda environment based on those conda-requirements and requirements:
 
```bash
conda create -n pyspark python=3.6
source activate pyspark
conda install --file conda_requirements.txt
pip install -r pip_requirements.txt
```
Please note that we did not add on purpose PySpark pip package, since this will not be a requirement in your production Spark Cluster environment.
However, in `tests/pip_requirements.txt` you can find the required PySpark packge to install locally in case you have not manually configured PySpark (old school style).

To deactivate this specific virtual environment:
```bash
source deactivate
```

If you need to completely remove this conda env, you can use the following command:

```bash
conda env remove --name pyspark
```

# Unit testing

## Packaging pre-requisites
Some of the existing unit tests require specific python packages, which are not necessarily the exact same as in production. 
For example, we need to create a pyspark env for some of the tests. 

Thus, it is highly advisable that you install those packages to the existing local conda env:

```bash
cd tests
source activate pyspark
pip install -r pip_requirements.txt
```

## Running the tests
We can finally run our tests:
```bash
source activate pyspark
python -m pytest tests/
```

Note: if you run into issues regarding python path, you may find the following command useful:
```bash
export PYTHONPATH=$PYTHONPATH:.
```


# Running locally

If you would like to run the Spark locally. 

Note that we also provided instructions to simulate interacting with AWS Cloud and other type of resources such as AWS S3 buckets and DBs, using docker to the rescue.
Here is a list of steps to get you started.

## Step 1) (Optional) Start the Docker containers

This step is completely optional, and is only intended if you want to test using an S3 bucket.

```bash
make start
```
This will use the provided Makefile to configure a local mock AWS profile (with fake credentials), and start a Docker S3 bucket.

You should also create the appropriate bucket for this job, for example as such:
```bash
aws s3 mb s3://my-bucket --profile spark_mock_env --endpoint http://127.0.0.1:8000
```
Then copy your sample data into it:
```bash
aws s3 cp mydata.json s3://my-bucket/raw-data/ --profile spark_mock_env --endpoint http://127.0.0.1:8000
```
To confirm that data is there:

```bash
aws s3 ls s3://my-bucket/raw-data/ --profile spark_mock_env --endpoint http://127.0.0.1:8000
```

## Step 2) (Optional) Download supporting jars

If your Spark Application is interacting with a Database and AWS S3 buckets, you should download the respective jars, such as:
- for postgres (please adjust to your required version): [postgresql-42.1.1.jar](https://mvnrepository.com/artifact/org.postgresql/postgresql/42.1.1)
- for AWS (please adjust to your required version, but know that it might be tricky): [aws-java-sdk-1.7.4.jar](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4) and [hadoop-aws-2.7.3.jar](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3)

## Step 3) (Optional) Setup local Environmental variables

You might want to set the environment variables, so that Spark knows how to interact with different resources. If your Spark Application is interacting with a Database and AWS S3 buckets, here is a suggested way to configure your environmental variables:

```bash
export PYSPARK_SUBMIT_ARGS=--jars ~/.ivy2/jars/postgresql-42.1.1.jar,~/.ivy2/jars/aws-java-sdk-1.7.4.jar,~/.ivy2/jars/hadoop-aws-2.7.3.jar pyspark-shell
export PYSPARK_PYTHON=~/anaconda/envs/pyspark/bin/python
```
Two important notes here: 1) please replace "~" with your own path where you downloaded, 2) adjust the path to which you downloaded the jars that you downloaded.


Last, but not least, if you are using a local Database, such as Postgres, you might want to export locally its password:
```bash
export DB_PASSWORD=<pwd>
```


## Step 4) Run the Spark Application
The sample application is extremely simple, so you can simply run it from the root of the repository directory with the following commands: 
```bash
export PYTHONPATH=$PYTHONPATH:.
source activate pyspark
python src/job.py --config=$(PWD)/conf/config_local.txt
```
The sample application uses a configuration file located `conf/config_local.txt`, which you can use to customize Application parameters.


**Pro tip: when running locally with docker, sometimes the job hangs when loading from S3. Just restart the S3 docker container, and you should be fine**

