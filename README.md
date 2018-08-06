
# GCP-Sentiment

## Introduction

This [website](http://jmcauley.ucsd.edu/data/amazon/) contains Amazon reviews for products across 24 categories (eg. Books, Electronics, Automotive, etc). These categories (also called domains) have a widely varying number of reviews. Book has nearly 9 million reviews whereas the category 'Amazon Instant Video' has less than 40,000 reviews.
In this project, I want to use text mining along with big data technologies (Spark) to create domain-specific lexicons. These lexicons will then be used for sentiment analysis on all 24 domain reviews. Do domain-specifc lexicons perform better only when used to analyse their own domain reviews or do they perform decently against other domain reviews too?

I started this project as a way to learn about using Spark on a cloud platform. I chose Google Cloud Platform because I'd used it before and felt that the Dataproc+Jupyter setup on GCP was easier to understand than the SageMaker service on AWS.

## Setup

0. Creating a project.
This project is called `pysenti`
1. Setting up a [Google Cloud Storage](https://cloud.google.com/storage) bucket.
This bucket will contain all the data and code related to this project. The following command sets up a regional bucket called `pysenti-data` in the `us-central1` location.
`gsutil mb -p pysenti -c regional -l us-central1 gs://pysenti-data/`
2. Downloading files from the website
TODO
3. Setting up a Dataproc cluster
I'll be using this Spark cluster with a Jupyter notebook to do all my analysis.
`gcloud dataproc --region us-central1 clusters create cluster-pyspark --bucket pysenti-data --subnet default --zone us-central1-a --single-node --master-machine-type n1-standard-1 --master-boot-disk-size 500 --image-version 1.2 --scopes https://www.googleapis.com/auth/cloud-platform --project pysenti --initialization-actions gs://dataproc-initialization-actions/jupyter/jupyter.sh`
	1. SSHing into the master node 
	`gcloud compute ssh cluster-pyspark-m --project=pysenti --zone=us-central1-a  -- -D 1080 -N`
	2. Opening a Chrome browser to connect to the Jupyter notebook interface
		`"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir="%Temp%\cluster-pyspark-m"`
	3. The jupyter interface can be accessed at `http://cluster-pyspark-m:8123` 