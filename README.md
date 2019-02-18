# ECR Cleanup utility

This utility is heavy modified version of  Amazon.com, Inc. [ecr-cleanup-lambda](https://github.com/awslabs/ecr-cleanup-lambda) to support multiple kubernetes clusters.

## Getting started

**NOTE:** Tested only with python 3.7.2.

**Prerequisites:**
  * kubernetes
  * boto3

### Generate lists of used images

Fist we need to generate list of used images in k8s cluster(s) and save it to S3 bucket. Run it on every cluster and save to same S3 bucket.
```
python ecr-cleanup.py genImagesList -h
```

### Clean unused images

Read all lists of used images from S3 bucket and clean ECR repository based given conditions.
```
python ecr-cleanup.py cleanImages -h
```
