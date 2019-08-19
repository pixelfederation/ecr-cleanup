import os
import argparse
import json
from kubernetes import client, config
import sys
import re
import boto3
import botocore.exceptions
import datetime

version = '0.2.0'
BUCKET_NAME = 'ecr-cleanup'
CONFIG_NAME_S3 = 'cluster_list'
CLUSTER_NAME = None
AWS_REGION = None
IMAGES_TO_KEEP = 20
DRY_RUN = False
EXCLUDE_REPOS = '""'  # format for printing default value
# IGNORE_TAGS_REGEX = '^$'

running_containers = []


def parse_args():
    parser = argparse.ArgumentParser(description='Deletes old images from ECR')
    subparsers = parser.add_subparsers(metavar='', dest='command')

    parser_gen = subparsers.add_parser('genImagesList', help='Generate list of running/live images from current k8s cluster and store it on s3')
    parser_clean = subparsers.add_parser('cleanImages', help='Clean ECR images')

    parser_gen.add_argument('-n', action='store', help="K8s cluster name", dest='cluster_name', metavar='')

    parser_clean.add_argument('-t', action='store_true', default='False', help='Prints the images for deletion without deleting them', dest='dry_run')
    parser_clean.add_argument('-r', action='store', help='ECR region (default: {})'.format(AWS_REGION), dest='aws_region', metavar='')
    parser_clean.add_argument('-k', action='store', help='Number of images to keep (default: {})'.format(IMAGES_TO_KEEP), dest='images_to_keep', metavar='')
    parser_clean.add_argument('-e', action='store', help='Exlude repositories, comma separated strings (default: {})'.format(EXCLUDE_REPOS), dest='exclude_repos', metavar='')

    parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + version)

    args = parser.parse_args()
    if not args.command:
        parser.error('Either genImagesList or cleanImages is required.\n')

    return args


# set variables in order: command line, env variables, set/keep default value
def set_vars(args):
    global CLUSTER_NAME
    global AWS_REGION
    global IMAGES_TO_KEEP
    global DRY_RUN
    global EXCLUDE_REPOS
    # global IGNORE_TAGS_REGEX

    if args.command == 'cleanImages':
        if args.aws_region:
            AWS_REGION = args.aws_region
        elif 'AWS_REGION' in os.environ:
            AWS_REGION = os.environ['AWS_REGION']

        if args.images_to_keep:
            IMAGES_TO_KEEP = int(args.images_to_keep)
        elif 'IMAGES_TO_KEEP' in os.environ:
            IMAGES_TO_KEEP = int(os.environ['IMAGES_TO_KEEP'])

        if args.dry_run:
            DRY_RUN = args.dry_run
        elif 'DRY_RUN' in os.environ:
            if os.environ['DRY_RUN'].lower() == 'true':
                DRY_RUN = True
            else:
                DRY_RUN = False

        if args.exclude_repos:
            EXCLUDE_REPOS = [item.strip() for item in args.exclude_repos.split(',')]
        elif 'EXCLUDE_REPOS' in os.environ:
            EXCLUDE_REPOS = [item.strip() for item in os.environ['EXCLUDE_REPOS'].split(',')]
        else:
            EXCLUDE_REPOS = []

    elif args.command == 'genImagesList':
        if args.cluster_name:
            CLUSTER_NAME = args.cluster_name
        elif 'CLUSTER_NAME' in os.environ:
            CLUSTER_NAME = os.environ['CLUSTER_NAME']


def add_live_container(image_name):
    global running_containers

    if '.dkr.ecr.' in image_name:
        if image_name not in running_containers:
            running_containers.append(image_name)


def generate_live_images_list():
    if CLUSTER_NAME:
        in_cluster = False
        try:
            config.load_incluster_config()
            in_cluster = True
        except:
            print("WARNING: Not in k8s cluster.")
            in_cluster = False

        if not in_cluster:
            try:
                config.load_kube_config()
            except:
                sys.exit("ERROR: Unable to load k8s config.")

        v1 = client.CoreV1Api()

        pods_list = v1.list_pod_for_all_namespaces(watch=False)
        for pod in pods_list.items:
            for container in pod.spec.containers:
                add_live_container(container.image)

            if pod.spec.init_containers:
                for init_container in pod.spec.init_containers:
                    add_live_container(init_container.image)

        for image in running_containers:
            print(image)

        print('Unique live images found:', len(running_containers))

        save_list_s3()
    else:
        sys.exit("ERROR: Missing cluster name")


def get_keep_tags():
    return ['latest']


def clean_ecr_repo():
    global running_containers
    if AWS_REGION:
        print("Cleaning ECR repo in ", AWS_REGION)

        # load list of runnig container images and remember oldest timestamp
        cleanup_timestamp = load_list_s3()

        try:
            ecr_client = boto3.client('ecr', region_name=AWS_REGION)

            # list of ECR repositories
            repositories = []

            # ECR repositories without live images
            repositories_wo_live_images = []

            # get list of ECR repositories
            repo_desc_paginator = ecr_client.get_paginator('describe_repositories')
            for repo_list_paginator in repo_desc_paginator.paginate():
                for repo in repo_list_paginator['repositories']:
                    repositories.append(repo)

            # get tags to keep
            keep_tags = get_keep_tags()

            for repo in repositories:
                print('----------------------------------------------------------------------')
                print(repo['repositoryUri'])

                images_live_sha = []
                images_keep_sha = []
                images_tagged_rest = []
                delete_sha = []
                delete_tag = []
                skip_repo = False

                if isExcluded(repo['repositoryUri'], EXCLUDE_REPOS):
                    skip_repo = True

                # get list of images in ECR repository
                image_desc_paginator = ecr_client.get_paginator('describe_images')
                for image_list_paginator in image_desc_paginator.paginate(registryId=repo['registryId'], repositoryName=repo['repositoryName']):
                    for image in image_list_paginator['imageDetails']:

                        # if image has no tags, mark it for deletion
                        # if image has some tags, check if it's live or if we want to keep the tag
                        if 'imageTags' in image:
                            for tag in image['imageTags']:
                                image_url = repo['repositoryUri'] + ":" + tag
                                if image_url in running_containers:
                                    append_to_list(images_live_sha, image['imageDigest'])
                                    remove_from_list(images_tagged_rest, image)
                                    break
                                else:
                                    if tag in keep_tags:
                                        append_to_list(images_keep_sha, image['imageDigest'])
                                        remove_from_list(images_tagged_rest, image)
                                    elif image['imageDigest'] not in images_live_sha and image['imageDigest'] not in images_keep_sha:
                                        append_to_list(images_tagged_rest, image)
                        else:
                            append_to_list(delete_sha, image['imageDigest'])

                print("Total number of images found:", len(images_tagged_rest) + len(delete_sha) + len(images_keep_sha) + len(images_live_sha))
                print("Number of tagged images found:", len(images_tagged_rest))
                print("Number of untagged images found:", len(delete_sha))
                print("Number of live images found:", len(images_live_sha))
                print("Number of images to keep by tag:", len(images_keep_sha))

                # sort images list by push date in reverse order
                images_tagged_rest.sort(key=lambda k: k['imagePushedAt'], reverse=True)

                # remember ECR repositories without live images for future cleaning
                if len(images_live_sha) <= 0:
                    repositories_wo_live_images.append(repo['repositoryUri'])

                images_to_delete = 0
                # prepare images for deletion
                if skip_repo:
                    print("\nDirectory is excluded from cleanup. Only untagged images will be deleted\n")
                else:
                    for image in images_tagged_rest:
                        image_time = image["imagePushedAt"].replace(tzinfo=None)
                        if images_tagged_rest.index(image) >= IMAGES_TO_KEEP and image_time < cleanup_timestamp:
                            images_to_delete += 1
                            append_to_list(delete_sha, image['imageDigest'])
                            for tag in image['imageTags']:
                                append_to_list(delete_tag, {"imageUrl": repo['repositoryUri'] + ":" + tag,
                                                            "pushedAt": image["imagePushedAt"]})

                    print("Number of tagged images before '{}' marked for deletion: {}".format(cleanup_timestamp, images_to_delete))

                # delete images
                if delete_sha:
                    print("\nNumber of images to be deleted: {}".format(len(delete_sha)))

                    delete_images(ecr_client, delete_sha, delete_tag, repo['registryId'], repo['repositoryName'])
                else:
                    print("Nothing to delete in the repository : " + repo['repositoryName'])

            print("\nRepositories without live images:")
            for repo_not_used in repositories_wo_live_images:
                print(repo_not_used)

        except Exception as e:
            # except botocore.exceptions.EndpointConnectionError as e:
            print(e)
    else:
        sys.exit('ERROR: Region not set.')


def isExcluded(name, list):
    for item in list:
        if name.endswith(item):
            return True
    return False


def get_chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def make_dictionary(list, key):
    result = []
    for item in list:
        append_to_list(result, {key: item})

    return result


def delete_images(ecr_client, delete_sha, delete_tag, id, name):
    if delete_sha:
        # spliting list of images to delete on chunks with 100 images each
        # http://docs.aws.amazon.com/AmazonECR/latest/APIReference/API_BatchDeleteImage.html#API_BatchDeleteImage_RequestSyntax
        i = 0
        delete_sha = make_dictionary(delete_sha, 'imageDigest')
        for delete_sha_chunk in get_chunks(delete_sha, 100):
            i += 1
            if not DRY_RUN:
                print('\tBatch delete AWS', i)
                delete_response = ecr_client.batch_delete_image(registryId=id, repositoryName=name, imageIds=delete_sha_chunk)
                print("AWS response", delete_response)
            else:
                print("registryId: " + id)
                print("repositoryName: " + name)
                print("Deleting {} chank of images".format(i))
                print("\nImage SHAs that are marked for deletion:", *delete_sha_chunk, sep="\n")
    if delete_tag:
        print("\nImage URLs that are marked for deletion:")
        for item in delete_tag:
            print(" - {} - {}".format(item['imageUrl'], item['pushedAt']))


def append_to_list(list, item):
    if item not in list:
        list.append(item)


def remove_from_list(list, item):
    if item in list:
        list.remove(item)


def save_list_s3():
    if CLUSTER_NAME:
        s3_resource = boto3.resource('s3')
        s3_obj = s3_resource.Object(BUCKET_NAME, CLUSTER_NAME)
        s3_obj.put(Body=json.dumps(running_containers))

        print("Live images list for cluster '{}' saved to S3.".format(CLUSTER_NAME))
        config_update_s3(CLUSTER_NAME)
    else:
        sys.exit("ERROR: Missing cluster name")


def config_update_s3(cluster_name):
    s3_resource = boto3.resource('s3')
    s3_obj = s3_resource.Object(BUCKET_NAME, CONFIG_NAME_S3)
    config_exists = False
    cluster_list = []

    # check if config file exists
    try:
        s3_obj.load()
        config_exists = True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            config_exists = False
            print("WARNING: Config file does not exist.")

    # load config if exists
    if config_exists:
        print("Loading config from S3")
        data = s3_obj.get()['Body'].read()
        cluster_list = json.loads(data)
        print("Current clusters list:", cluster_list)
    else:
        print("WARNING: Creating new config.")

    # update and save config file
    if CLUSTER_NAME in cluster_list:
        print("Config is up to date.")
    else:
        cluster_list.append(CLUSTER_NAME)
        s3_obj.put(Body=json.dumps(cluster_list))
        print("Config updated with '{}' cluster.".format(CLUSTER_NAME))


def load_list_s3():
    global running_containers
    oldest_list_time = datetime.datetime.now()
    s3_resource = boto3.resource('s3')
    s3_obj = s3_resource.Object(BUCKET_NAME, CONFIG_NAME_S3)
    cluster_list = []

    # try to load config file
    try:
        s3_obj.load()
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == "404":
            # config_exists = False
            sys.exit("ERROR: Config file does not exist.")
        else:
            sys.exit("ERROR: Unable to load config file.")
    except:
        sys.exit("ERROR: Unable to load config file.")

    #  if config_exists, try to load it and coresponding live images lists
    print("Loading config from S3")
    data = s3_obj.get()['Body'].read()
    cluster_list = json.loads(data)
    print("Current clusters list:", cluster_list)

    for cluster in cluster_list:
        print("Reading live images list from s3 for cluster '{}'".format(cluster))
        s3_list = s3_resource.Object(BUCKET_NAME, cluster)
        try:
            s3_list.load()
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":
                sys.exit("ERROR: images list file for clustr '{}' does not exist.".format(cluster))
            else:
                sys.exit("ERROR: Unable to load images list file '{}'.".format(cluster))
        except:
            sys.exit("ERROR: Unable to load images list file '{}'.".format(cluster))

        list_time = s3_list.last_modified.replace(tzinfo=None)
        print("List timestamp", list_time, end='')

        if list_time < oldest_list_time:
            oldest_list_time = list_time
            print(" is now oldest one. Will be used for ECR cleanup.")
        else:
            print(" is newer then oldest one.")

        data = s3_list.get()['Body'].read()
        image_list = json.loads(data)
        print('Live images found so far:', len(running_containers))
        print('Loading images:', len(image_list))

        for image in image_list:
            append_to_list(running_containers, image)

        print('Live images after load and deduplication:', len(running_containers))

    return oldest_list_time


if __name__ == "__main__":
    args = parse_args()
    set_vars(args)

    if args.command == 'genImagesList':
        generate_live_images_list()
    elif args.command == 'cleanImages':
        clean_ecr_repo()
