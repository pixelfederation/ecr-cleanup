FROM python:3.7.2-slim-stretch

RUN pip install kubernetes boto3

COPY ecr-cleanup.py /app/

ENTRYPOINT [ "python", "/app/ecr-cleanup.py" ]
