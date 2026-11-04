# The nightly API (app.py): chDB in AWS Lambda. chDB's wheel unpacks to over 500 MB,
# past the 250 MB a zip deployment may hold, so the function is a container image.
# Built from .github/nightly, the directory app.py shares nightly_data.py with:
#   docker build -f .github/nightly/api/Dockerfile .github/nightly
FROM public.ecr.aws/lambda/python:3.12

# The latest chdb-core release, like report.py; boto3 comes with the base image.
RUN pip install --no-cache-dir chdb-core

ENV HOME=/tmp
COPY nightly_data.py api/app.py ${LAMBDA_TASK_ROOT}/
CMD ["app.handler"]
