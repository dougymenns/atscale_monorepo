# Python 3.11 Lambda base image
FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY process_sheet.py ${LAMBDA_TASK_ROOT}
COPY db_utils.py ${LAMBDA_TASK_ROOT}
COPY utils.py ${LAMBDA_TASK_ROOT}
COPY main.py ${LAMBDA_TASK_ROOT}

CMD ["main.lambda_handler"]
