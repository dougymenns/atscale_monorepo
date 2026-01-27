# PYTHO3.9 LAMBDA BASE IMAGE
FROM public.ecr.aws/lambda/python:3.11

# COPY REQUIREMENTS.TXT TO CONTAINER
COPY requirements.txt ./

# INSTALL DEPENDENCIES
RUN pip install -r requirements.txt 

# COPY FUNCTION CODE TO CONTAINER
COPY process_timesheet.py ${LAMBDA_TASK_ROOT}
COPY db_utils.py ${LAMBDA_TASK_ROOT}
COPY utils.py ${LAMBDA_TASK_ROOT}
COPY main.py ${LAMBDA_TASK_ROOT}

CMD ["main.lambda_handler"]
