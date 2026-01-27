# Get login password and login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 282654131095.dkr.ecr.us-east-1.amazonaws.com
# Build the Docker image
docker build --platform linux/amd64 -t webhook_ct_timesheet .
# Tag the Docker image
docker tag webhook_ct_timesheet:latest 282654131095.dkr.ecr.us-east-1.amazonaws.com/webhook_ct_timesheet:latest
# Push the Docker image to ECR
docker push 282654131095.dkr.ecr.us-east-1.amazonaws.com/webhook_ct_timesheet:latest
# Update lambda function code
aws lambda update-function-code --function-name webhook_ct_timesheet --image-uri 282654131095.dkr.ecr.us-east-1.amazonaws.com/webhook_ct_timesheet:latest --output table