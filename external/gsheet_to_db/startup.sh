# Get login password and login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 282654131095.dkr.ecr.us-east-1.amazonaws.com
# Build the Docker image
docker build --platform linux/amd64 -t gsheet_to_db .
# Tag the Docker image
docker tag gsheet_to_db:latest 282654131095.dkr.ecr.us-east-1.amazonaws.com/gsheet_to_db:latest
# Push the Docker image to ECR
docker push 282654131095.dkr.ecr.us-east-1.amazonaws.com/gsheet_to_db:latest
# Update lambda function code
aws lambda update-function-code --function-name gsheet_to_db --image-uri 282654131095.dkr.ecr.us-east-1.amazonaws.com/gsheet_to_db:latest --output table