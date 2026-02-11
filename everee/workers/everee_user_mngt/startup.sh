# Get login password and login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 282654131095.dkr.ecr.us-east-1.amazonaws.com
# Build the Docker image
docker build --platform linux/amd64 -t everee_users_mngt .
# Tag the Docker image
docker tag everee_users_mngt:latest 282654131095.dkr.ecr.us-east-1.amazonaws.com/everee_users_mngt:latest
# Push the Docker image to ECR
docker push 282654131095.dkr.ecr.us-east-1.amazonaws.com/everee_users_mngt:latest
# Update lambda function code
aws lambda update-function-code --function-name everee_users_mngt --image-uri 282654131095.dkr.ecr.us-east-1.amazonaws.com/everee_users_mngt:latest --output table