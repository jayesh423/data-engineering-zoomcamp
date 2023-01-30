# Refresh service-account's auth-token
gcloud auth application-default login

# Initialize terraform state
terraform init

# Check changes to new infra plan
terraform plan 

# Create new infra and copy output for the homework
terraform apply 


# delete infra resources after completing the homework
terraform destroy
