
### Execution

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=train-delays-jpatel"
```

```shell
# Create new infra
terraform apply -var="project=train-delays-jpatel"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```
