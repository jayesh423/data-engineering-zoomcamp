from prefect.deployments import Deployment
from etl_web_to_gcs_hw import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("git-zoomcamp")

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="git-example",
     storage=storage,
     entrypoint="week_2_workflow_orchestration/homework/etl_web_to_gcs_hw.py:etl_web_to_gcs")

if __name__ == "__main__":
    deployment.apply()