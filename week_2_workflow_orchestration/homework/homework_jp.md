## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

Answer: 
    - Update script with this values: color=green, year= 2020
    - Getting error- KeyError: 'tpep_pickup_datetime'. 
        - Change columns from tpep_pickup_datetime to lpep_pickup_datetime and run again
    - Error: OSError: Cannot save file into a non-existent directory: 'data/green'
        - create green directory and run again
    - Log value: 17:53:05.073 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

Answer: 
    - prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs -n etl-gcs --cron "0 5 1 * *" -a
    - cron schedule is 0 5 1 * *
    - Log Output:
    (zoomcamp) jayesh@de-zoomcamp:~/git/data-engineering-zoomcamp/week_2_workflow_orchestration/flows/02_gcp$ prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs -n etl-gcs --cron "0 5 1 * *" -a
    Found flow 'etl-web-to-gcs'
    Default '.prefectignore' file written to 
    /home/jayesh/git/data-engineering-zoomcamp/week_2_workflow_orchestration/flows/02_gcp/.pr
    efectignore
    Deployment YAML created at 
    '/home/jayesh/git/data-engineering-zoomcamp/week_2_workflow_orchestration/flows/02_gcp/et
    l_web_to_gcs-deployment.yaml'.
    Deployment storage None does not have upload capabilities; no files uploaded.  Pass 
    --skip-upload to suppress this warning.
    Deployment 'etl-web-to-gcs/etl-gcs' successfully created with id 
    'a62f4346-cb59-4793-9b4c-bade251b9c96'.

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483

Answer:
    - Run etl_web_to_gcs for feb 2019 and mar 2019
    - verify files are present on GCS
    - run etl_gcs_to_bq deployment: 
        prefect deployment build etl_gcs_to_bq.py:etl_gcs_bq_parent -n "etl-gcs-bq"
        prefect deployment apply etl_gcs_bq_parent-deployment.yaml
        -- to run a flow from CLI
        prefect deployment run etl-gcs-bq-parent/etl-gcs-bq --params '{"color":"yellow", "year":2019, "months":[2,3]}'
        or
        prefect deployment run etl-gcs-bq-parent/etl-gcs-bq -p color=yellow -p year=2019 -p months=[2,3]


    20:27:52.774 | INFO    | Flow run 'elastic-muskrat' - Executing 'write_to_bq-e73a09d5-0' immediately... 
    rows: 7832545
    20:28:36.339 | INFO    | Task run 'write_to_bq-e73a09d5-0' - Finished in state Completed()
    20:28:36.382 | INFO    | Flow run 'elastic-muskrat' - Finished in state Completed()
    Total rows processed by this flow: 14851920    


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225

Answer:
    - add git hub block on orion UI
    - register prefect git: 
        pip install prefect-github
        prefect block register -m prefect_github
    - add deployment script- git_deploy similar to docker script
        storage = GitHub.load("git-zoomcamp")
        deployment = Deployment.build_from_flow(
            flow=etl_web_to_gcs,
            name="git-example",
            storage=storage,
            entrypoint="week_2_workflow_orchestration/homework/etl_web_to_gcs_hw.py:etl_web_to_gcs")

    - run it from shell: python git_deploy.py
    - Log Excerpt:
        06:55:18.831 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605
        06:55:18.868 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10


## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here