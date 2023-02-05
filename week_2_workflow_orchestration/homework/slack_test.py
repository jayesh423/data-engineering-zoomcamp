from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("slack-zoomcamp")
slack_webhook_block.notify("Hello!")