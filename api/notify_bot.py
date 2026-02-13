from fastapi import FastAPI
import requests

app = FastAPI()

SLACK_WEBHOOK = "https://hooks.slack.com/services/..."

@app.post("/notify-error/")
def notify_error(message:str):
    payload = {"text": message}
    requests.post(SLACK_WEBHOOK, json=payload)
    return {"status": "notified"}
