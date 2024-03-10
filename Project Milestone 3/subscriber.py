# Subscriber.py
# Michal Frackowiak 100401611

import os
from google.cloud import pubsub_v1
import json

# Set Google Cloud credentials and project details
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\mfrac\OneDrive\Desktop\SOFE4630U-MS1-main\pub-sub\steam-state-413103-43e034f2b96d.json"
project_id = "steam-state-413103"
subscription_id = "smartMeter-filtered-sub"

consumer = pubsub_v1.SubscriberClient()
subscription_path = consumer.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received {json.loads(message.data)}.")
    message.ack

streaming_pull_future = consumer.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

try:
    while True:
        pass
except KeyboardInterrupt:
    print("Stop Subscriber")
finally:
    consumer.close()
