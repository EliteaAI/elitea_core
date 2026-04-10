from flask import request
from pylon.core.tools import log
from tools import api_tools, db, auth, rpc_tools, VaultClient, config as c
from pydantic.v1 import ValidationError

from ...models.all import ApplicationVersion
from ...models.pd.chat import ApplicationChatRequest
from ...utils.predict_utils import generate_predict_payload
from ...utils.botframework import BotConnectorClient
from ...utils.constants import PROMPT_LIB_MODE
# from botbuilder.core import ActivityHandler

from requests import post


# def request_bot_token():
#     # Get the bot token from the Vault
#     # POST https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token
#     # Host: login.microsoftonline.com
#     # Content-Type: application/x-www-form-urlencoded

#     # grant_type=client_credentials&client_id=MICROSOFT-APP-ID&client_secret=MICROSOFT-APP-PASSWORD&scope=https%3A%2F%2Fapi.botframework.com%2F.default
#     req = post(url="https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token",
#                headers={"Content-Type": "application/x-www-form-urlencoded"},
#                data={
#                    "grant_type": "client_credentials",
#                    "client_id": "MICROSOFT-APP-ID",
#                    "client_secret": "MICROSOFT-APP-PASSWORD",
#                    "scope": "https://api.botframework.com/.default"
#                    }
#                )
#     resp = req.json()
#     return resp["access_token"]



import os

""" Bot Configuration """

class WebHookAPI(api_tools.APIModeHandler):
    def post(self, project_id: int, version_id: int):
        # FIXME: auth

         # Parse the incoming JSON payload
        incoming_activity = request.json

        # Here you would add logic to process the incoming activity
        # For example, check incoming_activity["type"] and respond accordingly
        log.debug(f"Received activity: {incoming_activity}")

        if incoming_activity.get("type") == "message":
            # here we need to predict something
            log.debug(f"Activity ID: f{incoming_activity['channelData'].get('clientActivityID')}")
            bot = BotConnectorClient(
                app_id='MICROSOFT-APP-ID',
                app_password='MICROSOFT-APP-PASSWORD',
                incoming_activity=incoming_activity
            )
            bot.send_typing_event()
            bot.reply_to_activity({"type": "message", "text": "Echo: " + incoming_activity.get('text', '')})
            return {"message": "Ok"}, 200
        else:
            return {"message": "Ok"}, 200



class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: WebHookAPI,
    }
