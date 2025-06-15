# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import json
import logging

from typing import Dict, List, Optional
from botbuilder.core import MessageFactory
from botbuilder.dialogs import (
    WaterfallDialog,
    WaterfallStepContext,
    DialogTurnResult,
    PromptOptions,
    TextPrompt
)
from botbuilder.dialogs.prompts import OAuthPrompt, OAuthPromptSettings, ConfirmPrompt
from botbuilder.core import MessageFactory, UserState, ConversationState
from data_models import UserProfile, ConversationData
from dialogs import LogoutDialog
from clients.databricks_genie_client import GenieClient

# Log
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class MainDialog(LogoutDialog):
    def __init__(self, connection_name: str, user_state: UserState, conversation_state: ConversationState):

        super(MainDialog, self).__init__(MainDialog.__name__, connection_name, user_state)

        self.add_dialog(
            OAuthPrompt(
                OAuthPrompt.__name__,
                OAuthPromptSettings(
                    connection_name=connection_name,
                    text="Please Sign In",
                    title="Sign In",
                    timeout=300000,
                ),
            )
        )

        self.add_dialog(TextPrompt(TextPrompt.__name__))
        self.add_dialog(ConfirmPrompt(ConfirmPrompt.__name__))
        self.add_dialog(
            WaterfallDialog(
                "WFDialog",
                [
                    self.prompt_step,
                    self.login_step,
                    self.command_step,
                    self.process_step
                ],
            )
        )

        self.initial_dialog_id = "WFDialog"
        self.conversation_state = conversation_state
        self.user_state = user_state
        self.conversation_ids: Dict[str, str] = {}

    async def prompt_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        return await step_context.begin_dialog(OAuthPrompt.__name__)

    async def login_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        # Get the token from the previous step. Note that we could also have gotten the
        user_info: UserProfile = step_context.result
        if step_context.result:
            # await step_context.context.send_activity("You are now logged in.")
                    
            # store the UserProfile
            accessor = self.user_state.create_property("UserProfile")
            await accessor.set(step_context.context, user_info)

            return await step_context.prompt(
                TextPrompt.__name__,
                PromptOptions(
                    prompt=MessageFactory.text(
                        "What would you like to do next?"
                    )
                ),
            )

        await step_context.context.send_activity(
            "Login was not successful please try again."
        )

        return await step_context.end_dialog()
    
    async def command_step(
        self, step_context: WaterfallStepContext) -> DialogTurnResult:
        step_context.values["command"] = step_context.result

        # Call the prompt again because we need the token. The reasons for this are:
        # 1. If the user is already logged in we do not need to store the token locally in the bot and worry
        #    about refreshing it. We can always just call the prompt again to get the token.
        # 2. We never know how long it will take a user to respond. By the time the
        #    user responds the token may have expired. The user would then be prompted to login again.
        #
        # There is no reason to store the token locally in the bot because we can always just call
        # the OAuth prompt to get the token or get a new token if needed.
        return await step_context.begin_dialog(OAuthPrompt.__name__)
    
    async def process_step(self, step_context: WaterfallStepContext) -> DialogTurnResult:
        conversation_data: ConversationData = step_context.result

        if step_context.result:
            token_response = step_context.result
            if token_response and token_response.token:
                parts = step_context.values["command"].split(" ")
                command = parts[0]

                # display logged in users name
                if command == "token":
                    await step_context.context.send_activity(
                        f"Your token is {token_response.token}"
                    )
                else:
                    question = step_context.values["command"]
                    user_id = step_context.context.activity.from_property.id
                    conversation_id = self.conversation_ids.get(user_id)

                    try:
                        answer, new_conversation_id = await GenieClient().ask_genie(question, conversation_id)
                        self.conversation_ids[user_id] = new_conversation_id

                        answer_json = json.loads(answer)
                        response = GenieClient().process_query_results(answer_json)

                        await step_context.context.send_activity(response)
                    except json.JSONDecodeError:
                        await step_context.context.send_activity("Failed to decode response from the server.")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        await step_context.context.send_activity("An error occurred while processing your request.") 
        else:
            await step_context.context.send_activity("We couldn't log you in.")

        return await step_context.replace_dialog(self.initial_dialog_id)



    

