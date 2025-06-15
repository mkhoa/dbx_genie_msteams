import json
import asyncio
import logging

from typing import Dict, List, Optional
from botbuilder.core import (
    ConversationState,
    UserState,
    ActivityHandler, 
    MessageFactory, 
    TurnContext, 
    CardFactory
) 
from botbuilder.schema import (
    ChannelAccount,
    HeroCard,
    CardAction,
    CardImage,
    ActivityTypes,
    Attachment,
    AttachmentData,
    Activity,
    ActionTypes,
)

from botbuilder.dialogs import Dialog
from helpers.dialog_helper import DialogHelper
from clients.databricks_genie_client import GenieClient
# Log
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class GenieBot(ActivityHandler):
    """Represents a bot that processes incoming activities.
    For each user interaction, an instance of this class is created and the OnTurnAsync method is called.
    This is a Transient lifetime service. Transient lifetime services are created
    each time they're requested. For each Activity received, a new instance of this
    class is created. Objects that are expensive to construct, or have a lifetime
    beyond the single turn, should be carefully managed.
    
    This bot uses the Genie API to interact with Databricks"""
    
    def __init__(
        self,
        conversation_state: ConversationState,
        user_state: UserState,
        dialog: Dialog
    ):
        if conversation_state is None:
            raise Exception(
                "[DialogBot]: Missing parameter. conversation_state is required"
            )
        if user_state is None:
            raise Exception("[DialogBot]: Missing parameter. user_state is required")
        if dialog is None:
            raise Exception("[DialogBot]: Missing parameter. dialog is required")

        self.conversation_state = conversation_state
        self.user_state = user_state
        self.conversation_data_accessor = self.conversation_state.create_property("ConversationData")
        self.user_profile_accessor = self.user_state.create_property("UserProfile")
        self.dialog = dialog
        self.WELCOME_MESSAGE = "I am Genie, your AI Data Analyst companion. You can ask me questions about your data, and I will do my best to assist you. Type /help or /intro to see what I can do!"
    
    async def on_turn(self, turn_context: TurnContext):
        await super().on_turn(turn_context)

        # Save any state changes that might have occurred during the turn.
        await self.conversation_state.save_changes(turn_context, False)
        await self.user_state.save_changes(turn_context, False)

    # async def on_message_activity(self, turn_context: TurnContext):
    #     """Handle incoming message activities."""

    #     await DialogHelper.run_dialog(
    #         self.dialog,
    #         turn_context,
    #         self.conversation_state.create_property("DialogState")
    #     )

    #     # Retrieve user profile and conversation data
    #     user_profile = await self.user_profile_accessor.get(turn_context, UserProfile)
        
    #     if hasattr(user_profile, 'token'):
    #         # If the user is authenticated, process the message
    #         await turn_context.send_activity(
    #             f"Here is your token {user_profile}"
    #         )
    #     else:
    #         print("User is not authenticated, running dialog to get token")

    async def on_message_activity(self, turn_context: TurnContext):
        
        await DialogHelper.run_dialog(
            self.dialog,
            turn_context,
            self.conversation_state.create_property("DialogState")
        )
        
    # async def __send_intro_card(self, turn_context: TurnContext):
    #     card = HeroCard(
    #         title="Welcome to Genie!",
    #         text="I am Genie, an AI-powered junior data analyst built by Databricks. "
    #         "I am here to help you analyze sales data using provided dataset on enterprise data platform.  "
    #         "Please ask me any questions related to the data, and I will assist you by executing the necessary queries and providing you with the results.", 
    #         images=[CardImage(url="https://www.cap4lab.com/static/img/illustrations/ai-and-machine-learning.0ee09bd5dc63.png")],
    #         buttons=[
    #             CardAction(
    #                 type=ActionTypes.open_url,
    #                 title="Get an overview",
    #                 text="Get an overview",
    #                 display_text="Get an overview",
    #                 value="https://docs.microsoft.com/en-us/azure/bot-service/?view=azure-bot-service-4.0",
    #             ),
    #             CardAction(
    #                 type=ActionTypes.open_url,
    #                 title="Ask a question",
    #                 text="Ask a question",
    #                 display_text="Ask a question",
    #                 value="https://stackoverflow.com/questions/tagged/botframework",
    #             ),
    #             CardAction(
    #                 type=ActionTypes.open_url,
    #                 title="Contact Senior",
    #                 text="Contact Senior",
    #                 display_text="Contact Senior Analyst",
    #                 value="https://docs.microsoft.com/en-us/azure/bot-service/bot-builder-howto-deploy-azure?view=azure-bot-service-4.0",
    #             ),
    #         ],
    #     )

    #     return await turn_context.send_activity(
    #         MessageFactory.attachment(CardFactory.hero_card(card))
    #     )