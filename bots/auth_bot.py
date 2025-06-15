# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from typing import List
from botbuilder.core import (
    ConversationState,
    UserState,
    TurnContext,
)
from botbuilder.dialogs import Dialog
from botbuilder.schema import ChannelAccount
from data_models import ConversationData, UserProfile
from helpers.dialog_helper import DialogHelper

from .genie_bot import GenieBot

class AuthBot(GenieBot):
    def __init__(
        self,
        conversation_state: ConversationState,
        user_state: UserState,
        dialog: Dialog
    ):
        super(AuthBot, self).__init__(conversation_state, user_state, dialog)
        # Initialize the dialog and get the token

    async def on_members_added_activity(self, members_added: List[ChannelAccount], turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    f"Hi there { member.name }. " + self.WELCOME_MESSAGE
                )

    # async def on_members_added_activity(self, members_added: List[ChannelAccount], turn_context: TurnContext):
    #     for member in members_added:
    #         # Greet anyone that was not the target (recipient) of this message.
    #         # To learn more about Adaptive Cards, see https://aka.ms/msbot-adaptivecards for more details.
    #         if member.id != turn_context.activity.recipient.id:
    #             await turn_context.send_activity(
    #                 "Welcome to AuthenticationBot. Type anything to get logged in. Type "
    #                 "'logout' to sign-out."
    #             )

    async def on_token_response_event(self, turn_context: TurnContext):
        # Run the Dialog with the new Token Response Event Activity.
        await DialogHelper.run_dialog(
            self.dialog,
            turn_context,
            self.conversation_state.create_property("DialogState")
        )
        # Retrieve user profile and conversation data
        user_profile = await self.user_profile_accessor.get(turn_context, UserProfile)
        
        if hasattr(user_profile, 'token'):
            # If the user is authenticated, process the message
            await turn_context.send_activity(
                f"Here is your token {user_profile.token}"
            )
        else:
            print("User is not authenticated, running dialog to get token")

