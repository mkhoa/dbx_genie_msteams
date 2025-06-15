import os
import sys
import traceback
import logging
import json

from datetime import datetime
from config import DefaultConfig
from aiohttp import web
from botbuilder.schema import Activity, ActivityTypes
from botbuilder.core import (
    BotFrameworkAdapterSettings, 
    BotFrameworkAdapter, 
    TurnContext,
    ConversationState,
    MemoryStorage,
    UserState
)

from bots import AuthBot
from dialogs import MainDialog

# Log
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Load environment variables
CONFIG = DefaultConfig()
SETTINGS = BotFrameworkAdapterSettings(CONFIG.APP_ID, CONFIG.APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)

# Create MemoryStorage and state
MEMORY = MemoryStorage()
USER_STATE = UserState(MEMORY)
CONVERSATION_STATE = ConversationState(MEMORY)

# Create dialog
DIALOG = MainDialog(CONFIG.CONNECTION_NAME, USER_STATE, CONVERSATION_STATE)
BOT = AuthBot(CONVERSATION_STATE, USER_STATE, DIALOG)

# Listen for incoming requests on /api/messages
async def messages(req: web.Request) -> web.Response:
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return web.Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers["Authorization"] if "Authorization" in req.headers else ""  

    try:
        response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
        if response:
            # prevent sending None body as 'null'  
            if response.body is None:  
                args = {'status': response.status}  
            else:  
                args = {'data': response.body, 'status': response.status}
            
            logger.log(response)

            return web.json_response(**args)
        
        return web.Response(status=201)
    
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return web.Response(status=500)
    
# Catch-all for errors.
async def on_error(context: TurnContext, error: Exception):
    """
    This check writes out errors to console log .vs. app insights.
    NOTE: In production environment, you should consider logging this to Azure application insights.
    """
    
    print(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
    traceback.print_exc()

    # Send a message to the user
    await context.send_activity("The bot encountered an error or bug.")
    await context.send_activity(
        "To continue to run this bot, please fix the bot source code."
    )
    # Send a trace activity if we're talking to the Bot Framework Emulator
    if context.activity.channel_id == "emulator":
        # Create a trace activity that contains the error object
        trace_activity = Activity(
            label="TurnError",
            name="on_turn_error Trace",
            timestamp=datetime.now(),
            type=ActivityTypes.trace,
            value=f"{error}",
            value_type="https://www.botframework.com/schemas/error",
        )
        # Send a trace activity, which will be displayed in Bot Framework Emulator
        await context.send_activity(trace_activity)

app = web.Application()
app.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    try:
        host = os.getenv("HOST", "localhost")
        port = int(os.environ.get("PORT", 3978))
        web.run_app(app, host=host, port=port)
    except Exception as error:
        logger.exception("Error running app")
