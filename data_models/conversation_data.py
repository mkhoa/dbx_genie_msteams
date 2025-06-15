# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
from databricks.sdk.service.dashboards import GenieAPI

class ConversationData:
    def __init__(
        self,
        timestamp: str = None,
        channel_id: str = None,
        genie_api: GenieAPI = None,
    ):
        self.timestamp = timestamp
        self.channel_id = channel_id
        self.genie_api = genie_api
