# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.


class UserProfile:
    def __init__(self, name: str = None, aad_token: str = None):
        self.name = name
        self.aad_token = aad_token
