import logging
import time
import logging.config 
import yaml
import os
import json
import sys
from envyaml import EnvYAML
import asyncio
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
# Import the workers of each application from the workers submodules
from admin_workers import * 
from board_workers import *
from dispatcher_workers import *
from user_management_workers import *

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("controls")

async def restart_dispatcher(gql_address, jwt, event):
    """Restart the dispatcher

        ...

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt : str
            GraphQL access token.
            
        event : str
            Event received from a subscription.
    """
    
    #logger.debug(event)
    
    if event["name"] == "RestartDispatcher":
        sys.exit()