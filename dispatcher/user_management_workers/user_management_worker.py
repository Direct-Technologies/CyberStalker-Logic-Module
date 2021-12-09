import logging
import logging.config 
import yaml
from envyaml import EnvYAML
import sys
import asyncio
import smtplib
import pandas as pd
from email.message import EmailMessage
import json
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("user_management")

async def user_management_worker(gql_address, jwt, event):
    """Creates notifications for user activation and password reset actions

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt : str
            GraphQL access token.
            
        event : str
            Event received from a subscription.
    """
    
    
    transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    # Only perform the notifications for human users with a valid email
    if '@' in event['mEmail'] and event['type'] == 'USER':
        # Users that are not activated yet need to be onboarded
        if event['activated'] == False:
            mutation = gql(
            """
            mutation{{
                createNotification(input: {{
                    notification: {{
                        actorType: "{}"
                        actor: "{}"
                        actorName: "{}"
                        tags: ["user_management", "onboarding"]
                        message: "{}"
                        }}
                    }}){{
                            notification {{
                                id
                            }}
                        }}
                    }}
            """.format(event['type'], event['id'], event['login'], r"{{\"type\": \"USER_ONBOARDING\", \"login\": \"{}\", \"email\": \"{}\", \"id\": \"{}\"}}".format(event['login'], event['mEmail'], event['id']))
            )
            result = await client.execute_async(mutation)
            logger.debug("Notification created on user activation: {}".format(result))
        # For activated users who have requested a password reset
        elif event['activated'] == True and event['passwordReset'] == True:
            mutation = gql(
            """
            mutation{{
                createNotification(input: {{
                    notification: {{
                        actorType: "{}"
                        actor: "{}"
                        actorName: "{}"
                        tags: ["user_management", "password_reset"]
                        message: "{}"
                        }}
                    }}){{
                            notification {{
                                id
                            }}
                        }}
                    }}
            """.format(event['type'], event['id'], event['login'], r"{{\"type\": \"USER_PASSWORD_RESET\", \"login\": \"{}\", \"email\": \"{}\", \"id\": \"{}\"}}".format(event['login'], event['mEmail'], event['id']))
            )
            result = await client.execute_async(mutation)
            logger.debug("Notification created on user passowrd reset: {}".format(result))
    return 0

async def user_management_subscription():
    return 0