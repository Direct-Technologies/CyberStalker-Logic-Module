import asyncio
import authenticate as auth
import backoff
from envyaml import EnvYAML
from email.message import EmailMessage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import json
import logging
import logging.config 
import os
import smtplib

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("email notification delivery")

@backoff.on_exception(backoff.expo, Exception)
async def email_notification_delivery_subscription():
    """
    
    """
    try:
        # Grab the async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        token_id = await auth.get_token_id(gql_address, jwt_env_key)
        # topic = "notifications:"+token_id
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            subscription = gql(
                """
                subscription onNotification{
                    Notifications{
                        event 
                        relatedNodeId
                        relatedNode {
                            ... on Notification {
                                id
                                subjectType
                                subject
                                subjectName
                                tags
                                by
                                spec
                                message 
                                to
                            }
                        }
                    }   
                }
                """
            )
            
            logger.info("👂 Listening on {} for {}".format("notifications", "email deliveries"))
            
            # Send system alarm notifications to be delivered via email
            async for event in session.subscribe(subscription):
                logger.debug(event)
                # Dispatch to the appropriate worker based on tags
                if all([t in event['Notifications']['tags'] for t in ['application', 'monitor'] ]):
                    logger.debug("Process monitor app notification delivery")
                #if event['Notifications']['relatedNode']['to'] == None:
                #    loop.create_task(admin_system_alarm_email_worker(event['Notifications']['relatedNode']))
                
    except Exception as e:
        logger.error(e)

