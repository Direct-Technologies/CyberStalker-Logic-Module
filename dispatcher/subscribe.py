import logging
import logging.config 
import yaml
import base64
import json
import string
from envyaml import EnvYAML
import asyncio
import os
import sys
import backoff
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

# Import the workers of each application from the workers submodules
from admin_workers import * 
from board_workers import *
from dispatcher_workers import *
from user_management_workers import *
import authenticate as auth

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("subs")

def trigger_parser(type_, event, conditions):
    """Trigger parser

        Parse events and trigger conditions to decide when a worker must be spawned.

        Parameters
        ----------
        type_ : str
            Must be one of 'CONTROL', 'OBJECT', 'OBJECT_PROPERTY', 'NOTIFICATION', 'USER'.
            
        event : dict
            A GraphQL subscription message.
            
        conditions : str
            A trigger conditions string in the proper format.
            
        Returns:
        --------
        A boolean. True if the worker must be spawned and False otherwise.
    """
    try:
        if type_ == 'CONTROL':
            if '*' in conditions:
                return True
        elif type_ == 'OBJECT':
            object_tags = event['listen']['relatedNode']['tags']
            if conditions == "*" or "[*]" in conditions:
                return True 
            else:
                for c in conditions.split(','):
                    if all([t in object_tags for t in c[1:-1].split('|')]):
                        return True
                return False
        elif type_ == 'OBJECT_PROPERTY':
            object_tags = event['listen']['relatedNode']['object']['tags']
            group_name = event['listen']['relatedNode']['groupName']
            property_name = event['listen']['relatedNode']['property']
            if conditions == "*":
                return True 
            else:
                for c in conditions.split(','):
                    c_tags = c.split(']')[0][1:].split('|')
                    c_group = c.split(']')[1].split('/')[0]
                    c_property = c.split(']')[1].split('/')[1]
                    if object_tags != None:
                        if (all([t in object_tags for t in c_tags]) or '*' in c_tags) and (c_group == group_name or c_group == "*") and (c_property == property_name or c_property == "*"):
                            return True
                    else:
                        if ('*' in c_tags) and (c_group == group_name or c_group == "*") and (c_property == property_name or c_property == "*"):
                            return True
                return False
        elif type_ == 'NOTIFICATION':
            notification_tags = event['listen']['relatedNode']['tags']
            if conditions == "*" or "[*]" in conditions:
                return True 
            else:
                for c in conditions.split(','):
                    if all([t in notification_tags for t in c.split('|')]):
                        return True
                return False
        elif type_ == 'USER':
            user_tags = event['listen']['relatedNode']['mTags']
            if conditions == "*" or "[*]" in conditions:
                return True 
            else:
                for c in conditions.split(','):
                    if all([t in user_tags for t in c.split('|')]):
                        return True
                return False
        else: 
            return False
    except Exception as e:
        logger.error(e)
        return False

async def subscribe(gql_address, jwt_env_key, topic, rules):
    """Subscription channel on a given topic

        Listen for events on a GraphQL subscription channel and spawns workers
        according to the rules defined in the configuration file.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt_env_key : str
            Key used to retrieve the active JWT from the environment variables. 
            Format JWT_ENV_{NAME OF APPLICATION FROM CONFIG}.
            
        topic : str
            The topic defining the GraphQL subscription channel. Format {type}:{token_id}
            
        rules : str
            The dispatching rules as defined in the configuration file.
    """
    # Get access token from the environment
    jwt = os.environ[jwt_env_key]
    # Get the async loop
    loop = asyncio.get_event_loop()
    # Start a Websocket session
    transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
    async with Client(
        transport=transport, fetch_schema_from_transport=False,
    ) as session:
        # Start a GraphQL subscription specifically for objects
        objects_subscription = gql(
            """
            subscription {{
                listen(topic: "{}") {{
                    relatedNode {{
                        ... on Object {{
                            id
                            schemaType
                            name
                            enabled
                            tags
                            schemaId
                        }}
                        ... on ObjectProperty {{
                            id
                            objectId
                            groupName
                            property
                            value 
                            object {{
                                id
                                schemaType
                                name
                                enabled
                                tags
                                schemaId
                            }}
                        }}
                    }}
                }}
            }}
        """.format(topic)
        )
        # Start a GraphQL subscription specifically for misc
        misc_subscription = gql(
            """
            subscription {{
                listen(topic: "{}") {{
                    relatedNode {{
                        ... on User {{
                            id
                            login
                            password
                            enabled
                            description
                            mName
                            mPhone
                            mEmail
                            mTags
                            type
                            activated
                            passwordReset
                        }}
                    }}
                }}
            }}
        """.format(topic)
        )
        # Start a GraphQl subscription specifically for notifications
        notifications_subscription = gql(
            """
            subscription {{
                listen(topic: "{}") {{
                    relatedNode {{
                        ... on Notification {{
                            id
                            actorType
                            actor
                            actorName
                            tags
                            message
                            spec
                            createdAt
                        }}
                    }}
                }}
            }}
        """.format(topic)
        )
        # Start a GraphQL subscription specifically for controls
        # Necessary to not mix id fields of type uuid and int (specific to controls)
        controls_subscription = gql(
            """
            subscription {{
                listen(topic: "{}") {{
                    relatedNodeId
                    relatedNode {{
                        ... on ControlExecution {{
                            id
                            objectId
                            callerId
                            controller
                            name
                            type
                            params
                            linkedControlId
                            done
                        }}
                    }}
                }}
            }}
        """.format(topic)
        )
        # Listen for events on the subscription
        # Controls must be separated from standard subscriptions because of different id types.
        # Standard ids are uuids while controls use ints.
        if "controls" in topic:
            subscription = controls_subscription
        elif "objects" in topic:
            subscription = objects_subscription
        elif "notifications" in topic:
            subscription = notifications_subscription
        elif "misc" in topic:
            subscription = misc_subscription
        else:
            logger.error("Subscription error, unhandled topic: {}".format(topic))
            sys.exit(1)
        logger.info("Listening on {}".format(topic))
        # Process coming events
        async for event in session.subscribe(subscription):
            try:
                # Handle controls
                if "controls" in topic:
                    logger.debug("Event on topic {} ->\n{}".format(topic, event))
                    # If normal RPC
                    if event['listen']['relatedNode'] != None:
                        control_payload = event['listen']['relatedNode']
                    # If stealth RPC
                    else:
                        control_payload = json.loads(base64.b64decode(event['listen']['relatedNodeId']))[0]
                    for k, v in rules.items():
                        if trigger_parser('CONTROL', event, v):
                            logger.debug("Spawn RPC worker {} on {}.".format(k, topic))
                            loop.create_task(eval("{}(gql_address, jwt, control_payload)".format(k)))
                # Handle objects / objects properties
                elif "objects" in topic:
                    # If the event concerns an object property
                    if 'property' in list(event['listen']['relatedNode'].keys()):
                        for k, v in rules.items():
                            if trigger_parser('OBJECT_PROPERTY', event, v):
                                logger.debug("Spawn worker {} on {}".format(k, topic))
                                loop.create_task(eval("{}(gql_address, jwt, event['listen']['relatedNode'])".format(k)))
                    # If the event concerns an object
                    else:
                        for k, v in rules.items():
                            if trigger_parser('OBJECT', event, v):
                                logger.debug("Spawn worker {} on {}".format(k, topic))
                                loop.create_task(eval("{}(gql_address, jwt, event['listen']['relatedNode'])".format(k)))
                # Handle notifications
                elif "notifications" in topic:
                    #logger.debug("Event on topic {} ->\n{}".format(topic, event))
                    for k, v in rules.items():
                        if trigger_parser('NOTIFICATION', event, v):
                            logger.debug("Spawn worker {} on {}".format(k, topic))
                            loop.create_task(eval("{}(gql_address, jwt, event['listen']['relatedNode'])".format(k)))
                # Handle everything else
                elif "misc" in topic:
                    #logger.debug("Event on topic {} ->\n{}".format(topic, event))
                    # If the event concerns a user
                    if 'login' in list(event['listen']['relatedNode'].keys()):
                        for k, v in rules.items():
                            if trigger_parser('USER', event, v):
                                logger.debug("Spawn worker {} on {}".format(k, topic))
                                loop.create_task(eval("{}(gql_address, jwt, event['listen']['relatedNode'])".format(k)))
                else:
                    # DEBUG
                    logger.debug("Unhandled event")
            except Exception as e:
                logger.error(e)
                continue
            
@backoff.on_exception(backoff.expo, Exception)
async def subscribe_on_topic(application, topic):
    """Subscription channel on a given topic

        Listen for events on a GraphQL subscription channel and spawns workers
        according to the rules defined in the configuration file.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt_env_key : str
            Key used to retrieve the active JWT from the environment variables. 
            Format JWT_ENV_{NAME OF APPLICATION FROM CONFIG}.
            
        topic : str
            The topic defining the GraphQL subscription channel. Format {type}:{token_id}
            
        rules : str
            The dispatching rules as defined in the configuration file.
    """
    try:
        # Connection info
        gql_address = config["gql"]["address"]
        # Get access token from the environment
        jwt_env_key = "JWT_ENV_"+application.upper()
        jwt = os.environ[jwt_env_key]
        token_id = await auth.get_token_id(gql_address, jwt_env_key)
        topic = topic+":"+token_id
        # Get the async loop
        loop = asyncio.get_event_loop()
        # Start a Websocket session
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            controls_subscription = gql(
                """
                subscription {{
                    listen(topic: "{}") {{
                        relatedNodeId
                        relatedNode {{
                            ... on ControlExecution {{
                                id
                                objectId
                                callerId
                                controller
                                name
                                type
                                params
                                linkedControlId
                                done
                            }}
                        }}
                    }}
                }}
            """.format(topic)
            )
            # Listen for events on the subscription
            if "controls" in topic:
                subscription = controls_subscription
            else:
                logger.error("Subscription error, unhandled topic: {}".format(topic))
            logger.info("Listening on {} for {}".format(topic, application))
            # Process coming events
            async for event in session.subscribe(subscription):
                # Handle controls
                if "controls" in topic:
                    #logger.info("Event on topic {} ->\n{}".format(topic, event))
                    # If normal RPC
                    if event['listen']['relatedNode'] != None:
                        control_payload = event['listen']['relatedNode']
                    # If stealth RPC
                    else:
                        control_payload = json.loads(base64.b64decode(event['listen']['relatedNodeId']))[0]
                    for k in config['apps'][application]['rpc']:
                        loop.create_task(eval("{}(gql_address, jwt, control_payload)".format(k)))
                else:
                    # DEBUG
                    logger.debug("Unhandled event")
    except Exception as e:
        logger.error(e)
        
    return 0