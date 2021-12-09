""" Resolve the state of generic monitor objects

"""

import asyncio
from typing import cast

from aiohttp.helpers import current_task
import backoff
from envyaml import EnvYAML
import json
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import logging
import logging.config 
import os
import time

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("states")

# Translating ui keys to python values for conditions operators
operators_dict = {'=': '==', '<': '<', '>': '>', '!=': '!=', 'contains': 'in'}

def typecasting(value):
    casted_value = value
    if casted_value == "true":
        casted_value = True
    elif casted_value == "false":
        casted_value = False
    elif type(casted_value) == type({"a": 1}):
        casted_value = str(casted_value)
    
    return casted_value

async def resolve_monitor_object_state_generic_worker(event):
    """
    """
    try:
        logger.info(event)
        
        value = event['value']
        condition = event['object']['condition']
        monitor_object = event['object']['objectsToObjectsByObject2Id']
        states = monitor_object[0]['object1']['objectsToObjectsByObject1Id']
        
        #logger.debug("Monitor object: {}".format(monitor_object))
        
        # logger.info(value)
        # logger.info(condition)
        # logger.info(monitor_object)
        # logger.info(states)
        
        # Check pre-conditions for processing states
        
        if len(monitor_object) < 1:
            #logger.info("======> monitor_object")
            return 0
        
        if len(states) < 1:
            #logger.info("======> states")
            return 0
        
        if monitor_object[0]['object1']['enabled'] == False or event['object']['enabled'] == False:
            #logger.info("======> enabled condition")
            return 0
        
        # Processing states
        
        current_icon = monitor_object[0]['object1']['color']
        current_color = monitor_object[0]['object1']['icon']
        
        # logger.info(current_icon)
        # logger.info(current_color)
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
                
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # Extract states and order them by name
        states = sorted([state['object2'] for state in states], key = lambda k: k['name'])
        default_state = [s for s in states if 'Default' in s['name']]
        
        if len(default_state) > 0:
            default_color = default_state[0]['color']
            default_icon = default_state[0]['icon']
        else:
            default_color = 'Default'
            default_icon = ''
        
        #logger.info(default_state)
        
        for state in states: 
            #logger.debug("Processing state: {}".format(state))
            try:
                if state['enabled'] == True:
                    state_condition = state['condition']
                    state_value = state['value']
                    state_icon = state['icon']
                    state_color = state['color']
                    if eval("{} {} {}".format(typecasting(state_value), operators_dict[state_condition['operator']], typecasting(state_condition['value']))):
                        #logger.info("Condition met")
                        if current_icon != state_icon and current_color != state_color:
                            
                            optionsArrayPayload = "[{ groupName: \"Current state\", property: \"Color\", value: \"" + str(state_color) + "\"}, { groupName: \"Current state\", property: \"Icon\", value: \"" + str(state_icon) + "\"}]"
                            
                            mutation_update_state = gql(
                                """
                                mutation UpdateMonitorObjectState {{
                                updateObjectPropertiesByName(input: {{
                                    objectId: "{}"
                                    transactionId: "{}"
                                    propertiesArray: {}
                                    }}){{
                                        boolean
                                    }}
                                }}
                                """.format(monitor_object[0]['object1']['id'], round(time.time() * 1000), optionsArrayPayload)
                            )
                            mutation_update_state_result = await client.execute_async(mutation_update_state)
                            logger.info("🟢 State updated to color {}, icon {}, from conditions {} based on value {} for object {}.".format(state_color, state_icon, state_condition, state_value, monitor_object[0]['object1']['id']))
                            return 0 
            except Exception as e:
                continue
        
        # If no optional state is matching go back to default state
        if current_color != default_color:
            optionsArrayPayload = "[{ groupName: \"Current state\", property: \"Color\", value: \""+ default_color +"\"}, { groupName: \"Current state\", property: \"Icon\", value: \""+ default_icon +"\"}]"
                            
            mutation_update_state = gql(
                """
                mutation UpdateMonitorObjectState {{
                updateObjectPropertiesByName(input: {{
                    objectId: "{}"
                    transactionId: "{}"
                    propertiesArray: {}
                    }}){{
                        boolean
                    }}
                }}
                """.format(monitor_object[0]['object1']['id'], round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            # logger.info("🟢 State updated to default")
            return 0
        
    except Exception as e:
        logger.error("🔴 {}".format(e))

@backoff.on_exception(backoff.expo, Exception)
async def resolve_monitor_object_state_generic_subscription():
    """
    """
    try:
        # Grab async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # Check states on startup
        init_transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        init_client = Client(transport=init_transport, fetch_schema_from_transport=False)
        
        init_query = gql(
            """
            query ResolveMonitorObjectStateGeneric {
                objects(filter: {
                    schemaTags: {
                        contains: ["application", "monitor", "object state"]
                    }
                })
            }
            """
        )
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            
            # GQL subscription for ...
            # ... on Object {
            #                             id
            #                             schemaType
            #                             name
            #                             enabled
            #                             tags
            #                             schemaId
            #                         }
            subscription = gql(
                """
                subscription ResolveMonitorObjectStateGeneric {
                    Objects(filterA: {
                        tags: ["application", "monitor", "object state"]
                    }){
                        event
                        relatedNode {
                                    
                                    ... on ObjectProperty {
                                        id
                                        objectId
                                        groupName
                                        property
                                        value 
                                        object {
                                            id
                                            name
                                            enabled
                                            color: property(propertyName: "State/Color")
                                            icon: property(propertyName: "State/Icon")
                                            value: property(propertyName: "State/Value")
                                            condition: property(propertyName: "State/Condition")
                                            objectsToObjectsByObject2Id(filter: {
                                                object1: {
                                                    schemaTags: {
                                                    contains: ["application", "monitor", "object"]
                                                    }
                                                }
                                                object2 : {
                                                    schemaTags : {
                                                    contains: ["application", "monitor", "object state"]
                                                    }
                                                }
                                            }){
                                                object1 {
                                                    id
                                                    name
                                                    enabled
                                                    color: property(propertyName: "Current state/Color")
                                                    icon: property(propertyName: "Current state/Icon")
                                                    objectsToObjectsByObject1Id(filter: {
                                                        object2 : {
                                                            schemaTags : {
                                                                contains: ["application", "monitor", "object state"]
                                                            }
                                                        }
                                                        }){
                                                        object2 {
                                                            id
                                                            name
                                                            enabled
                                                            color: property(propertyName: "State/Color")
                                                            icon: property(propertyName: "State/Icon")
                                                            value: property(propertyName: "State/Value")
                                                            condition: property(propertyName: "State/Condition")
                                                        }
                                                    }
                                                }
                                                
                                            }
                                        }
                                    }
                                }
                        }
                    }
                """
            )
            
            logger.info("Dispatcher subscribed on monitor objects state events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                #logger.info("Monitor state event: {}".format(event))
                #if "delete" not in event['Objects']['event']:
                if "update" in event['Objects']['event'] or "insert" in event['Objects']['event']:
                    loop.create_task(resolve_monitor_object_state_generic_worker(event['Objects']['relatedNode']))
        
    except Exception as e:
        logger.error(e)    
    
    return 0