""" Counter Statistics

This worker manages the counter statistics objects. These objects count the number of objects of a given schema id that satisfy a set of criteria.

"""

import asyncio
from aiohttp.helpers import current_task
import backoff
import dateutil.parser
import datetime
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
logger = logging.getLogger("statistics")

# Translating ui keys to python values for conditions operators
operators_dict = {'=': '==', '<': '<', '>': '>', '!=': '!=', 'contains': 'in'}

# Frequency of the main loop in seconds
timeout = 5

@backoff.on_exception(backoff.expo, Exception)
async def subscribe_to_objects(event):
    """ Subscribe to objects
    
    Tracks objects of a given schema id that satisfy a set of criteria. Those objects are then counted to update a counter statistics object.
    
    Parameters:
    -----------
        event: json object, records the changes on a counter statistics object or its properties
        
    Returns:
    --------
        none: acts by side effects on the counter statistics object by updating the Value/Value and Value/Percentage properties.
    
    """
    try:
        # Grab async event loop
        loop = asyncio.get_event_loop()
        
        #logger.info(event)
        
        if 'object' in event:
            statistics_object_id = event['object']['id']
        else:
            statistics_object_id = event['id']
        
        current_value = None 
        
        while True:
            await asyncio.sleep(timeout)
            
            # GQL parameters
            gql_address = config['gql']['address']
            jwt_env_key = "JWT_ENV_DISPATCHER"
            jwt = os.environ[jwt_env_key]
            
            transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            
            # Two different branches to handle object creation and configuration updates
            if 'objectId' in event:
                schemaId = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'SchemaId'][0]
                linkedOnly = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Linked only'][0]
                objFilter = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Filter'][0]
            else:
                schemaId = [x['value'] for x in event['objectProperties'] if x['property'] == 'SchemaId'][0]
                linkedOnly = [x['value'] for x in event['objectProperties'] if x['property'] == 'Linked only'][0]
                objFilter = [x['value'] for x in event['objectProperties'] if x['property'] == 'Filter'][0]
                
            find_objects_query = gql(
            """
            query FindObjects {{
                objects(filter: {{
                    schemaId: {{
                        equalTo: "{}"
                    }}
                }}) {{
                    id
                    enabled
                    objectProperties {{
                        id
                        groupName
                        property
                        value
                    }}
                    objectsToObjectsByObject2Id {{
                    id
                    object1Id
                    object2Id
                    }}
                }}
            }}
            """.format(schemaId)
            )
            find_objects_result = await client.execute_async(find_objects_query)
            
            #logger.info(find_objects_result)
            
            total_count = len(find_objects_result['objects'])
            
            if linkedOnly:
                objects = [obj for obj in find_objects_result['objects'] if any([oid['object1Id'] == statistics_object_id for oid in obj['objectsToObjectsByObject2Id']])]
            else:
                objects = find_objects_result['objects']
                
            # TODO: Fix filtering logic
            if objFilter['filtering']:
                objects = [obj for obj in objects]
                #objects = [obj for obj in objects if all([eval("{} {} {}".format(oid['value'])) for oid in obj['objectProperties']])]
                
            value = len(objects)
            percentage = 100.0 * len(objects) / total_count
            
            if current_value != value:
                current_value = value
        
                optionsArrayPayload = "[{{ groupName: \"Value\", property: \"Value\", value: {}}}, {{ groupName: \"Value\", property: \"Percentage\", value: {}}}]".format(value, percentage) 
                mutation_update_state = gql(
                    """
                    mutation UpdateStatisticsObjectState {{
                    updateObjectPropertiesByName(input: {{
                        objectId: "{}"
                        transactionId: "{}"
                        propertiesArray: {}
                        }}){{
                            boolean
                        }}
                    }}
                    """.format(statistics_object_id, round(time.time() * 1000), optionsArrayPayload)
                )
                mutation_update_state_result = await client.execute_async(mutation_update_state)
        
    except Exception as e:
        logger.error(e)   

@backoff.on_exception(backoff.expo, Exception)
async def counter_statistics_subscription():
    """  Counter statistics subscription
    
    Every time the dispatcher starts a subcription is created to track the creation, deletion and updates of counter statistics objects.
    When a counter statistics object is created or updated, an asynchronous task is triggered that is responsible to update the Value/Value (count) and Value/Percentage properties of the counter statistics object.
    When a counter statistics object is deleted the asynchronous task that was triggered to update its values is canceled.
    
    Parameters:
    -----------
        none: is triggered when the dispatcher starts by reading its configuration from the dispatcher app profile
    Returns:
    --------
        none: acts by side effects, it creates and cancel tasks related to counter statistics objects
    
    """
    try:
        # Grab async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # Check existing statistics objects on startup
        transport_init = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client_init = Client(transport=transport_init, fetch_schema_from_transport=False)
        
        query_existing_statistics = gql(
            """
            query FindCounterStatisticsObjects{
                    objects(filter: {
                        schemaTags: {
                            contains: ["application", "dispatcher", "statistics", "counter"]
                        }
                    }) {
                        id
                        schemaType
                        name
                        enabled
                        tags
                        schemaId
                        objectProperties {
                            id 
                            groupName
                            property
                            value
                        }
                    }
                }
            """
        )
        existing_statistics_result = await client_init.execute_async(query_existing_statistics)
        
        #logger.info(existing_statistics_result)
        
        for stats_object in existing_statistics_result['objects']:
            # Find the subscription task to be canceled
            task = [task for task in asyncio.all_tasks() if task.get_name() == "counter_statistics_sub_{}".format(stats_object['id'])]
            # Cancel it
            if len(task) > 0:
                task[0].cancel()
            
            loop.create_task(subscribe_to_objects(stats_object), name = "counter_statistics_sub_{}".format(stats_object['id']))
        
        # Handle creation of new objects and changes in configuration of existing objects
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:

            # GQL subscription for ...
            subscription = gql(
                """
                subscription StatisticsObjects {
                    Objects(filterA: {
                        tags: ["application", "dispatcher", "statistics", "counter"]
                        propertyChanged: [{groupName: "Settings", property: "SchemaId"}, {groupName: "Settings", property: "Linked only"}, {groupName: "Settings", property: "Filter"}]
                    }){
                        event
                        relatedNode {
                                    ... on Object {
                                        id
                                        schemaType
                                        name
                                        enabled
                                        tags
                                        schemaId
                                        objectProperties {
                                            id 
                                            groupName
                                            property
                                            value
                                        }
                                    }
                                    ... on ObjectProperty {
                                        id
                                        objectId
                                        groupName
                                        property
                                        value 
                                        updatedAt
                                        object {
                                            id
                                            schemaType
                                            name
                                            enabled
                                            tags
                                            schemaId
                                            objectProperties {
                                                id 
                                                groupName
                                                property
                                                value
                                            }
                                        }    
                                        }
                                    }
                                }
                        }
                    
                """
            )
            
            logger.info("🎧 Dispatcher subscribed on counter statistics objects events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                #logger.info(event)
                
                # Two branches to process creation/deletion separately from config changes
                if 'id' in event['Objects']['relatedNode']:
                    object_id = event['Objects']['relatedNode']['id']
                else:
                    object_id = event['Objects']['relatedNode']['object']['id']
                
                if "delete" not in event['Objects']['event']:
                    loop.create_task(subscribe_to_objects(event['Objects']['relatedNode']), name = "counter_statistics_sub_{}".format(object_id))
                else:
                    # Find the subscription task to be canceled
                    task = [task for task in asyncio.all_tasks() if task.get_name() == "counter_statistics_sub_{}".format(object_id)]
                    # Cancel it
                    if len(task) > 0:
                        task[0].cancel()
        
    except Exception as e:
        logger.error(e)    
