""" Objects Statistics

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
import js2py

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("statistics")

# Translating ui keys to python values for conditions operators
operators_dict = {'=': '==', '<': '<', '>': '>', '!=': '!=', 'contains': 'in'}

async def compute_statistics(analyzedObjects, statisticsObject):
    """ Compute statistics on a property
    
    Update the statistics property on new event.
    
    Args:
    =====
        event: json object
            An 'objects', 'relatedNode' event.
        statisticsObject: json object
            An event that corresponds to the initialization of a statistics subscription.
    Returns:
    ========
        None
    """
    try:
        #logger.info(analyzedObjects)
        #logger.info(statisticsObject)
        
        if 'object' in statisticsObject:
            statistics_object_id = statisticsObject['object']['id']
            function = [p['value'] for p in statisticsObject['object']['objectProperties'] if p['property'] == 'Function'][0]
        else:
            statistics_object_id = statisticsObject['id']
            function = [p['value'] for p in statisticsObject['objectProperties'] if p['property'] == 'Function'][0]
        
        if len(analyzedObjects['objects']) > 0:
           values = [obj['value'] for obj in analyzedObjects['objects'] if obj['value'] is not None]
        else:
            values = []
        
        if len(values) < 1:
           value = "{{value: {}}}".format('n/a')
        elif function == 'Min':
            value = "{{value: {}}}".format(min(values))
        elif function == 'Max':
            value = "{{value: {}}}".format(max(values))
        elif function == 'Average':
            value = "{{value: {}}}".format(sum(values)/len(values))
        elif function == 'Custom':
            custom_code = [p['value'] for p in statisticsObject['object']['objectProperties'] if p['property'] == 'Custom function'][0]["code"]
            custom_function = js2py.eval_js(custom_code)
            
            value = "{{value: {}}}".format(custom_function(values))
        else:
            value = "{{value: {}}}".format('n/a')
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        optionsArrayPayload = "[{{ groupName: \"Value\", property: \"Value\", value: {}}}]".format(value) # str(analyzedObjects['objects']).replace("'", '"')
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
        
        return 0
        
    except Exception as e:
        logger.error(e)   

@backoff.on_exception(backoff.expo, Exception)
async def subscribe_to_object_property(event):
    """ Property subscription for a given statistics
    
    Args:
    =====
        event: json object
            An 'objects', 'relatedNode' event.
    Returns:
    ========
        None
    """
    try:
        # Grab async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        #logger.info(event)
        
        if 'object' in event:
            statistics_object_id = event['object']['id']
        else:
            statistics_object_id = event['id']
        
        statisticsObject = event
        
        if 'objectId' in event:
            logger.info("schemaId")
            schemaId = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'SchemaId'][0]
            propertyName = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Property'][0]
            linkedOnly = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Linked only'][0]
            objFilter = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Filter'][0]
        else:
            logger.info("no schemaId")
            schemaId = [x['value'] for x in event['objectProperties'] if x['property'] == 'SchemaId'][0]
            propertyName = [x['value'] for x in event['objectProperties'] if x['property'] == 'Property'][0]
            linkedOnly = [x['value'] for x in event['objectProperties'] if x['property'] == 'Linked only'][0]
            objFilter = [x['value'] for x in event['objectProperties'] if x['property'] == 'Filter'][0]
            
        #logger.info(schemaId)
        #logger.info(propertyName)
        #logger.info(propertyName.split("/")[0])
        #logger.info(propertyName.split("/")[1])
        
        transport_init = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client_init = Client(transport=transport_init, fetch_schema_from_transport=False)
        
        find_properties_query = gql(
            """
            query FindProperties {{
                objects(filter: {{
                    schemaId: {{
                        equalTo: "{}"
                    }}
                }}){{
                    id
                    value: property(propertyName: "{}/{}")
                    objectsToObjectsByObject2Id {{
                    id
                    object1Id
                    object2Id
                    }}
                }}
            }}
            """.format(schemaId, propertyName.split("/")[0], propertyName.split("/")[1])
        )
        find_properties_result = await client_init.execute_async(find_properties_query)
        
        #logger.info(find_properties_result)
        
        # Init calculation
        #loop.create_task(compute_statistics(find_properties_result, statisticsObject))
        
        if linkedOnly:
            ids_list = [obj['id'] for obj in find_properties_result['objects'] if any([oid['object1Id'] == statistics_object_id for oid in obj['objectsToObjectsByObject2Id']])]
            filtered_properties_result = {"objects": [obj for obj in find_properties_result['objects'] if any([oid['object1Id'] == statistics_object_id for oid in obj['objectsToObjectsByObject2Id']])]}
            loop.create_task(compute_statistics(filtered_properties_result, statisticsObject))
        else:
            ids_list = [obj['id'] for obj in find_properties_result['objects']]
            loop.create_task(compute_statistics(find_properties_result, statisticsObject))
        
        #ids_list = []
        #for obj in find_properties_result['objects']:
        #    ids_list.append(obj['id'])
        
        ids_string = str(ids_list).replace("'", '"')
            
        if schemaId is not None and propertyName is not None and len(find_properties_result['objects']) > 0:
            # GQL client
            transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
            async with Client(
                transport=transport, fetch_schema_from_transport=False,
            ) as session:

                # GQL subscription for ...
                subscription = gql(
                    """
                    subscription StatisticsObjectsProperties {{
                        Objects(filterA: {{
                            id: {}
                            propertyChanged: [{{groupName: "{}", property: "{}"}}]
                        }}){{
                            event
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
                                            updatedAt    
                                            }}
                                        }}
                                    }}
                            }}
                        
                    """.format(ids_string, propertyName.split("/")[0], propertyName.split("/")[1])
                )
                
                logger.info("🎧 Dispatcher subscribed on object properties events for statistics.")
                
                # Send event to be processed by the worker
                async for event in session.subscribe(subscription):
                    #logger.info(event)
                    
                    # We don't process deletion of the statistics objects
                    if "delete" not in event['Objects']['event']:
                        
                        if 'object' in statisticsObject:
                            statistics_object_id = statisticsObject['object']['id']
                        else:
                            statistics_object_id = statisticsObject['id']
                        
                        # Check existing statistics objects on startup
                        logger.info("Update statistics config info")
                        transport_init = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
                        client_init = Client(transport=transport_init, fetch_schema_from_transport=False)
                        
                        query_existing_statistics = gql(
                            """
                            query FindStatisticsObjects{{
                                    objects(filter: {{
                                        schemaTags: {{
                                            contains: ["application", "dispatcher", "statistics", "objects"]
                                        }}
                                        id: {{
                                            equalTo: "{}"
                                        }}
                                    }}) {{
                                        id
                                        schemaType
                                        name
                                        enabled
                                        tags
                                        schemaId
                                        objectProperties {{
                                            id 
                                            groupName
                                            property
                                            value
                                        }}
                                    }}
                                }}
                            """.format(statistics_object_id)
                        )
                        existing_statistics_result = await client_init.execute_async(query_existing_statistics)
                        
                        #logger.info(existing_statistics_result)
                        
                        for stats_object in existing_statistics_result['objects']:
                            statisticsObject = stats_object
                        
                        find_properties_query = gql(
                            """
                            query FindProperties {{
                                objects(filter: {{
                                    schemaId: {{
                                        equalTo: "{}"
                                    }}
                                }}){{
                                    id
                                    value: property(propertyName: "{}/{}")
                                }}
                            }}
                            """.format(schemaId, propertyName.split("/")[0], propertyName.split("/")[1])
                        )
                        find_properties_result = await client_init.execute_async(find_properties_query)
                        
                        #logger.info(find_properties_result)
                        
                        loop.create_task(compute_statistics(find_properties_result, statisticsObject))
                        
                    else:
                        logger.info("Statistics subscription closed.")
                        return 0
                        
            return 0
        
    except Exception as e:
        logger.error(e)   

@backoff.on_exception(backoff.expo, Exception)
async def analyzer_subscription():
    """ Analyzer statistics objects subscription
    
    Create a subscription that listens on statistics objects events as identified by the tags ['application', 'disptacher', 'statistics', 'timeseries'].
    
    Spawns and restarts subscriptions for specific statistics object on creation or updates.
    
    Args:
    =====
        None
    Returns:
    ========
        None
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
            query FindStatisticsObjects{
                    objects(filter: {
                        schemaTags: {
                            contains: ["application", "dispatcher", "statistics", "objects"]
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
            loop.create_task(subscribe_to_object_property(stats_object))
        
        # GQL client for websocket
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:

            # GQL subscription for ...
            subscription = gql(
                """
                subscription StatisticsObjects {
                    Objects(filterA: {
                        tags: ["application", "dispatcher", "statistics", "objects"]
                        propertyChanged: [{groupName: "Settings", property: "Function"}, {groupName: "Settings", property: "Filter"}, {groupName: "Settings", property: "Linked only"}, {groupName: "Settings", property: "Property"}, {groupName: "Settings", property: "SchemaId"}, {groupName: "Settings", property: "Custom function"}]
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
            
            logger.info("🎧 Dispatcher subscribed on objects statistics events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                #logger.info(event)
                
                # Two branches to process creation/deletion separately from config changes
                if 'id' in event['Objects']['relatedNode']:
                    object_id = event['Objects']['relatedNode']['id']
                else:
                    object_id = event['Objects']['relatedNode']['object']['id']
                
                # We don't process deletion of the alarms
                if "delete" not in event['Objects']['event']:
                    # Find the subscription task to be canceled before they are resetted with a new configuration
                    task = [task for task in asyncio.all_tasks() if task.get_name() == "objects_statistics_sub_{}".format(object_id)]
                    # Cancel it
                    if len(task) > 0:
                        task[0].cancel()
                    
                    loop.create_task(subscribe_to_object_property(event['Objects']['relatedNode']), name = "objects_statistics_sub_{}".format(object_id))
                else:
                    # Find the subscription task to be canceled
                    task = [task for task in asyncio.all_tasks() if task.get_name() == "objects_statistics_sub_{}".format(object_id)]
                    # Cancel it
                    if len(task) > 0:
                        task[0].cancel()
        
    except Exception as e:
        logger.error(e)    
