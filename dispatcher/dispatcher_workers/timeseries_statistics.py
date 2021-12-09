""" Timeseries Statistics

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

async def compute_statistics(event, statisticsObject):
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
        logger.info(event)
        # Extracting event['value'] and event['updatedAt']
        #logger.info(statisticsObject['object'])
        # statistics_object_id = statisticsObject['object']['id']
        # json.loads(statisticsObject['object']['objectProperties'])
        # current_timeseries = [p['value'] for p in json.loads(statisticsObject['object']['objectProperties']) if p['property'] == 'Timeseries'][0]
        
        if 'object' in statisticsObject:
            statistics_object_id = statisticsObject['object']['id']
            current_timeseries = [p['value'] for p in statisticsObject['object']['objectProperties'] if p['property'] == 'Timeseries'][0]
            period = int([p['value'] for p in statisticsObject['object']['objectProperties'] if p['property'] == 'Period'][0])
            function = [p['value'] for p in statisticsObject['object']['objectProperties'] if p['property'] == 'Function'][0]
        else:
            statistics_object_id = statisticsObject['id']
            current_timeseries = [p['value'] for p in statisticsObject['objectProperties'] if p['property'] == 'Timeseries'][0]
            period = int([p['value'] for p in statisticsObject['objectProperties'] if p['property'] == 'Period'][0])
            function = [p['value'] for p in statisticsObject['objectProperties'] if p['property'] == 'Function'][0]
        
        logger.info("Current timeseries")
        logger.info(current_timeseries)
        logger.info("New value")
        logger.info({'value': event['value'], 'timestamp': event['updatedAt']})
        
        now = datetime.datetime.now()
        logger.info("Reference time: {}".format(now))
        #logger.info("Current time series")
        #logger.info([(now.timestamp() - dateutil.parser.isoparse(v['timestamp']).replace(tzinfo=None).timestamp()) for v in current_timeseries])
        
        if current_timeseries != [{}]:
        
            #list_timeseries = [v['value'] for v in current_timeseries if (now.timestamp() - dateutil.parser.isoparse(v['timestamp']).replace(tzinfo=None).timestamp()) < period]
            #   list_timeseries = [v['y'] for v in current_timeseries if (now.timestamp() - dateutil.parser.isoparse(v['x']).replace(tzinfo=None).timestamp()) < period]
            list_timeseries = [v['y'] for v in current_timeseries if (now.timestamp() - v['x']) < period]
            #list_timeseries = [v['value'] for v in current_timeseries if (now - dateutil.parser.isoparse(v['timestamp']).replace(tzinfo=None)).total_seconds() < period]
            list_timeseries.append(event['value'])
            
            logger.info("List timeseries")
            logger.info(list_timeseries)
            
            #logger.info("Current timeseries")
            #logger.info([(now.timestamp() - dateutil.parser.isoparse(v['timestamp']).replace(tzinfo=None).timestamp()) for v in current_timeseries])
            
            #   current_timeseries = ["{{value: {}, timestamp: '{}'}}".format(v['value'], v['timestamp']) for v in current_timeseries if (now.timestamp() - dateutil.parser.isoparse(v['timestamp']).replace(tzinfo=None).timestamp()) < period]
            #   current_timeseries = ["{{value: {}, timestamp: '{}'}}".format(v['value'], v['timestamp']) for v in current_timeseries if (now - dateutil.parser.isoparse(v['timestamp']).replace(tzinfo=None)).total_seconds() < period]
            #   current_timeseries = ["{{y: {}, x: '{}'}}".format(v['y'], v['x']) for v in current_timeseries if (now - dateutil.parser.isoparse(v['x']).replace(tzinfo=None)).total_seconds() < period]
            current_timeseries = ["{{y: {}, x: {}}}".format(v['y'], v['x']) for v in current_timeseries if (now.timestamp() - v['x']) < period]
        
        else :
            
            current_timeseries = []
            list_timeseries = []
            list_timeseries.append(event['value'])
        
        #current_timeseries.append("{{value: {}, timestamp: '{}'}}".format(event['value'], event['updatedAt']))
        current_timeseries.append("{{y: {}, x: {}}}".format(event['value'], dateutil.parser.isoparse(event['updatedAt']).replace(tzinfo=None).timestamp()))
        
        logger.info("Updated timeseries")
        logger.info(current_timeseries)
        updated_timeseries = current_timeseries
        
        
        string_timeseries = str(updated_timeseries).replace("'", "")#str(updated_timeseries).replace('"', "").replace("'", '"')
        
        logger.info("String timeseries")
        logger.info(string_timeseries)
        
        if function == 'Timeseries':
            value = "{{value: {}}}".format(string_timeseries)
        elif function == 'Min':
            value = "{{value: {}}}".format(min(list_timeseries))
        elif function == 'Max':
            value = "{{value: {}}}".format(max(list_timeseries))
        elif function == 'Average':
            value = "{{value: {}}}".format(sum(list_timeseries)/len(list_timeseries))
        else:
            value = "{{value: {}}}".format('NA')
        
        #logger.info(value)
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        optionsArrayPayload = "[{{ groupName: \"Value\", property: \"Timeseries\", value: {}}}, {{ groupName: \"Value\", property: \"Value\", value: {}}}]".format(string_timeseries, value)
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
            """.format(statistics_object_id, round(time.time() * 1000), optionsArrayPayload)
        )
        mutation_update_state_result = await client.execute_async(mutation_update_state)
        
        return 0
        
    except Exception as e:
        logger.error(e)   

@backoff.on_exception(backoff.expo, Exception)
async def subscribe_to_object_property(event):
    """ Property subscription for a given statistics
    
    Create a subscription that listen for changes on the property of an object for which we want to calculate a statistics.
    
    This is used to build and update the timeseries of the statistics object
    
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
        
        logger.info(event)
        
        statisticsObject = event
        
        if 'objectId' in event:
            logger.info("objectId")
            objectId = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Object'][0]
            propertyName = [x['value'] for x in event['object']['objectProperties'] if x['property'] == 'Property'][0]
        else:
            logger.info("no objectId")
            objectId = [x['value'] for x in event['objectProperties'] if x['property'] == 'Object'][0]
            propertyName = [x['value'] for x in event['objectProperties'] if x['property'] == 'Property'][0]
            
        logger.info(objectId)
        logger.info(propertyName)
        logger.info(propertyName.split("/")[0])
        logger.info(propertyName.split("/")[1])
            
        if objectId is not None and propertyName is not None:
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
                            id: ["{}"]
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
                        
                    """.format(objectId, propertyName.split("/")[0], propertyName.split("/")[1])
                )
                
                logger.info("Dispatcher subscribed on statistics objects events.")
                
                # Send event to be processed by the worker
                async for event in session.subscribe(subscription):
                    logger.info(event)
                    
                    # We don't process deletion of the alarms
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
                                            contains: ["application", "dispatcher", "statistics", "timeseries"]
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
                        
                        logger.info(existing_statistics_result)
                        
                        for stats_object in existing_statistics_result['objects']:
                            statisticsObject = stats_object
                        
                        loop.create_task(compute_statistics(event['Objects']['relatedNode'], statisticsObject))
                        
                    else:
                        logger.info("Statistics subscription closed.")
                        return 0
                        
            return 0
        
    except Exception as e:
        logger.error(e)   

@backoff.on_exception(backoff.expo, Exception)
async def statistics_subscription():
    """ Statistics objects subscription
    
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
                            contains: ["application", "dispatcher", "statistics", "timeseries"]
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
        
        logger.info(existing_statistics_result)
        
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
                        tags: ["application", "dispatcher", "statistics", "timeseries"]
                        propertyChanged: [{groupName: "Settings", property: "Function"}, {groupName: "Settings", property: "Object"}, {groupName: "Settings", property: "Period"}, {groupName: "Settings", property: "Property"}]
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
            
            logger.info("Dispatcher subscribed on statistics objects events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                logger.info(event)
                
                # We don't process deletion of the alarms
                if "delete" not in event['Objects']['event']:
                    loop.create_task(subscribe_to_object_property(event['Objects']['relatedNode']))
        
    except Exception as e:
        logger.error(e)    
