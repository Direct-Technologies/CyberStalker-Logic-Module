""" Resolve the alarms of the monitor object items

"""

import asyncio

from aiohttp.helpers import current_task
import backoff
import dateutil.parser
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
logger = logging.getLogger("alarms")

# Translating ui keys to python values for conditions operators
operators_dict = {'=': '==', '<': '<', '>': '>', '!=': '!=', 'contains': 'in'}

async def resolve_monitor_object_global_alarms(event):
    """
    """
    try:
        
        #logger.debug("GLOBAL ====> {}".format(event))
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
                
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)

        #logger.debug(event['object']['objectsToObjectsByObject2Id'][0]['object1']['objectsToObjectsByObject1Id'])
        items = event['object']['objectsToObjectsByObject2Id'][0]['object1']['objectsToObjectsByObject1Id']
        
        object_id = event['object']['objectsToObjectsByObject2Id'][0]['object1']['id']
        object_name = event['object']['objectsToObjectsByObject2Id'][0]['object1']['name']
        object_alarm_status = event['object']['objectsToObjectsByObject2Id'][0]['object1']['alarmStatus']
        
        #alarms = [any([a['activated'] == "true" for a in json.loads(i['object2']['alarms']) ]) for i in items]
        alarms = [i['object2']['alert'] == 'TRIGGERED' for i in items]
        alarms_off = [i['object2']['alert'] == 'OFF' for i in items]
    
        #logger.info("Alarms:")
        #logger.info(alarms)
        #logger.info("Alarms off")
        #logger.info(alarms_off)
    
        #logger.debug(alarms)
        if all(alarms_off) and object_alarm_status != "OFF":
            # Update object alarm status to true
            optionsArrayPayload = "[{ groupName: \"Statuses\", property: \"Alarm\", value: \"OFF\"}]"
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
                """.format(object_id, round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            logger.info("🟢 Updated {}/{} alarm status to OFF.".format(object_name, object_id))
            
            message = "{} dismissed".format(object_name)#.format(object_name, object_id)
            spec = r'''{\"type\": \"MONITOR_ALART\", \"alarm\": \"MONITOR_OBJECT\"}'''
            mutation_create_notification = gql(
                """
                mutation{{
                    createNotification(input: {{
                        notification: {{
                            subjectType: DATASET
                            subject: "{}"
                            subjectName: "{}"
                            tags: ["application", "monitor", "alert"]
                            message: "{}"
                            spec: "{}"
                            }}
                        }}){{
                                notification {{
                                    id
                                }}
                            }}
                        }}
                """.format(object_id, object_name, message, spec)
            )
            result_create_notification = await client.execute_async(mutation_create_notification)
            logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, object_name, object_id))
        elif any(alarms) == True and object_alarm_status != "TRIGGERED":
            # Update object alarm status to true
            optionsArrayPayload = "[{ groupName: \"Statuses\", property: \"Alarm\", value: \"TRIGGERED\"}]"
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
                """.format(object_id, round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            logger.info("🟢 Updated {}/{} alarm status to TRIGGERED.".format(object_name, object_id))
            
            message = "{} triggered".format(object_name)#.format(object_name, object_id)
            spec = r'''{\"type\": \"MONITOR_ALART\", \"alarm\": \"MONITOR_OBJECT\"}'''
            mutation_create_notification = gql(
                """
                mutation{{
                    createNotification(input: {{
                        notification: {{
                            subjectType: DATASET
                            subject: "{}"
                            subjectName: "{}"
                            tags: ["application", "monitor", "alert", "triggered"]
                            message: "{}"
                            spec: "{}"
                            }}
                        }}){{
                                notification {{
                                    id
                                }}
                            }}
                        }}
                """.format(object_id, object_name, message, spec)
            )
            result_create_notification = await client.execute_async(mutation_create_notification)
            logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, object_name, object_id))
        elif any(alarms) and object_alarm_status == "TRIGGERED":
            return 0
        elif all(alarms) == False and object_alarm_status != "ON":
            # Update object alarm status to false
            optionsArrayPayload = "[{ groupName: \"Statuses\", property: \"Alarm\", value: \"ON\"}]"
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
            """.format(object_id, round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            logger.info("🟢 Updated {}/{} alarm status to ON.".format(object_name, object_id))
            
            if object_alarm_status != "OFF":
                message = "{} dismissed".format(object_name)#.format(object_name, object_id)
                spec = r'''{\"type\": \"MONITOR_ALART\", \"alarm\": \"MONITOR_OBJECT\"}'''
                mutation_create_notification = gql(
                    """
                    mutation{{
                        createNotification(input: {{
                            notification: {{
                                subjectType: DATASET
                                subject: "{}"
                                subjectName: "{}"
                                tags: ["application", "monitor", "alert"]
                                message: "{}"
                                spec: "{}"
                                }}
                            }}){{
                                    notification {{
                                        id
                                    }}
                                }}
                            }}
                    """.format(object_id, object_name, message, spec)
                )
                result_create_notification = await client.execute_async(mutation_create_notification)
                logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, object_name, object_id))
        elif object_alarm_status != "ON":
            # Update object alarm status to false
            optionsArrayPayload = "[{ groupName: \"Statuses\", property: \"Alarm\", value: \"ON\"}]"
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
            """.format(object_id, round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            logger.info("🟢 Updated {}/{} alarm status to ON.".format(object_name, object_id))
            
            
    except Exception as e:
        logger.error("🔴 {}".format(e))

@backoff.on_exception(backoff.expo, Exception)
async def resolve_monitor_object_global_alarms_subscription():
    """
    """
    try:
        # Grab async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            
            # GQL subscription for ...
            subscription = gql(
                """
                subscription ResolveMonitorObjectItemsAlarms {
                    Objects(filterA: {
                        tags: ["application", "monitor"]
                        propertyChanged: [{groupName: "State", property: "Alert"}]
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
                                            name
                                            enabled
                                            value: property(propertyName: "State/Value")
                                            alarms: property(propertyName: "State/Alarms")
                                            alert: property(propertyName: "State/Alert")
                                            objectProperties(filter: {
                                                property: {
                                                equalTo: "Alarms"
                                                }
                                            }){
                                                id
                                                groupName
                                                property
                                                value
                                            }
                                            objectsToObjectsByObject2Id(filter: {
                                                object1: {
                                                    schemaTags: {
                                                    contains: ["application", "monitor"]
                                                    }
                                                }
                                                object2 : {
                                                    schemaTags : {
                                                    contains: ["application", "monitor"]
                                                    }
                                                }
                                            }){
                                                object1 {
                                                    id
                                                    name
                                                    enabled
                                                    schemaTags
                                                    alarmStatus: property(propertyName: "Statuses/Alarm")
                                                    objectsToObjectsByObject1Id(filter: {
                                                        object2 : {
                                                            schemaTags : {
                                                                overlaps: ["application", "monitor", "object monitoring item", "object geo item"]
                                                            }
                                                        }
                                                        }){
                                                        object2 {
                                                            id
                                                            name
                                                            enabled
                                                            schemaTags
                                                            alarms: property(propertyName: "State/Alarms")
                                                            alert: property(propertyName: "State/Alert")
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
            
            logger.info("Dispatcher subscribed on monitor objects alarms events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                #logger.debug(event)
                
                # We don't process deletion of the alarms
                if "delete" not in event['Objects']['event']:
                    loop.create_task(resolve_monitor_object_global_alarms(event['Objects']['relatedNode']))
        
    except Exception as e:
        logger.error(e)    
    
    return 0

