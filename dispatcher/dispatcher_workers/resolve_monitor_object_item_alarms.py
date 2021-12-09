""" Resolve the alarms of the monitor object items

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
logger = logging.getLogger("alarms")

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

async def resolve_monitor_object_item_alarms_old(event):
    """
    """
    try:
        
        logger.debug(event)
        
        # Sleep for the duration of the alarm timeout
        if event['alarm']['timeout']['units'] == 'seconds':
            time.sleep(event['alarm']['timeout']['value'])
        elif event['alarm']['timeout']['units'] == 'minutes':
            time.sleep(event['alarm']['timeout']['value'] * 60)
            
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
                
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        alarm = event['alarm']
        monitor_value = event['object']['value']
        status = event['alertstatus']
        uuid = event['id']
        
        #if 'activated' not in alarm:
        #        alarm['activated'] = "false"
            
        on_schedule = True
        if alarm['timeIntervalInMinutes']['from'] < alarm['timeIntervalInMinutes']['to']:
            on_schedule = True #updated_at_epoch = dateutil.parser.isoparse(updated_at).timestamp() 
        elif alarm['timeIntervalInMinutes']['from'] < alarm['timeIntervalInMinutes']['to']:
            on_schedule = True
        
        is_updated = False
        if on_schedule == True:    
            
            # Check conditions
            if monitor_value is None:
                logger.info("🟠 Skipped alert, no value provided on {}/{}.".format(event['name'], event['id']))
            
            if eval("{} {} {}".format(typecasting(monitor_value), operators_dict[alarm['condition']['operator']], typecasting(alarm['condition']['value']))) == True and status != "TRIGGERED":
                status = "TRIGGERED"
                is_updated = True
            elif eval("{} {} {}".format(typecasting(monitor_value), operators_dict[alarm['condition']['operator']], typecasting(alarm['condition']['value']))) == False and status == "TRIGGERED":
                status = "OFF"
                is_updated = True    
        
        if is_updated:
            # Format alarm for mutation
            # alarm = str(alarm).replace('\'', '\\\"')
            
            # mutation_update_alarm = gql(
            #     """
            #     mutation UpdateAlarmStatus {{
            #         updateJsonPropertyArray(input: {{
            #             propertyId: "{}"
            #             arrayElement: "{}"
            #             elementPosition: {}
            #         }}) {{
            #             boolean    
            #         }}
            #     }}
            #     """.format(event['alarmsid'], alarm, event['alarmindex'])
            # )
            # mutation_update_alarm_result = await client.execute_async(mutation_update_alarm)
            
            #logger.debug(mutation_update_alarm_result)
            
            optionsArrayPayload = "[{ \\\"groupname\\\": \\\"State\\\",\\\"property\\\":\\\"Alert\\\",\\\"value\\\": \\\""+ str(status) +"\\\"}]"
            mutation_update_state = gql(
                """
                mutation UpdateMonitorObjectState {{
                updateObjectPropertiesExtended(input: {{
                    objId: "{}"
                    transactionId: "{}"
                    optionsArray: "{}"
                    }}){{
                        boolean
                    }}
                }}
                """.format(uuid, round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            logger.info("🟢 Updated {}/{} alert status to {}.".format(event['object']['name'], event['object']['id'], status))
            
    except Exception as e:
        logger.error("🔴 {}".format(e))

async def resolve_monitor_object_item_alarms(event):
    """
    """
    try:
        
        logger.info(event)
        logger.info("Item name")
        logger.info(event['object']['info'])
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
                
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        if len(event['object']['alarms']) < 1 or event['object']['alarms'] == [{}]:
            logger.info("🟠 No alarms configured on {}/{}.".format(event['object']['name'], event['object']['id']))
            return 0
        
        alarms = event['object']['alarms']#json.loads(event['object']['alarms'])
        is_updated = False
        
        # Process non-linked event:
        if event['linkedPropertyId'] is None and event['property'] == 'Value' and event['value'] is not None:
            if event['object']['alert'] in ["ON", "TRIGGERED"]:
                if len(event['object']['alarms']) < 1:
                    # Set alarm to OFF
                    status = "OFF"
                    # Set property value to null
                    value = "null"
                    is_updated = True
                else:
                    # Set alarm to ON
                    status = "ON"
                    # Set property value to null
                    value = "null"
                    is_updated = True
            elif event['object']['alert'] == "OFF" and len(event['object']['alarms']) > 0:
                # Set alarm to ON
                status = "ON"
                # Set property value to null
                value = "null"
                is_updated = True
            
            if is_updated:
                optionsArrayPayload = "[{ groupName: \"State\", property: \"Alert\", value: \""+ str(status) +"\"}, { groupName: \"State\", property: \"Value\", value: "+ str(value) +"}]"
                mutation_update_state = gql(
                    """
                    mutation UpdateMonitorObjectStateOnUnlink {{
                    updateObjectPropertiesByName(input: {{
                        objectId: "{}"
                        transactionId: "{}"
                        propertiesArray: {}
                        }}){{
                            boolean
                        }}
                    }}
                    """.format(event['object']['id'], round(time.time() * 1000), optionsArrayPayload)
                )
                mutation_update_state_result = await client.execute_async(mutation_update_state)
                logger.info("🟢 Updated {}/{} alert status to {} on unlink property event.".format(event['object']['name'], event['object']['id'], status))
            return 0
        
        # Sort the alarms by duration of timeout
        alarms = sorted(alarms, key=lambda k: int(k['timeout'].get('value')) * (1 if k['timeout'].get('units') == 'seconds' else 60), reverse=False)
        delays = list(set([int(a['timeout']['value']) * (1 if a['timeout']['units'] == 'seconds' else 60) for a in alarms]))
        
        #logger.debug(alarms)
        
        #logger.debug(event['updatedAt'])
        #logger.debug(dateutil.parser.isoparse(event['updatedAt']).timestamp())
        
        now = datetime.datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes = ( (dateutil.parser.isoparse(event['updatedAt']).replace(tzinfo=None) - midnight).seconds % 3600 ) // 60
        
        if event['object']['alert'] == "OFF" or event['object']['alert'] == "ON":
            
            for idx, delay in enumerate(delays):
                time.sleep(delay - (0 if idx == 0 else delays[idx-1]))
                
                for alarm in [a for a in alarms if delay == int(a['timeout']['value']) * (1 if a['timeout']['units'] == 'seconds' else 60)]:
                    
                    try:
                        now = datetime.datetime.now()
                        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
                        minutes = ( (dateutil.parser.isoparse(event['updatedAt']).replace(tzinfo=None) - midnight).seconds % 3600 ) // 60
                        on_schedule = True
                        if alarm['timeIntervalInMinutes']['from'] < alarm['timeIntervalInMinutes']['to']:
                            if minutes > alarm['timeIntervalInMinutes']['from'] and minutes < alarm['timeIntervalInMinutes']['to']:
                                on_schedule = True 
                            else:
                                on_schedule = False
                        elif alarm['timeIntervalInMinutes']['from'] > alarm['timeIntervalInMinutes']['to']:
                            if minutes > alarm['timeIntervalInMinutes']['from'] and minutes < 1440:
                                on_schedule = True
                            elif minutes > 0 and minutes < alarm['timeIntervalInMinutes']['to']:
                                on_schedule = True 
                            else:
                                on_schedule = False
                    except Exception as e:
                        logger.info("🟠 Malformed timestamp for alert on {}/{}.".format(event['object']['name'], event['object']['id']))
                        on_schedule = True
                    
                    is_updated = False
                    if on_schedule == True:    
                        
                        # Check conditions
                        try:
                            if event['object']['value'] is None:
                                logger.info("🟠 Skipped alert, no value provided on {}/{}.".format(event['object']['name'], event['object']['id']))
                                if event['object']['alert'] == "OFF" and idx == len(delays) - 1:
                                    status = "ON"
                                    is_updated = True
                            elif eval("{} {} {}".format(typecasting(event['object']['value']), operators_dict[alarm['condition']['operator']], typecasting(alarm['condition']['value']))) == True:
                                alarm_condition = "{} {} {}".format(event['object']['value'], operators_dict[alarm['condition']['operator']], typecasting(alarm['condition']['value']))
                                status = "TRIGGERED"
                                is_updated = True    
                            elif event['object']['alert'] == "OFF" and idx == len(delays) - 1:
                                status = "ON"
                                is_updated = True
                        except Exception as e:
                            logger.info("🟠 Malformed condition, skipped alert on {}/{}.".format(event['object']['name'], event['object']['id']))
                            
                    if is_updated:
                        break   
                
                if is_updated:
                    break       
            
        elif event['object']['alert'] == "TRIGGERED":
            
            # Immediately evaluate all the conditions without delay or restrictions
            try:
                alarms_states = [eval("{} {} {}".format(typecasting(event['object']['value']), operators_dict[a['condition']['operator']], typecasting(a['condition']['value']))) for a in alarms]
            except Exception as e:
                logger.info("🟠 Failed to update {}/{} alert status. Check conditions format.".format(event['object']['name'], event['object']['id']))
                return 0
            
            # Turn off the alert if all the conditions are turned off
            if not any(alarms_states):
                #logger.info("🟢 No condition met for {}/{} turn alert ON.".format(event['object']['name'], event['object']['id']))
                is_updated = True
                status = "ON"
        
        if is_updated: 
            # Fetch again credentials to make sure they are up to date after timeout
            # GQL parameters
            gql_address = config['gql']['address']
            jwt_env_key = "JWT_ENV_DISPATCHER"
            jwt = os.environ[jwt_env_key]
                    
            # GQL client
            transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            
            optionsArrayPayload = "[{ groupName: \"State\", property: \"Alert\", value: \""+ str(status) +"\"}]"
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
                """.format(event['object']['id'], round(time.time() * 1000), optionsArrayPayload)
            )
            mutation_update_state_result = await client.execute_async(mutation_update_state)
            logger.info("🟢 Updated {}/{} alert status to {}.".format(event['object']['name'], event['object']['id'], status))
            
            if status == "TRIGGERED":
                message = "{}, {} ({})".format(event['object']['objectsToObjectsByObject2Id'][0]['object1']['name'], event['object']['info'], "{} {} {}".format(event['object']['value'], operators_dict[alarm['condition']['operator']], alarm['condition']['value']))#.format(event['object']['name'], event['object']['id'])
                spec = r'''{\"type\": \"MONITOR_ALERT\", \"alarm\": \"MONITOR_ITEM\"}'''
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
                    """.format(event['object']['objectsToObjectsByObject2Id'][0]['object1']['id'], event['object']['objectsToObjectsByObject2Id'][0]['object1']['name'], message, spec)#format(event['object']['id'], event['object']['name'], message, spec)
                )
                result_create_notification = await client.execute_async(mutation_create_notification)
                logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, event['object']['name'], event['object']['id']))
            elif event['object']['alert'] != "OFF":
                message = "{}, {} (current value {})".format(event['object']['objectsToObjectsByObject2Id'][0]['object1']['name'], event['object']['info'], event['object']['value'])
                spec = r'''{\"type\": \"MONITOR_ALERT\", \"alarm\": \"MONITOR_ITEM\"}'''
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
                    """.format(event['object']['objectsToObjectsByObject2Id'][0]['object1']['id'], event['object']['objectsToObjectsByObject2Id'][0]['object1']['name'], message, spec)#format(event['object']['id'], event['object']['name'], message, spec)
                )
                result_create_notification = await client.execute_async(mutation_create_notification)
                logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, event['object']['name'], event['object']['id']))
            
    except Exception as e:
        logger.error("🔴 {}".format(e))

@backoff.on_exception(backoff.expo, Exception)
async def resolve_monitor_object_item_alarms_subscription():
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
                        tags: ["application", "monitor", "object monitoring item"]
                        propertyChanged: [{groupName: "State", property: "Value"}, {groupName: "State", property: "Alarms"}]
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
                                        linkedPropertyId
                                        object {
                                            id
                                            name
                                            enabled
                                            value: property(propertyName: "State/Value")
                                            alarms: property(propertyName: "State/Alarms")
                                            alert: property(propertyName: "State/Alert")
                                            info: property(propertyName: "Info/Name")
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
                                                                contains: ["application", "monitor"]
                                                            }
                                                        }
                                                        }){
                                                        object2 {
                                                            id
                                                            name
                                                            enabled
                                                            schemaTags
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
            
            logger.info("Dispatcher subscribed on monitor objects items events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                #logger.debug(event)
                
                # Don't process deletion of monitor items
                if "delete" not in event['Objects']['event']:
                    # Find all the alarms tasks running for this monitor item and reset them if a new message is received
                    task_name = "resolve_monitor_object_item_alarms_{}".format(event['Objects']['relatedNode']['object']['id'])
                    tasks = [task for task in asyncio.all_tasks() if task.get_name() == task_name]
                    # If the tasks are already running cancel them
                    if len(tasks) > 0:
                        tasks[0].cancel()
                    
                    loop.create_task(resolve_monitor_object_item_alarms(event['Objects']['relatedNode']), name=task_name)
                    
    except Exception as e:
        logger.error(e)    
    
    return 0

