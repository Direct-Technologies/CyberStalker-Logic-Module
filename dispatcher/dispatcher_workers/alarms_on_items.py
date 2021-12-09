import logging
import logging.config 
import yaml
from envyaml import EnvYAML
import asyncio
import smtplib
import pandas as pd
from email.message import EmailMessage
import json
import time
from datetime import datetime
import os
import backoff
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("dispatcher")

@backoff.on_exception(backoff.expo, Exception)
async def alarms_on_items_job():
    """

        62b81b5e-459b-47a5-9a97-b14304d87a16
        
    """
    try:
        # On startup of the job, get the parameters from the dispatcher's profile
        jwt_dispatcher = os.environ["JWT_ENV_DISPATCHER"]
        transport_dispatcher = AIOHTTPTransport(url="http://{}/graphql".format(config["gql"]["address"]), headers={'Authorization': 'Bearer ' + jwt_dispatcher})
        client_dispatcher = Client(transport=transport_dispatcher, fetch_schema_from_transport=False)
        
        query_uuid = gql(
            """
            query GetDispatcherObjectId {
                getUserProfileId
            }
            """
        )
        query_uuid_result = await client_dispatcher.execute_async(query_uuid)
        
        if query_uuid_result['getUserProfileId'] == None:
            logger.error("Dispatcher object not found")
            
        # Get the profile object
        query_profile = gql(
            """
            query GetDispatcherProfile {{
                                        objectProperties(filter: {{
                                            objectId: {{equalTo: "{}"}}
                                            groupName: {{equalTo: "dispatcher"}}
                                            property: {{equalTo: "alarms_on_items_job"}}
                                        }}){{
                                            groupName
                                            property
                                            value
                                            meta
                                        }}
                                    }}
            """.format(query_uuid_result['getUserProfileId'])
        )
        profile = await client_dispatcher.execute_async(query_profile)
        
        #logger.debug(profile)
        
        params = json.loads(profile['objectProperties'][0]['meta'])
        
        # Default parameter, run the job once per minute
        board_timeout = 60
        # If another valid timeout is provided use it instead
        if type(params['timer']) == int:
            if params['timer'] >= 30:
                board_timeout = params['timer']
        
    except Exception as e:
        logger.error("🟠 Backoff, restarting alarms_on_items_job on error, {}".format(e))
    
    # Exit if the job is tagged as inactive
    if profile['objectProperties'][0]['value'] == False:
        logger.info("This task won't be running. Change 'active' parameter to 'true' to launch the task.")
        return 0
    
    logger.info("Starting Task: {} | Params: {}".format("alarms_on_items_job", params))
    
    while True:
        await asyncio.sleep(board_timeout)
        try:
            jwt = os.environ["JWT_ENV_BOARD"]
            transport = AIOHTTPTransport(url="http://{}/graphql".format(config["gql"]["address"]), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            # Get all the widget counter objects
            find_objects_query = gql(
                """
                query FindItemsObject {
                    objects(filter: {
                        schemaId: {
                            equalTo: "62b81b5e-459b-47a5-9a97-b14304d87a16"
                        }
                    }){
                        id
                        name
                        enabled
                        description
                        createdAt
                        updatedAt
                        by
                        schemaId
                        tags
                        favourite
                        muted
                        schemaName
                        schemaTags
                        schemaType
                        objectProperties {
                            id
                            groupName
                            property
                            value
                            updatedAt
                            meta
                        }
                    }
                }
                """
            )
            find_objects_result = await client.execute_async(find_objects_query)
            
            #logger.debug(find_objects_result)
            
            # TODO: Refactor this mess
            # Check the alarm conditions on each of the widgets
            for o in find_objects_result['objects']:
                logger.debug(o)
                uuid = o['id']
                op = pd.DataFrame(o['objectProperties'])
                logger.debug(op)
                for i, item in op[op['groupName'] == 'Items'].iterrows():
                    logger.debug(item)
                    logger.debug(item['meta']['alarms'])
                    
                #logger.debug( wp[wp['groupName'] == 'Counter'][['meta']] )
                
                #logger.debug( json.loads(wp[wp['groupName'] == 'Counter'][['meta']].values[0][0]) )
                
                #logger.debug(wp)
                
                is_alarmed = json.loads(wp[wp['groupName'] == 'Counter'][['meta']].values[0][0])['alarmed']
                
                # Loop over the alarms 
                #logger.debug(wp[wp['groupName'] == 'Alarm'])
                a_status_list = []
                n_status_list = []
                for i, a in wp[wp['groupName'] == 'Alarm'].iterrows():
                    a_updated_at = a['updatedAt']
                    a_status = a['value']
                    n_status = a_status
                    a_meta = json.loads(a['meta'])
                    #logger.debug(a_meta)
                    #logger.debug(a_updated_at)
                    #
                    if a_meta['condition'] == '=':
                            if int(count) == int(a_meta['trigger']):
                                if a_status == 'PENDING':
                                    if time.time() - datetime.fromisoformat(a_updated_at).timestamp() > int(a_meta['allowedtime']):
                                        n_status = 'ON'
                                elif a_status == 'ON':
                                    n_status = 'ON'
                                else:
                                    n_status = 'PENDING'
                            else:
                                n_status = 'OFF'
                    elif a_meta['condition'] == '<':
                            if int(count) < int(a_meta['trigger']):
                                if a_status == 'PENDING':
                                    if time.time() - datetime.fromisoformat(a_updated_at).timestamp() > int(a_meta['allowedtime']):
                                        n_status = 'ON'
                                elif a_status == 'ON':
                                    n_status = 'ON'
                                else:
                                    n_status = 'PENDING'
                            else:
                                n_status = 'OFF'
                    elif a_meta['condition'] == '<=':
                            if int(count) <= int(a_meta['trigger']):
                                if a_status == 'PENDING':
                                    if time.time() - datetime.fromisoformat(a_updated_at).timestamp() > int(a_meta['allowedtime']):
                                        n_status = 'ON'
                                elif a_status == 'ON':
                                    n_status = 'ON'
                                else:
                                    n_status = 'PENDING'
                            else:
                                n_status = 'OFF'
                    elif a_meta['condition'] == '>':
                            if int(count) > int(a_meta['trigger']):
                                if a_status == 'PENDING':
                                    if time.time() - datetime.fromisoformat(a_updated_at).timestamp() > int(a_meta['allowedtime']):
                                        n_status = 'ON'
                                elif a_status == 'ON':
                                    n_status = 'ON'
                                else:
                                    n_status = 'PENDING'
                            else:
                                n_status = 'OFF'
                    elif a_meta['condition'] == '>=':
                            if int(count) >= int(a_meta['trigger']):
                                if a_status == 'PENDING':
                                    if time.time() - datetime.fromisoformat(a_updated_at).timestamp() > int(a_meta['allowedtime']):
                                        n_status = 'ON'
                                elif a_status == 'ON':
                                    n_status = 'ON'
                                else:
                                    n_status = 'PENDING'
                            else:
                                n_status = 'OFF'
                    a_status_list.append(a_status)
                    n_status_list.append(n_status)
                
                #logger.debug(a_status_list)
                #logger.debug(n_status_list)
                
                # Decide on global alarm status of the widget
                alarm_status = is_alarmed
                new_alarm_status = alarm_status
                if ('ON' in n_status_list) and is_alarmed != True:
                    new_alarm_status = True # Need to write to db
                elif ('ON' not in n_status_list) and is_alarmed == True:
                    new_alarm_status = False # Need to write to db
                # Update widget alarms
                #optionsArrayPayload = "[{ \\\"groupname\\\": \\\"Alarm\\\",\\\"property\\\":\\\"ALARM_STATUS\\\",\\\"value\\\": \\\"" +  + "\\\"}]"
                #logger.debug(alarm_status)
                #logger.debug(new_alarm_status)
                # Build the payload to update only what has changed
                if new_alarm_status == alarm_status and (a_status_list == n_status_list):
                    continue
                else:
                    optionsArrayPayloadCounter = ""
                    optionsArrayPayloadAlarms = ""
                    if new_alarm_status != alarm_status:
                        optionsArrayPayloadCounter = optionsArrayPayloadCounter + ",{ \\\"groupname\\\": \\\"Counter\\\",\\\"property\\\":\\\"COUNT\\\", \\\"value\\\": \\\"" + str(count) + "\\\", \\\"meta\\\": { \\\"alarmed\\\": " + str(new_alarm_status) + " } }"
                    
                        optionsArrayPayloadCounter = optionsArrayPayloadCounter[1:]
                        
                        update_widget_alarm_status_mutation = gql(
                            """
                            mutation UpdateWidgetAlarmStatus {{
                            updateObjectPropertiesExtended(input: {{
                                objId: "{}"
                                transactionId: "{}"
                                optionsArray: "[{}]"
                                }}){{
                                    boolean
                                }}
                            }}
                            """.format(uuid, round(time.time() * 1000), optionsArrayPayloadCounter)
                        )
                        update_widget_alarm_status_result = await client.execute_async(update_widget_alarm_status_mutation)
                        
                        #logger.debug(update_widget_alarm_status_result)
                    
                    for i in range(len(a_status_list)):
                        if a_status_list[i] != n_status_list[i]:
                            optionsArrayPayloadAlarms = optionsArrayPayloadAlarms + ",{ \\\"groupname\\\": \\\"Alarm\\\",\\\"property\\\":\\\"ALARM_1\\\",\\\"value\\\": \\\"" + str(n_status_list[i]) + "\\\"}"
                    
                    # if a_status_list[0] != n_status_list[0]:
                    #     optionsArrayPayload = optionsArrayPayload + ",{ \\\"groupname\\\": \\\"Alarm\\\",\\\"property\\\":\\\"ALARM_1\\\",\\\"value\\\": \\\"" + str(n_status_list[0]) + "\\\"}"
                    # if a_status_list[1] != n_status_list[1]:
                    #     optionsArrayPayload = optionsArrayPayload + ",{ \\\"groupname\\\": \\\"Alarm\\\",\\\"property\\\":\\\"ALARM_2\\\",\\\"value\\\": \\\"" + str(n_status_list[1]) + "\\\"}"
                    # if a_status_list[2] != n_status_list[2]:
                    #     optionsArrayPayload = optionsArrayPayload + ",{ \\\"groupname\\\": \\\"Alarm\\\",\\\"property\\\":\\\"ALARM_3\\\",\\\"value\\\": \\\"" + str(n_status_list[2]) + "\\\"}"
                    
                    optionsArrayPayloadAlarms = optionsArrayPayloadAlarms[1:] 
                    
                    update_widget_mutation = gql(
                        """
                        mutation UpdateWidgetAlarms {{
                        updateObjectProperties(input: {{
                            objId: "{}"
                            transactionId: "{}"
                            optionsArray: "[{}]"
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(uuid, round(time.time() * 1000), optionsArrayPayloadAlarms)
                    )
                    update_widget_result = await client.execute_async(update_widget_mutation)
                    
                    #logger.debug(update_widget_result)
                    
                    # Create a notification when the widget alarms gets turned on
                    if new_alarm_status != alarm_status and new_alarm_status == True:
                        spec = r'''{\"type\": \"alert\", \"alarm\": \"ON\", \"property\": \"UNKNOWN\"}'''
                        message = "Counter alarm"
                        alarm_notification_mutation = gql(
                            """
                            mutation{{
                                createNotification(input: {{
                                    notification: {{
                                        actorType: "{}"
                                        actor: "{}"
                                        actorName: "{}"
                                        tags: ["alert", "board"]
                                        message: "{}"
                                        spec: "{}"
                                        }}
                                    }}){{
                                            notification {{
                                                id
                                            }}
                                        }}
                                    }}
                            """.format(w['schemaType'], w['id'], w['name'], message, spec)
                        )
                        alarm_notification_result = await client.execute_async(alarm_notification_mutation)
                        logger.debug("Widget alarm notification created: {}".format(alarm_notification_result))
        except Exception as e:
            logger.error("🟠 Backoff, restarting alarms_on_items_job on error, {}".format(e))
            continue