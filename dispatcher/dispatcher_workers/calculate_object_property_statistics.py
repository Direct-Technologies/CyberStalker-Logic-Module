""" Calculate object property statistics

"""

import asyncio
import backoff
from datetime import datetime
import dateutil.parser
from envyaml import EnvYAML
from email.message import EmailMessage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import json
import logging
import logging.config 
import os
import pandas as pd
import smtplib
import time

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("board")

async def calculate_object_property_statistics_worker(event):
    """Worker updating all the counter widgets

        Specific to the board application. Powers the Board Widget Counter objects described by the
        schema id 2a675e27-6394-4669-9978-c0c9546d1057.
        
        The worker is to be spawned everytime an event is received through the objects subscription 
        channel.
        
        Once the worker is spawned it proceeds with the following logic:
        
        0. It checks that the event is a property update. If the event is anything else the worker
        stops there.
        
        1. The worker makes a GQL query that returns all the Board Widget Counter objects.
        
        2. It then examines all the Board Widget Counter objects to find which ones are connected to
        the property update received in the triggering event.
        
        3. If there is at least one Board Widget counter object connected to the triggering event then
        the worker makes a GQL query to get all the objects sharing the same schema id as the object 
        referenced in the triggering event. 
        
        4. For each Board Widget Counter the worker takes the list of objects obtained in (3) and performs
        the relevant aggregation to compute the new value of the Board Widget Counter. The new value is
        used to run an update on the Board Widget Counter.
        
        Parameters
        ----------
     
        raw_event : str
            Event received from a subscription.
    """
    
    #logger.debug(event)
    
    event = raw_event['Objects']['relatedNode']
    
    try:
    
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_BOARD"
        jwt = os.environ[jwt_env_key]
        
        # Check that we have received a property event. Otherwise terminate the worker.
        if 'property' not in event:
            return 0
        # Setup GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        # Find all the board counter widgets
        find_widgets_query = gql(
            """
            query FindWidgets1 {
                objects(filter: {
                    schemaId: {
                        equalTo: "2a675e27-6394-4669-9978-c0c9546d1057"
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
                    }
                    sourceId: property(propertyName: "Source/SCHEMA_ID")
                    sourceProperty: property(propertyName: "Source/PROPERTY")
                    filterCondition: property(propertyName: "Filter/CONDITION")
                    filterTriggerValue: property(propertyName: "Filter/TRIGGER_VALUE")
                }
            }
            """
        )
        find_widgets_result = await client.execute_async(find_widgets_query)
        
        #logger.debug(find_widgets_result)
        
        # Check Source/SCHEMA_ID and Source/PROPERTY to see if they match the event
        related_widgets = [w for w in find_widgets_result['objects'] if (w['sourceId'] == event['object']['schemaId'] and w['sourceProperty'] == event['property'])]
                
        #logger.debug(related_widgets)
                
        # If there is any widget to be updated gather the object properties 
        if len(related_widgets) > 0:
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
                """.format(event['object']['schemaId'], event['groupName'], event['property'])
            )
            find_properties_result = await client.execute_async(find_properties_query)
            
            #logger.debug(find_properties_result)
            
            # Convert to data frame for easy data manipulation
            properties_df = pd.DataFrame(find_properties_result['objects'])
            
            #logger.debug(properties_df)
            
            # Update each of the related workers
            for w in related_widgets:
                # Set the new counter value
                if w['filterCondition'] == '=':
                    counter = len(properties_df.loc[properties_df['value'] == w['filterTriggerValue']].index)
                elif w['filterCondition'] == '<=':
                    counter = len(properties_df.loc[properties_df['value'] <= w['filterTriggerValue']].index)
                elif w['filterCondition'] == '<':
                    counter = len(properties_df.loc[properties_df['value'] < w['filterTriggerValue']].index)
                elif w['filterCondition'] == '>=':
                    counter = len(properties_df.loc[properties_df['value'] >= w['filterTriggerValue']].index)
                elif w['filterCondition'] == '>':
                    counter = len(properties_df.loc[properties_df['value'] > w['filterTriggerValue']].index)
                else:
                    counter = "NA"
                #logger.debug(counter)
                # Update the counter value
                optionsArrayPayload = "[{ \\\"groupname\\\": \\\"Counter\\\",\\\"property\\\":\\\"COUNT\\\",\\\"value\\\": \\\"" + str(counter) + "\\\"}]"
                update_counter_mutation = gql(
                    """
                    mutation UpdateMediaObject {{
                    updateObjectProperties(input: {{
                        objId: "{}"
                        transactionId: "{}"
                        transactionSize: 1
                        optionsArray: "{}"
                        }}){{
                            boolean
                        }}
                    }}
                    """.format(w['id'], round(time.time() * 1000), optionsArrayPayload)
                )
                update_counter_result = await client.execute_async(update_counter_mutation)

                #logger.debug(update_counter_result)
                
    except Exception as e:
        logger.error(e)

@backoff.on_exception(backoff.expo, Exception)
async def calculate_object_property_statistics_subscription(property_statistics_object_id):
    """Calculate object property statistics subscription
    
        ...
        
        Parameters
        ----------
        property_statistics_object_id : str
            Uuid of the property statistics object.
    """
    try:
        # GQL connection
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        # Logic of subscription filter creation
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        find_property_statistics_object_query = gql(
            """
            query FindPropertyStatisticsObjectQuery {{
                objects(filter: {{
                    objectId: {{
                        equalTo: "{}"
                    }}
                }}) {{
                    id
                    name
                    schemaTags
                    enabled
                    objectProperties {{
                        id
                        objectId
                        groupName
                        property
                        value
                        type
                        updatedAt
                    }}
                }}
            }}
            """.format(property_statistics_object_id)
        )
        find_property_statistics_object_result = await client.execute_async(find_property_statistics_object_query)
        
        logger.debug(find_property_statistics_object_result)
        
        return 0
        # Find all the objects to be used for statistics calculation
        find_widgets_query = gql(
            """
            query FindWidgets1 {
                objects(filter: {
                    schemaId: {
                        equalTo: "2a675e27-6394-4669-9978-c0c9546d1057"
                    }
                }){
                    sourceId: property(propertyName: "Source/SCHEMA_ID")
                    sourceProperty: property(propertyName: "Source/PROPERTY")
                }
            }
            """
        )
        find_widgets_result = await client.execute_async(find_widgets_query)
        
        schema_ids = '[{}]'.format(','.join(['"{}"'.format(w['sourceId']) for w in find_widgets_result['objects']]))
        property_names = [w['sourceProperty'] for w in find_widgets_result['objects']]
        
        find_objects_query = gql(
            """
            query FindObjectsIds {{
                objects(filter: {{
                    schemaId: {{
                        in: {}
                    }}
                }}){{
                    id
                }}
            }}
            """.format(schema_ids)
        )
        find_objects_result = await client.execute_async(find_objects_query)
        # Build the filters for the targetted subscription    
        id_filter = '[{}]'.format(",".join(['"{}"'.format(o['id']) for o in find_objects_result['objects']]))
        property_changed_filter = '[{}]'.format(",".join([ '{{groupName: null, property: "{}" }}'.format(str(p)) for p in property_names]))
        #
        loop = asyncio.get_event_loop()
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            subscription = gql(
                """
                subscription WidgetCounterSub {{
                    Objects(filterA: {{
                        id: {}
                        propertyChanged: {}
                    }}){{
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
                                        meta
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
                """.format(id_filter, property_changed_filter)
            )
            async for event in session.subscribe(subscription):
                logger.debug(event)
                loop.create_task(widget_counter_worker(event))
    except Exception as e:
        logger.error(e)
            

    """Periodic job updating all the counter widgets

        Specific to the board application. Powers the Board Widget Counter objects described by the
        schema id 2a675e27-6394-4669-9978-c0c9546d1057.
        
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
                                            groupName: {{equalTo: "board"}}
                                            property: {{equalTo: "widget_counter_job"}}
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
        logger.error("🟠 Backoff, restarting widget_counter_job on error, {}".format(e))
    
    # Exit if the job is tagged as inactive
    if profile['objectProperties'][0]['value'] == False:
        logger.info("This task won't be running. Change 'active' parameter to 'true' to launch the task.")
        return 0
    
    logger.info("Starting Task: {} | Params: {}".format("widget_counter_job", params))
    
    while True:
        await asyncio.sleep(board_timeout)
        try:
            jwt = os.environ["JWT_ENV_BOARD"]
            transport = AIOHTTPTransport(url="http://{}/graphql".format(config["gql"]["address"]), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            # Get all the widget counter objects
            find_widgets_query = gql(
                """
                query FindWidgets2 {
                    objects(filter: {
                        schemaId: {
                            equalTo: "2a675e27-6394-4669-9978-c0c9546d1057"
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
                        count: property(propertyName: "Counter/COUNT")
                        sourceId: property(propertyName: "Source/SCHEMA_ID")
                        sourceProperty: property(propertyName: "Source/PROPERTY")
                        filterCondition: property(propertyName: "Filter/CONDITION")
                        filterTriggerValue: property(propertyName: "Filter/TRIGGER_VALUE")
                        alarm1: property(propertyName: "Alarm/ALARM_1")
                        alarm2: property(propertyName: "Alarm/ALARM_2")
                        alarm3: property(propertyName: "Alarm/ALARM_3")
                    }
                }
                """
            )
            find_widgets_result = await client.execute_async(find_widgets_query)
            
            #logger.debug(find_widgets_result)
            # TODO: Refactor this mess
            # Check the alarm conditions on each of the widgets
            for w in find_widgets_result['objects']:
                #logger.debug(w)
                
                uuid, count = w['id'], w['count']
                
                wp = pd.DataFrame(w['objectProperties'])
                
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
                                    if time.time() - dateutil.parser.isoparse(a_updated_at).timestamp() > int(a_meta['allowedtime']):
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
                                    if time.time() - dateutil.parser.isoparse(a_updated_at).timestamp() > int(a_meta['allowedtime']):
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
                                    if time.time() - dateutil.parser.isoparse(a_updated_at).timestamp() > int(a_meta['allowedtime']):
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
                                    if time.time() - dateutil.parser.isoparse(a_updated_at).timestamp() > int(a_meta['allowedtime']):
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
                                    if time.time() - dateutil.parser.isoparse(a_updated_at).timestamp() > int(a_meta['allowedtime']):
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
                                transactionSize: 1
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
                            transactionSize: 1
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
            logger.error("🟠 Backoff, restarting widget_counter_job on error, {}".format(e))
            continue