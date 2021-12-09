""" Resolve the geo alarms of the monitor object items

"""
import asyncio
import backoff
import dateutil.parser
import datetime
from envyaml import EnvYAML
import geojson
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import logging
import logging.config 
import os
from pyproj import Transformer
import time
from turfpy import measurement

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("alarms")

# Translating ui keys to python values for conditions operators
operators_dict = {'=': '==', '<': '<', '>': '>', '!=': '!=', 'contains': 'in'}

async def resolve_monitor_object_item_geo_alarms(event):
    """
    """
    try:
        
        #logger.debug(event)
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
                
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        position = geojson.Point((event['value']['lat'], event['value']['lon']))
        geo_items = event['object']['objectsToObjectsByObject1Id']
        
        for geo_item in geo_items:
            is_updated = False
            geo_alarms = geo_item['object2']['alarms']
            geo_alert = geo_item['object2']['alert']
            geo_source = geo_item['object2']['sourceId']
            #logger.info("Geo item {} | {} | {}".format(geo_alarms, geo_source, geo_alert))
            
            if geo_source is None or geo_source == "":
                continue
            
            query_geo_source = gql(
                """
                query GetGeoSource {{
                object(id: "{}") {{
                  id
                  name
                  enabled
                  schemaTags
                  points: property(propertyName: "Position/Points")
                  floor: property(propertyName: "Position/Floor")
                  onlyOneFloor: property(propertyName: "Info/Only one floor")
                  positionCenter: property(propertyName: "Position/Center")
                  positionRadius: property(propertyName: "Position/Radius")
                  
                }}
                }}
                """.format(geo_source)
            )
            geo_source_result = await client.execute_async(query_geo_source)
            
            #logger.info(geo_source_result)
            
            if geo_source_result['object']['points'] is not None:
                geo_type = "ZONE"
                geo_polygon = {"coordinates": geo_source_result['object']['points'], "type": "Polygon"}
                tcoordinates = [[]]
                transformer = Transformer.from_crs("epsg:3857", "epsg:4326")
                for lon, lat in geo_source_result['object']['points'][0]:
                    tlon, tlat = transformer.transform(lon, lat)
                    tcoordinates[0].append([tlon, tlat])
                t_geo_polygon = {"coordinates": tcoordinates, "type": "Polygon"}
                transformer = Transformer.from_crs("epsg:4326", "epsg:3857")
                is_point_inside = measurement.boolean_point_in_polygon(position, t_geo_polygon)
            elif geo_source_result['object']['positionCenter'] is not None:
                geo_type = "LANDMARK"
                geo_point = {"coordinates": [geo_source_result['object']['positionCenter']['lat'], geo_source_result['object']['positionCenter']['lon']], "type": "Point"}
                distance_from_the_center = measurement.distance(geojson.Feature(geometry=geojson.Point((event['value']['lat'], event['value']['lon']))), geojson.Feature(geometry=geojson.Point((geo_source_result['object']['positionCenter']['lat'], geo_source_result['object']['positionCenter']['lon']))), units="m")
            else:
                #logger.info("Geo type: UKNOWN")
                continue
            
            if len(geo_alarms) < 1 or geo_alarms == [{}]:
                logger.info("🟠 No alarms configured on {}/{}.".format(geo_item['object2']['name'], geo_item['object2']['id']))
                continue
            
            # Sort the alarms by duration of timeout
            alarms = sorted(geo_alarms, key=lambda k: int(k['timeout'].get('value')) * (1 if k['timeout'].get('units') == 'seconds' else 60), reverse=False)
            delays = list(set([int(a['timeout']['value']) * (1 if a['timeout']['units'] == 'seconds' else 60) for a in alarms]))
            
            now = datetime.datetime.now()
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            minutes = ( (dateutil.parser.isoparse(event['updatedAt']).replace(tzinfo=None) - midnight).seconds % 3600 ) // 60
            
            if geo_alert == "OFF" or geo_alert == "ON":
                
                for idx, delay in enumerate(delays):
                    time.sleep(delay - (0 if idx == 0 else delays[idx-1]))
                    
                    for alarm in [a for a in alarms if delay == int(a['timeout']['value']) * (1 if a['timeout']['units'] == 'seconds' else 60)]:
                        
                        #logger.info(alarm)
                        
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
                            
                            #logger.info("PARAMETERS =====> \n geo_alert: {} \n geo_type: {} \n alarm_conditions_type: {} \n object_radius: {} \n condition_operator: {} \n condition_value: {}".format(geo_alert, geo_type, alarm['condition']['type'], geo_source_result['object']['positionRadius'],  alarm['condition']['operator'],  alarm['condition']['value']))
                            # Check the alarm conditions
                            try:
                                if event['value'] is None:
                                    logger.info("🟠 Skipped alert, no value provided on {}/{}.".format(event['object']['name'], event['object']['id']))
                                    if geo_alert == "OFF" and idx == len(delays) - 1:
                                        status = "ON"
                                        is_updated = True
                                # elif eval("{} {} {}".format(event['value'], operators_dict[alarm['condition']['operator']], alarm['condition']['value'])) == True:
                                #     status = "TRIGGERED"
                                #     is_updated = True    
                                elif geo_type == "LANDMARK" and alarm['condition']['type'] == "position":
                                    #logger.info("LANDMARK INSIDE =======>")
                                    if distance_from_the_center > geo_source_result['object']['positionRadius'] and alarm['condition']['value'] == False and geo_alert != "TRIGGERED":
                                        status = "TRIGGERED"
                                        is_updated = True 
                                    elif distance_from_the_center < geo_source_result['object']['positionRadius'] and alarm['condition']['value'] == True and geo_alert != "TRIGGERED":
                                        status = "TRIGGERED"
                                        is_updated = True 
                                    elif geo_alert != "ON":
                                        status = "ON"
                                        is_updated = True
                                elif geo_type == "LANDMARK" and alarm['condition']['type'] == "distance":
                                    #logger.info("LANDMARK DISTANCE =======>")
                                    if eval("{} {} {}".format(distance_from_the_center, alarm['condition']['operator'], alarm['condition']['value'])) and geo_alert != "TRIGGERED":
                                        status = "TRIGGERED"
                                        is_updated = True 
                                    elif geo_alert != "ON":
                                        status = "ON"
                                        is_updated = True
                                elif geo_type == "ZONE" and alarm['condition']['type'] == "position":
                                    #logger.info("ZONE INSIDE =======>")
                                    if is_point_inside == False and alarm['condition']['value'] == False and geo_alert != "TRIGGERED":
                                        status = "TRIGGERED"
                                        is_updated = True 
                                    elif is_point_inside == True and alarm['condition']['value'] == True and geo_alert != "TRIGGERED":
                                        status = "TRIGGERED"
                                        is_updated = True 
                                    elif geo_alert != "ON":
                                        status = "ON"
                                        is_updated = True
                                elif geo_alert == "OFF" and idx == len(delays) - 1:
                                    status = "ON"
                                    is_updated = True
                            except Exception as e:
                                logger.info("🟠 Malformed condition, skipped alert on {}/{}.".format(geo_item['object2']['name'], geo_item['object2']['id']))
                                
                        if is_updated:
                            break   
                    
                    if is_updated:
                        break       
                
            elif geo_alert == "TRIGGERED":
                
                # TODO:
                # Immediately evaluate all the conditions without delay or restrictions
                try:
                    #alarms_states = [eval("{} {} {}".format(event['object']['value'], operators_dict[a['condition']['operator']], a['condition']['value'])) for a in alarms]
                    alarm_states = []
                    for idx, delay in enumerate(delays):
                        #time.sleep(delay - (0 if idx == 0 else delays[idx-1]))
                        
                        for alarm in [a for a in alarms if delay == int(a['timeout']['value']) * (1 if a['timeout']['units'] == 'seconds' else 60)]:
                            
                            is_updated = False
                                
                            #logger.info("PARAMETERS =====> \n geo_alert: {} \n geo_type: {} \n alarm_conditions_type: {} \n object_radius: {} \n condition_operator: {} \n condition_value: {}".format(geo_alert, geo_type, alarm['condition']['type'], geo_source_result['object']['positionRadius'],  alarm['condition']['operator'],  alarm['condition']['value']))
                            # Check the alarm conditions
                            try:
                                if event['value'] is None:
                                    logger.info("🟠 Skipped alert, no value provided on {}/{}.".format(event['object']['name'], event['object']['id']))
                                    if geo_alert == "OFF" and idx == len(delays) - 1:
                                        status = "ON"
                                        alarm_states.append(True)
                                # elif eval("{} {} {}".format(event['value'], operators_dict[alarm['condition']['operator']], alarm['condition']['value'])) == True:
                                #     status = "TRIGGERED"
                                #     is_updated = True    
                                elif geo_type == "LANDMARK" and alarm['condition']['type'] == "position":
                                    #logger.info("LANDMARK INSIDE =======>")
                                    if distance_from_the_center > geo_source_result['object']['positionRadius'] and alarm['condition']['value'] == False:
                                        status = "TRIGGERED"
                                        alarm_states.append(True)
                                    elif distance_from_the_center < geo_source_result['object']['positionRadius'] and alarm['condition']['value'] == True:
                                        status = "TRIGGERED"
                                        alarm_states.append(True)
                                    else:
                                        status = "ON"
                                        alarm_states.append(False)
                                elif geo_type == "LANDMARK" and alarm['condition']['type'] == "distance":
                                    #logger.info("LANDMARK DISTANCE =======>")
                                    if eval("{} {} {}".format(distance_from_the_center, alarm['condition']['operator'], alarm['condition']['value'])):
                                        status = "TRIGGERED"
                                        alarm_states.append(True)
                                    else:
                                        status = "ON"
                                        alarm_states.append(False)
                                elif geo_type == "ZONE" and alarm['condition']['type'] == "position":
                                    #logger.info("ZONE INSIDE =======>")
                                    if is_point_inside == False and alarm['condition']['value'] == False:
                                        status = "TRIGGERED"
                                        alarm_states.append(True)
                                    elif is_point_inside == True and alarm['condition']['value'] == True:
                                        status = "TRIGGERED"
                                        alarm_states.append(True)
                                    else:
                                        status = "ON"
                                        alarm_states.append(False)
                                elif geo_alert == "OFF" and idx == len(delays) - 1:
                                    status = "ON"
                                    alarm_states.append(False)
                            except Exception as e:
                                logger.info("🟠 Malformed condition, skipped alert on {}/{}.".format(geo_item['object2']['name'], geo_item['object2']['id']))
                                    
                            
                except Exception as e:
                    logger.info("🟠 Failed to update {}/{} alert status. Check conditions format.".format(event['object']['name'], event['object']['id']))
                    return 0
                
                # Turn off the alert if all the conditions are turned off
                if not any(alarm_states) and geo_alert != "ON":
                #    #logger.info("🟢 No condition met for {}/{} turn alert ON.".format(event['object']['name'], event['object']['id']))
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
                    """.format(geo_item['object2']['id'], round(time.time() * 1000), optionsArrayPayload)
                )
                mutation_update_state_result = await client.execute_async(mutation_update_state)
                logger.info("🟢 Updated {}/{} alert status to {}.".format(geo_item['object2']['name'], geo_item['object2']['id'], status))
                
                if status == "TRIGGERED":
                    is_default_message = True
                    if geo_type == "ZONE":
                        if alarm['condition']['type'] == "position":
                            if alarm['condition']['value'] == True:
                                message = "{} is inside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                            elif alarm['condition']['value'] == False:
                                message = "{} is outside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                    elif geo_type == "LANDMARK":
                        if alarm['condition']['type'] == "position":
                            if alarm['condition']['value'] == True:
                                message = "{} is inside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                            elif alarm['condition']['value'] == False:
                                message = "{} is outside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                        elif alarm['condition']['type'] == "distance":
                            if alarm['condition']['operator'] == '>':
                                message = "{} more than {}m from {}".format(event['object']['name'], alarm['condition']['value'], geo_source_result['object']['name'])
                                is_default_message = False
                            elif alarm['condition']['operator'] == '<':
                                message = "{} less than {}m from {}".format(event['object']['name'], alarm['condition']['value'], geo_source_result['object']['name'])
                                is_default_message = False
                    if is_default_message:
                        message = "Triggered by {} ({} {}).".format(geo_item['object2']['name'], alarm['condition']['type'], alarm['condition']['value'])#.format(geo_item['object2']['name'], geo_item['object2']['id'])
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
                        """.format(event['object']['id'], event['object']['name'], message, spec)#format(geo_item['object2']['id'], geo_item['object2']['name'], message, spec)
                    )
                    result_create_notification = await client.execute_async(mutation_create_notification)
                    logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, geo_item['object2']['name'], geo_item['object2']['id']))
                    
                    continue #break
                else:
                    is_default_message = True
                    if geo_type == "ZONE":
                        if alarm['condition']['type'] == "position":
                            if alarm['condition']['value'] == True:
                                message = "{} is outside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                            elif alarm['condition']['value'] == False:
                                message = "{} is inside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                    elif geo_type == "LANDMARK":
                        if alarm['condition']['type'] == "position":
                            if alarm['condition']['value'] == True:
                                message = "{} is outside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                            elif alarm['condition']['value'] == False:
                                message = "{} is inside {}".format(event['object']['name'], geo_source_result['object']['name'])
                                is_default_message = False
                        elif alarm['condition']['type'] == "distance":
                            if alarm['condition']['operator'] == '>':
                                message = "{} less than {}m from {}".format(event['object']['name'], alarm['condition']['value'], geo_source_result['object']['name'])
                                is_default_message = False
                            elif alarm['condition']['operator'] == '<':
                                message = "{} more than {}m from {}".format(event['object']['name'], alarm['condition']['value'], geo_source_result['object']['name'])
                                is_default_message = False
                    if is_default_message:
                        message = "Dismissed by {} ({} {}).".format(geo_item['object2']['name'], alarm['condition']['type'], alarm['condition']['value'])#.format(geo_item['object2']['name'], geo_item['object2']['id'])
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
                        """.format(event['object']['id'], event['object']['name'], message, spec)#format(geo_item['object2']['id'], geo_item['object2']['name'], message, spec)
                    )
                    result_create_notification = await client.execute_async(mutation_create_notification)
                    logger.info("🟢 Created notification ({}) for alert on {}/{}.".format(result_create_notification, geo_item['object2']['name'], geo_item['object2']['id']))
                    
                    continue #break

        return 0
        
    except Exception as e:
        logger.error("🔴 {}".format(e))

@backoff.on_exception(backoff.expo, Exception)
async def resolve_monitor_object_item_geo_alarms_subscription():
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
                        tags: ["application", "monitor", "object"]
                        propertyChanged: [{groupName: "Position", property: "Point"}]
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
                                            schemaTags
                                            alarmStatus: property(propertyName: "Statuses/Alarm")
                                            objectsToObjectsByObject1Id(filter: {
                                                object1: {
                                                    schemaTags: {
                                                    contains: ["application", "monitor"]
                                                    }
                                                }
                                                object2 : {
                                                    schemaTags : {
                                                    contains: ["application", "monitor", "object geo item"]
                                                    }
                                                }
                                            }){
                                                object2 {
                                                    id
                                                    name
                                                    enabled
                                                    schemaTags
                                                    alarms: property(propertyName: "State/Alarms")   
                                                    sourceId: property(propertyName: "State/Source")
                                                    alert: property(propertyName: "State/Alert")
                                                }
                                                
                                                
                                            }
                                            
                                        }
                                    }
                                }
                        }
                    }
                """
            )
            
            logger.info("Dispatcher subscribed on monitor objects geo items events.")
            
            # Send event to be processed by the worker
            async for event in session.subscribe(subscription):
                #logger.debug(event)
                
                # Don't process deletion of monitor items
                if "delete" not in event['Objects']['event']:
                    # Find all the alarms tasks running for this monitor item and reset them if a new message is received
                    task_name = "resolve_monitor_object_item_geo_alarms_{}".format(event['Objects']['relatedNode']['object']['id'])
                    tasks = [task for task in asyncio.all_tasks() if task.get_name() == task_name]
                    # If the tasks are already running cancel them
                    if len(tasks) > 0:
                        tasks[0].cancel()
                    
                    loop.create_task(resolve_monitor_object_item_geo_alarms(event['Objects']['relatedNode']), name=task_name)
                    
    except Exception as e:
        logger.error(e)    
    
    return 0

