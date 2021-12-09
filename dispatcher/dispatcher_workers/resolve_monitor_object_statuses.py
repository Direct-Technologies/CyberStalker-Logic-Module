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
async def resolve_monitor_object_statuses_job():
    """
       ... 
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
                                            property: {{equalTo: "resolve_monitor_object_statuses_job"}}
                                        }}){{
                                            groupName
                                            property
                                            value
                                        }}
                                    }}
            """.format(query_uuid_result['getUserProfileId'])
        )
        profile = await client_dispatcher.execute_async(query_profile)
        
        #logger.debug(profile)
        
        params = profile['objectProperties'][0]['value']
        
        # Default parameter, run the job once per minute
        timer = 60
        # If another valid timeout is provided use it instead
        if type(params['timer']) == int:
            if params['timer'] >= 2:
                timer = params['timer']
        
    except Exception as e:
        logger.error("🟠 Backoff, restarting resolve_monitor_object_statuses_job on error, {}".format(e))
    
    # Exit if the job is tagged as inactive
    if profile['objectProperties'][0]['value'] == False:
        logger.info("This task won't be running. Change 'active' parameter to 'true' to launch the task.")
        return 0
    
    logger.info("🟢 Starting Task: {} | Params: {}".format("resolve_monitor_object_statuses_job", params))
    
    while True:
        await asyncio.sleep(timer)
        try:
            jwt = os.environ["JWT_ENV_DISPATCHER"]
            transport = AIOHTTPTransport(url="http://{}/graphql".format(config["gql"]["address"]), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            # Get all the widget counter objects
            find_objects_query = gql(
                """
                query FindMonitorObjects{
                    objects(filter: {
                        schemaTags: {
                            contains: ["application", "monitor", "object"]
                        }
                    }) {
                        id
                        name
                        schemaTags
                        activity: property(propertyName: "Statuses/Activity")
                        batteryLevel: property(propertyName: "Statuses/Battery level")
                        batteryType: property(propertyName: "Statuses/Battery type")
                        data: property(propertyName: "Statuses/Data")
                        emulation: property(propertyName: "Statuses/Emulation")
                        gps: property(propertyName: "Statuses/GPS")
                        status: property(propertyName: "Statuses/Status")
                        sources: property(propertyName: "Position/Sources")
                        objectsToObjectsByObject1Id(filter: {
                            object2: {
                                schemaType: {
                                    equalTo: "device"
                                }
                            }
                            }){
                            object2 {
                                id
                                name
                                schemaTags
                                responseStatus: property(propertyName: "Measurements/RESPONSE_STATUS")
                                dcPower: property(propertyName: "Measurements/DC_POWER")
                                batteryLevel: property(propertyName: "Measurements/BATTERY_LEVEL")
                                batteryLow: property(propertyName: "Measurements/BATTERY_LOW")
                                gpsStatus: property(propertyName: "Measurements/GPS_STATUS")
                            }
                        }
                    }
                }
                """
            )
            find_objects_result = await client.execute_async(find_objects_query)
            
            #logger.debug(find_objects_result)
            
            for obj in find_objects_result['objects']:
                response_status_list = []
                dc_power_list = []
                battery_level_list = []
                battery_low_list = []
                gps_status_list = []
                for device in obj['objectsToObjectsByObject1Id']:
                    #logger.debug(device)
                    response_status_list.append(device['object2']['responseStatus'])
                    dc_power_list.append(device['object2']['dcPower'])
                    battery_level_list.append(device['object2']['batteryLevel'])
                    battery_low_list.append(device['object2']['batteryLow'])
                    gps_status_list.append(device['object2']['gpsStatus'])
                    
                response_status_value = "UNKNOWN"
                if len(response_status_list) > 0:
                    if all([False if r is None else r for r in response_status_list]):
                        response_status_value = "ACTIVE"
                    else:
                        response_status_value = "NO_RESPONSE"
                
                dc_power_value = "UNKNOWN"
                if len(dc_power_list) > 0:
                    if all(dc_power_list):
                        dc_power_value = "DC"
                    else:
                        if any(battery_low_list):
                            dc_power_value = "LOW"
                        else:
                            dc_power_value = "FULL"
                
                battery_level_value = 0
                if len(battery_level_list) > 0:
                    battery_level_list = [0 if b is None else b for b in battery_level_list]
                    battery_level_value = min(battery_level_list)
                    
                gps_status_value = "UNKNOWN"
                if len(gps_status_list) > 0:
                    if min([0 if s is None else s for s in gps_status_list]) in [0,1]:
                        gps_status_value = "FIXED"
                    else:
                        gps_status_value = "NOT_FIXED"
                    
                # Update the object statuses
                updates = []
                # Battery level
                if obj['batteryLevel'] != battery_level_value:
                    updates.append("{groupName: \"Statuses\", property: \"Battery level\", value: " + str(battery_level_value) + "}")
                # Battery type
                if obj['batteryType'] != dc_power_value:
                    updates.append("{groupName: \"Statuses\", property: \"Battery type\", value: \"" + str(dc_power_value) + "\"}")
                # Gps
                #if obj['gps'] != gps_status_value:
                #    updates.append("{groupName: \"Statuses\", property: \"GPS\", value: \"" + str(gps_status_value) + "\"}")
                # Status
                if obj['activity'] != response_status_value:
                    updates.append("{groupName: \"Statuses\", property: \"Activity\", value: \"" + str(response_status_value) + "\"}")
                
                if len(updates) > 0:
                    optionsArrayPayload = "["
                    for i in range(len(updates)):
                        if i < len(updates) - 1:
                            optionsArrayPayload = optionsArrayPayload + updates[i] + " ,"
                        else:
                            optionsArrayPayload = optionsArrayPayload + updates[i] + "]"
                
                    #logger.debug(optionsArrayPayload)
                
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
                        """.format(obj['id'], round(time.time() * 1000), optionsArrayPayload)
                    )
                    mutation_update_state_result = await client.execute_async(mutation_update_state)
                    logger.info("🟢 Updated statuses of {}/{} with {}.".format(obj['name'], obj['id'], optionsArrayPayload))
                
            continue
            
            for obj in find_objects_result['objects']:
                for source in obj['sources']:
                    if 'sourceId' in source:
                        find_source_property_query = gql(
                            """
                            query FindSourceProperty{{
                                objectProperty(id: "{}") {{
                                    id
                                    objectId
                                    groupName
                                    property
                                    value
                                }}
                            }}
                            """.format(source['propertyId'])
                        )
                        find_source_property_result = await client.execute_async(find_source_property_query)
                        
                        #logger.info(find_source_property_result)
                        
                        if find_source_property_result['objectProperty']['property'] == "GPS":
                            if find_source_property_result['objectProperty']['value'] is not None:
                                if find_source_property_result['objectProperty']['value']['status'] != 0:
                                    if obj['position']['lat'] != find_source_property_result['objectProperty']['value']['lat'] or obj['position']['lon'] != find_source_property_result['objectProperty']['value']['lon']:
                                        optionsArrayPayload = "[{ groupName: \"Position\", property: \"Point\", value: {lat: "+ str(find_source_property_result['objectProperty']['value']['lat']) + ", lon:" +  str(find_source_property_result['objectProperty']['value']['lon']) +", alt:" +  str(find_source_property_result['objectProperty']['value']['alt']) +"} }]"
                                        mutation_update_position = gql(
                                            """
                                            mutation UpdateMonitorObjectPosition {{
                                            updateObjectPropertiesByName(input: {{
                                                objectId: "{}"
                                                transactionId: "{}"
                                                propertiesArray: {}
                                                }}){{
                                                    boolean
                                                }}
                                            }}
                                            """.format(obj['id'], round(time.time() * 1000), optionsArrayPayload)
                                        )
                                        mutation_update_position_result = await client.execute_async(mutation_update_position)
                                        logger.info("🟢 Updated position of {}/{} to {}.".format(obj['name'], obj['id'], find_source_property_result['objectProperty']['value']))
                                        continue
                                else:
                                    logger.info("🟠 Cannot resolve coordinates if source {} status is {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], find_source_property_result['objectProperty']['value']['status'], obj['name'], obj['id']))    
                            else:
                                logger.info("🟠 Cannot resolve coordinates if source {} value is {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], find_source_property_result['objectProperty']['value'], obj['name'], obj['id']))
                        else:
                            logger.info("🟠 Cannot resolve coordinates from source of type {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], obj['name'], obj['id']))
                            continue
            
            continue
            
            # Cache all scanners in the system
            find_scanners_query = gql(
                """
                query FindScanners{
                    objects(filter: {
                        schemaTags: {
                            contains: ["scanners", "coordinates"]
                        }
                    }){
                        id
                        name
                        schemaId
                        tags
                        schemaName
                        schemaTags
                        beacons: property(propertyName: "Measurements/BEACONS")
                    }
                }
                """
            )
            find_scanners_result = await client.execute_async(find_scanners_query)
            
            #logger.debug(find_scanners_result)
            
            # Collect all the scanners to be reused later in this cycle
            scanners = pd.DataFrame(find_scanners_result['objects'])
            
            for o in find_objects_result['objectProperties']:
                #logger.debug(o['object'])
                op = pd.DataFrame(o['object']['objectProperties'])
                op_sources = op[(op['groupName'] == 'Coordinates') & (op['property'].str.contains('SOURCE'))]
                
                o_position = op[(op['groupName'] == 'Coordinates') & (op['property'] == 'POSITION')].iloc[0]['value']
                o_meta = op[(op['groupName'] == 'Coordinates') & (op['property'] == 'POSITION')].iloc[0]['meta']
                resolved_position = o_position
                position_source = o_meta
                
                #logger.debug(resolved_position)
                #logger.debug(position_source)
                
                for source in op_sources.itertuples():
                    #logger.debug(source) 
                    #logger.debug(source.property)
                    #logger.debug(source.value)
                    
                    s = json.loads(source.value)
                    
                    #logger.debug(s['id'])
                    #logger.debug(s['source'])
                    
                    # If no source id is given skip to next source
                    if s['id'] is None:
                        continue
                    # Get the tracker object
                    find_tracker_query = gql(
                    """
                    query FindTracker{{
                        object(id: "{}"){{
                            nodeId
                            id
                            objectProperties {{
                                id
                                groupName
                                property
                                value
                                meta
                                updatedAt
                            }}
                        }}   
                    }}
                    """.format(s['id'])
                    )
                    find_tracker_result = await client.execute_async(find_tracker_query)
                    
                    tracker = pd.DataFrame(find_tracker_result['object']['objectProperties'])
                    
                    # Proceed depending on source type
                    if s['source'] == 'Coordinates/GPS':
                        #logger.debug('Coordinates/GPS')
                        
                        gps_status = tracker[(tracker['groupName'] == 'Measurements') & (tracker['property'] == 'GPS_STATUS')].iloc[0]['value']
                        gps_lon = tracker[(tracker['groupName'] == 'Measurements') & (tracker['property'] == 'GPS_LON')].iloc[0]['value']
                        gps_lat = tracker[(tracker['groupName'] == 'Measurements') & (tracker['property'] == 'GPS_LAT')].iloc[0]['value']
                        
                        #logger.debug("Status {} | Lon {} | Lat {}".format(gps_status, gps_lon, gps_lat))
                        
                        if gps_status != 0 and gps_lon is not None and gps_lat is not None:
                            resolved_position = {"lon": gps_lon, "lat": gps_lat, "alt": 0}
                            position_source = {"source": "{}:{}".format(s['id'], s['source'])}
                            
                            #logger.debug(resolved_position)
                            #logger.debug(position_source)
                            
                            break
                            
                        #logger.debug('No coordinates sources could be resolved')
                        resolved_position = {"lon": "None", "lat": "None", "alt": "None"}
                        position_source = {"source": "Position could not be resolved from sources."}
                        continue
                    
                    elif s['source'] == 'Coordinates/BEACONS':
                        #logger.debug('Coordinates/BEACONS')
                        
                        beacons = tracker[(tracker['groupName'] == 'Measurements') & (tracker['property'] == 'BEACONS')]
                        
                        logger.debug(beacons)
                        
                        if not beacons.empty:
                            beacons_list = beacons.iloc[0]['value']
                            #last_scan_time = beacons.iloc[0]['updatedAt']
                            
                            closest_beacon_id = pd.DataFrame(beacons_list).sort_values(by = ['rssi'], ascending = False).iloc[0]['id']
                            
                            #logger.debug(closest_beacon_id)
                            
                            find_beacon_query = gql(
                            """
                                query FindBeacon{{
                                    object(id: "{}"){{
                                        id
                                        name
                                        schemeId
                                        tags
                                        schemaName
                                        schemaTags
                                        schemaType
                                        position: property(propertyName: "Coordinates/POSITION")
                                    }}
                                }}
                                """.format(closest_beacon_id)
                            )
                            find_beacon_result = await client.execute_async(find_beacon_query)
                            
                            if find_beacon_result is not None:
                                resolved_position = find_beacon_result['object']['position']
                                position_source = {"source": "{}:{}".format(closest_beacon_id, "Coordinates/POSITION")}
                                
                                break
                                
                            #logger.debug('No coordinates sources could be resolved')
                            resolved_position = {"lon": "None", "lat": "None", "alt": "None"}
                            position_source = {"source": "Position could not be resolved from sources."}
                            continue
                       
                        
                    elif s['source'] == 'Coordinates/SCANNERS':
                        #logger.debug('Coordinates/SCANNERS')
                        
                        #logger.debug(scanners)
                        # The beacon attached to the object being tracked s['id']
                        
                        # All the scanners that have seen the beacon in their last update
                        scanned = scanners[scanners['beacons'].str.contains(s['id'])]
                       
                        #logger.debug('No coordinates sources could be resolved')
                        resolved_position = {"lon": "None", "lat": "None", "alt": "None"}
                        position_source = {"source": "Position could not be resolved from sources."}
                        continue
                        
                    else:
                        #logger.debug('No coordinates sources could be resolved')
                        resolved_position = {"lon": "None", "lat": "None", "alt": "None"}
                        position_source = {"source": "Position could not be resolved from sources."}
                        continue
                
                # =================> Update the tracked object here
                logger.debug("Resolving cooredinates on {}, uuid: {}".format(o['object']['name'], o['object']['id']))
                logger.debug(resolved_position)
                logger.debug(position_source)
                
                o_position = op[(op['groupName'] == 'Coordinates') & (op['property'] == 'POSITION')].iloc[0]['value']
                o_meta = op[(op['groupName'] == 'Coordinates') & (op['property'] == 'POSITION')].iloc[0]['meta']
                
                
                # Update the object only if the position has changed
                # TODO: Conversion to json is slow
                if o_position != json.dumps(resolved_position): 
                
                    optionsArrayPayload = "{ \\\"groupname\\\": \\\"Coordinates\\\",\\\"property\\\":\\\"POSITION\\\", \\\"value\\\": " + str(json.dumps(resolved_position)).replace('"', '\\\"') + ", \\\"meta\\\": { \\\"source\\\": \\\"" + str(position_source['source']) + "\\\" } }"
                        
                    logger.debug(optionsArrayPayload)
                    
                    update_position_mutation = gql(
                        """
                        mutation UpdatePosition {{
                        updateObjectPropertiesExtended(input: {{
                            objId: "{}"
                            transactionId: "{}"
                            optionsArray: "[{}]"
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(o['object']['id'], round(time.time() * 1000), optionsArrayPayload)
                    )
                    update_position_result = await client.execute_async(update_position_mutation)
                    
                    logger.debug(update_position_result)
                    
            #continue # =====================> Move on to next object
        
        except Exception as e:
            logger.error("🟠 Backoff, restarting resolve_coordinates_job on error, {}".format(e))
            continue