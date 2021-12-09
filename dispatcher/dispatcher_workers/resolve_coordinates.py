""" Resolve coordinates of monitor objects

Timer based task.

Runs the main loop every at regular time intervals.

Identify the monitor objects and attempts to resolve their coordinates based on the coordinates sources.

"""
import asyncio
import backoff
from envyaml import EnvYAML
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import logging
import logging.config 
import os
import time

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("resolve coordinates")

@backoff.on_exception(backoff.expo, Exception)
async def resolve_coordinates_job():
    """Coordinates Solver
    
        The generic objects of the monitor application (tagged with ['application', 'monitor', 'object']) have Coordinates group composed of the following properties:
        
        - Coordinates/POSITION (json) {'lat': <LATITUDE OF THE OBJECT>, 'lon': <LONGITUDE OF THE OBJECT>, 'alt': <ALTITUDE OF THE OBJECT>}
        - Coordinates/SOURCE_1 (json) {'sourceid': <UUID OF THE OBJECT USED AS A SOURCE TO RESOLVE THE COORDINATES>, 'sourcetype': <Coordinates/GPS OR Coordinates/BEACONS OR Coordinates/SCANNERS OR Coordinates/FIXED>}
        - Coordinates/SOURCE_2 (json) {'sourceid': <UUID OF THE OBJECT USED AS A SOURCE TO RESOLVE THE COORDINATES>, 'sourcetype': <Coordinates/GPS OR Coordinates/BEACONS OR Coordinates/SCANNERS OR Coordinates/FIXED>}
        ...
        - Coordinates/SOURCE_N (json) {'sourceid': <UUID OF THE OBJECT USED AS A SOURCE TO RESOLVE THE COORDINATES>, 'sourcetype': <Coordinates/GPS OR Coordinates/BEACONS OR Coordinates/SCANNERS OR Coordinates/FIXED>}
            
        The Coordinates/POSITION property is calculated by the coordinates solved based on the Coordinates/SOURCE_i properties where the index denotes the order of priority (1 being highest priority).
        
        Each coordinates source specifies the uuid of a device and a source type which describes the way the position is to be resolved.
        
        The coordinates solver supports the following resolution methods:
        
        1. Coordinates/GPS
            The solver looks for the properties Measurements/GPS_STATUS, Measurements/GPS_LON and Measurements/GPS_LAT in the device specified by the source id.
            If such properties are available and Measurements/GPS_STATUS is non zero then those coordinates measurements are used to update the Coordinates/POSITION of the generic object in the monitor application.
        
        2. Coordinates/BEACONS [fixed beacons]
            The solver looks for the property Coordinates/BEACONS in the device specified by the source id.
            Coordinates/BEACONS is expected to be a list of beacons of the form [{'uuid': <UUID OF A BEACON>, 'rssi': <RSSI READ OF THE BEACON DURING LAST SCAN>}, ...].
            The solver selects the element of the Coordinates/BEACONS list with the strongest signal.
            Using the beacon uuid the solver identifies the monitor object associated to the beacon and reads its Coordinates/POSITION.
            If the Coordinates/POSITION is set, it is used to update the Coordinates/POSITION of the generic object in the monitor application.
        
        3. Coordinates/SCANNERS [fixed scanners]
            The solver looks for all the scanner devices tagged with ['scanner', 'coordinates']. Provisioned in the system.
            Each scanner device is expected to provide a Coordinates/BEACONS property listing scanned beacons of the form [{'uuid': <UUID OF A BEACON>, 'rssi': <RSSI READ OF THE BEACON DURING LAST SCAN>}, ...].
            The solver look through the scanners to find the scanner with the strongest signal for the source id provided in Coordinates/SOURCE_i.
            Using the scanner uuid the solver identifies the monitor object associated to the scanner and read its Coordinates/POSITION.
            If the Coordinates/POSITION is set, it is used to update the Coordinates/POSITION of the generic object in the monitor application.
        
        4. Coordinates/FIXED
            The solver looks identifies the monitor object associated to the device specified by the source id.
            If the Coordinates/POSITION is set, it is used to update the Coordinates/POSITION of the generic object in the monitor application.
        
        If we cannot resolve the coordinates with any of the sources the latitude, longitude and altitude default to none.
        
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
                                            property: {{equalTo: "resolve_coordinates_job"}}
                                        }}){{
                                            groupName
                                            property
                                            value
                                        }}
                                    }}
            """.format(query_uuid_result['getUserProfileId'])
        )
        profile = await client_dispatcher.execute_async(query_profile)
        
        params = profile['objectProperties'][0]['value']
        
        # Default parameter, run the job once per minute
        timer = 60
        # If another valid timeout is provided use it instead
        if type(params['timer']) == int:
            if params['timer'] >= 5:
                timer = params['timer']
        
    except Exception as e:
        logger.error("🟠 Backoff, restarting resolve_coordinates_job on error, {}".format(e))
    
    # Exit if the job is tagged as inactive
    if profile['objectProperties'][0]['value'] == False:
        logger.info("This task won't be running. Change 'active' parameter to 'true' to launch the task.")
        return 0
    
    logger.info("🟢 Starting Task: {} | Params: {}".format("resolve_coordinates_job", params))
    
    # Main loop for coordinates resolution
    while True:
        await asyncio.sleep(timer)
        try:
            jwt = os.environ["JWT_ENV_DISPATCHER"]
            transport = AIOHTTPTransport(url="http://{}/graphql".format(config["gql"]["address"]), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            # Get all the widget counter objects
            find_objects_query = gql(
                """
                query FindObjectsWithCoordinatesToResolve{
                    objects(filter: {
                        schemaTags: {
                            contains: ["application", "monitor", "object"]
                        }
                    }) {
                        id
                        name
                        schemaTags
                        position: property(propertyName: "Position/Point")
                        sources: property(propertyName: "Position/Sources")
                    }
                }
                """
            )
            find_objects_result = await client.execute_async(find_objects_query)
            
            # Get all the beacons provisioned into the monitoring platform
            find_beacons_query = gql(
                """
                query FindObjectsWithCoordinatesToResolve{
                    objects(filter: {
                                        schemaTags: {
                                            contains: ["application", "monitor", "object"]
                                        }
                                    }
                            ){
                                id
                                name
                                schemaTags
                                position: property(propertyName: "Position/Point")
                                sources: property(propertyName: "Position/Sources")
                                objectsToObjectsByObject1Id(filter: {
                                    object1: {
                                        schemaTags: {
                                        contains: ["application", "monitor", "object"]
                                        }
                                    }
                                    object2 : {
                                        schemaType : {
                                        equalTo: "device"
                                        }
                                    }
                                }) {
                                    object2 {
                                            id
                                            name
                                            enabled
                                            trackingId: property(propertyName: "Spatial/TRACKING_ID")
                                            beacons: property(propertyName: "Spatial/BEACONS")
                                        }
                                }   
                            }
                                                        }
                """
            )
            find_beacons_result = await client.execute_async(find_beacons_query)
            
            beacons_list = []
            scanners_list = []
            for beacon_result in find_beacons_result['objects']:
                position = beacon_result['position']
                for device in beacon_result['objectsToObjectsByObject1Id']:
                    d_id, d_name, d_tracking_id, d_beacons = device['object2']['id'], device['object2']['name'], device['object2']['trackingId'], device['object2']['beacons']
                    if d_tracking_id is not None:
                        beacons_list.append({"id": d_id, "name": d_name, "position": position, "trackingid": d_tracking_id})
                    if d_beacons is not None:
                        scanners_list.append({"id": d_id, "name": d_name, "position": position, "beacons": d_beacons})
                        
            #logger.debug("🚨 Beacons: {}".format(beacons_list))
            #logger.debug("📡 Scanners: {}".format(scanners_list))
            
            # TODO: Can be made asynchronous
            # Loop through the monitoring objects and try to resolve their coordinates
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
                        
                        #==========> Resolve with GPS coordinates
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
                                        #logger.info("🟢 Updated position of {}/{} to {}.".format(obj['name'], obj['id'], find_source_property_result['objectProperty']['value']))
                                        continue
                                else:
                                    True
                                    #logger.info("🟠 Cannot resolve coordinates if source {} status is {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], find_source_property_result['objectProperty']['value']['status'], obj['name'], obj['id']))    
                            else:
                                True
                                #logger.info("🟠 Cannot resolve coordinates if source {} value is {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], find_source_property_result['objectProperty']['value'], obj['name'], obj['id']))
                        #==========> Resolve by scanning beacons
                        elif find_source_property_result['objectProperty']['property'] == "BEACONS":
                            if find_source_property_result['objectProperty']['value'] is not None:
                                if find_source_property_result['objectProperty']['value'] != [{}]:
                                    # Rank the scanners by rssi
                                    ranked_signal = sorted(find_source_property_result['objectProperty']['value'], key =lambda k: int(k["rssi"]), reverse=True)
                                    
                                    is_resolved = False
                                    for signal in ranked_signal:
                                        for beacon in beacons_list:
                                            if beacon['trackingid'] == signal['mac'] and is_resolved == False:
                                                if obj['position']['lat'] != beacon['position']['lat'] or obj['position']['lon'] != beacon['position']['lon']:
                                                    optionsArrayPayload = "[{ groupName: \"Position\", property: \"Point\", value: {lat: "+ str(beacon['position']['lat']) + ", lon:" +  str(beacon['position']['lon']) +", alt:" +  str(0) +"} }]"
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
                                                    #logger.info("🟢 Updated position of {}/{} to {} based on beacon {}.".format(obj['name'], obj['id'], beacon['position'], beacon['trackingid']))
                                                    is_resolved = True
                                                    break 
                                                else:
                                                    is_resolved = True
                                                    break
                                        if is_resolved == True:
                                            break    
                                    if is_resolved == False:
                                        True
                                        #logger.info("🟠 Could not resolve coordinates")    
                                else:
                                    True
                                    #logger.info("🟠 Cannot resolve coordinates if source {} status is {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], find_source_property_result['objectProperty']['value']['status'], obj['name'], obj['id']))    
                            else:
                                True
                                #logger.info("🟠 Cannot resolve coordinates if source {} value is {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], find_source_property_result['objectProperty']['value'], obj['name'], obj['id']))
                        #==========> Resolve by searching through scanners
                        elif find_source_property_result['objectProperty']['property'] == "TRACKING_ID":
                                tracking_id = find_source_property_result['objectProperty']['value']
                                sources = []
                                for scanner in scanners_list:
                                    for beacon in scanner['beacons']:
                                        if "mac" in beacon:
                                            if beacon['mac'] == tracking_id:
                                                sources.append({"position": scanner['position'], "rssi": beacon['rssi'], "id": scanner['id']})
                                if len(sources) > 0:
                                    ranked_scanners = sorted(sources, key =lambda k: int(k["rssi"]), reverse=True)
                                    if obj['position']['lat'] != ranked_scanners[0]['position']['lat'] or obj['position']['lon'] != ranked_scanners[0]['position']['lon']:
                                        optionsArrayPayload = "[{ groupName: \"Position\", property: \"Point\", value: {lat: "+ str(ranked_scanners[0]['position']['lat']) + ", lon:" +  str(ranked_scanners[0]['position']['lon']) +", alt:" +  str(0) +"} }]"
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
                                        #logger.info("🟢 Updated position of {}/{} to {} based on scanner {}.".format(obj['name'], obj['id'], ranked_scanners[0]['position'], ranked_scanners[0]['id']))
                                        continue
                                else: 
                                    True
                                    #logger.info("🟠 Could not resolve coordinates") 
                        else:
                            #logger.info("🟠 Cannot resolve coordinates from source of type {} on object {}/{}.".format(find_source_property_result['objectProperty']['property'], obj['name'], obj['id']))
                            continue
            
            continue
        
        except Exception as e:
            logger.error("🟠 Backoff, restarting resolve_coordinates_job on error, {}".format(e))
            continue