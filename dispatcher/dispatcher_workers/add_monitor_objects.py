""" 
RPC for adding devices as monitor objects in bulk
"""
import asyncio
from admin_workers import * 
from board_workers import *
from dispatcher_workers import *
from envyaml import EnvYAML
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import json
import logging
import logging.config 
import os
import time
from user_management_workers import *
import yaml

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("controls")

def validate_rpc_parameters_and_device(parameters, device):
    """ Checks the validity of the rpc parameters
    
    Args:
    =====
        parameters: json object

        device: json object
        
    Return:
    =======
        boolean
        True if the parameters are valid. 
        False otherwise.
    
    """
    
    if parameters['general']['objectNameContains'] == "":
        if parameters['general']['status'] == 'All':
            return True 
        elif parameters['general']['status'] == 'Enabled' and device['enabled'] == True:
            return True
        elif parameters['general']['status'] == 'Disabled' and device['enabled'] == False:
            return True
    elif parameters['general']['objectNameContains'] in device['name']:
        if parameters['general']['status'] == 'All':
            return True 
        elif parameters['general']['status'] == 'Enabled' and device['enabled'] == True:
            return True
        elif parameters['general']['status'] == 'Disabled' and device['enabled'] == False:
            return True
    
    return False

def create_properties_link_string(properties_link_array):
    
    if len(properties_link_array) > 0:
        properties_link_string_array = ['{{sourceObjectId: "{}",  sourceGroupName: "{}", sourceProperty: "{}", destSchemaId: "{}", destGroupName: "{}", destProperty: "{}" }}'.format(link['sourceObjectId'], link['sourceGroupName'], link['sourceProperty'], link['destSchemaId'], link['destGroupName'], link['destProperty']) for link in properties_link_array]
        properties_link_string = "propertiesLinks: [{}]".format(",".join(properties_link_string_array))    
    else:
        properties_link_string = ""
            
    return properties_link_string

async def add_monitor_objects(gql_address, jwt, event):
    """ Add Monitor Objects RPC
    """
    try:
        
        # For debugging purpose
        #logger.info(event)
        
        # Catch the RPC by name
        if event["name"] == "AddMonitorObjects":
            
            # Get the token of the user submitting the creation
            # GQL calls will be made on behalf of the user submition
            if event['params']['JWT'] is not None:
                jwt = event['params']['JWT']
            
            # Set the GQL client
            transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            
            # Acknowledge receipt of the RPC
            mutation_acknowledge = gql(
                """
                mutation Acknowledge_AddMonitorObjectsRpc{{
                updateControlExecutionAck(
                    input: {{
                    controlsExecutionId: \"{}\"
                }}) {{
                    boolean
                    }}
                }}
                """.format(event["id"])
            )
            mutation_acknowledge_result = await client.execute_async(mutation_acknowledge)
            
            logger.debug("💡 Result for mutation sending RPC acknolwdegement: {}".format(mutation_acknowledge_result))
            
            # Grab the parameters for the creation of the monitor objects
            parameters = json.loads(event['params']['PARAMETERS'])
            
            # For debugging purpose
            #logger.info(parameters)
            
            # Find all the objects to import
            query_find_devices = gql(
                """
                query FindDevices_AddMonitorObjectsRpc{{
                    objects(filter: {{ schema: {{ id: {{ equalTo: "{}" }} }} }}) {{
                        id
                        name
                        enabled
                        objectProperties {{
                            id
                            groupName
                            property
                            value
                        }}
                        objectsToObjectsByObject2Id(filter: {{ 
                            object1: {{ 
                                schema: {{ 
                                    type: {{ equalTo: DATASET }} 
                                    mTags: {{ equalTo: ["application", "monitor", "object"] }}
                                    }} 
                                }}        
                            }}) {{
                            object2Id
                        }}
                    }}
                }}
                """.format(parameters['general']['objectType'])
            )
            result_find_devices = await client.execute_async(query_find_devices)
            
            # For debugging purpose
            #logger.info("Found devices")
            #logger.info(result_find_devices)
            
            # Instantiate the object count
            devices_processed = 0
            devices_already_linked = 0
            devices_errors = 0
            i = int(parameters['general']['objectTemplateStart'])
            for device in result_find_devices['objects']:
                
                devices_processed += 1
                
                # Check if device is already linked to a monitor object
                if len(device['objectsToObjectsByObject2Id']) > 0:
                    devices_already_linked += 1
                    continue
                
                # Create if the rpc parameters are valid
                if validate_rpc_parameters_and_device(parameters, device):
                    
                    # For debugging purpose
                    #logger.info(device['objectProperties'])
                    
                    #
                    location = [{"sourceId": device['id'], "propertyId": p['id']} for p in device['objectProperties'] if p['groupName']+"/"+p['property'] in [l['sourcePropertyId'] for l in parameters['location']]]
                    sources = "["+','.join(['{{sourceId: "{}", propertyId: "{}"}}'.format(l['sourceId'], l['propertyId']) for l in location])+"]"
                    
                    #
                    positions = [p['value'] for p in device['objectProperties'] if p['value'] is not None and p['id'] in [l['propertyId'] for l in location]]
                    if len(positions) > 0:
                        createdLocation = "{{alt: 0, lat: {}, lon: {}}}".format(positions[0]['lat'], positions[0]['lon'])
                    else:
                        createdLocation = "{{alt: 0, lat: {}, lon: {}}}".format(parameters['centerMap'][1], parameters['centerMap'][0])
                    
                    # Building properties link
                    properties_link_array = []
                    # Monitoring item
                    for monitoringItem in parameters['monitoringItems']:
                        link = {"sourceObjectId": device['id'], "sourceGroupName": monitoringItem['property'].split("/")[0], "sourceProperty": monitoringItem['property'].split("/")[1], "destSchemaId": monitoringItem['itemId'], "destGroupName": "State", "destProperty": "Value"}
                        properties_link_array.append(link)
                    # Geo item
                    for geoItem in parameters['geoItems']:
                        link = {"sourceObjectId": geoItem['id'], "sourceGroupName": "Info", "sourceProperty": "Own id", "destSchemaId": geoItem['itemId'], "destGroupName": "State", "destProperty": "Source"}
                        properties_link_array.append(link)
                        
                    # For debugging purpose
                    #logger.info(create_properties_link_string(properties_link_array))
                    
                    children = '"{}"'.format(device['id'])
                    if len(parameters['geoItems']) > 0:
                        for p in parameters['geoItems']:
                            children = children+',"{}"'.format(p['id'])
                            
                    mutation_create_object = gql(
                        """
                        mutation CreateMonitorObject_ForAddMonitorObjectsRpc{{
                          createObjectWithProperties(input: {{
                              name: "{}"
                              enabled: true
                              schemaId: "{}"
                              childs: [{}]
                              properties: [{{groupName: "Position", property: "Sources", value: {}}}, {{groupName: "Position", property: "Point", value: {}}}]
                              {}
                          }})  {{
                            uuid  
                          }}
                        }}
                        """.format(parameters['general']['objectNameTemplate'].format(i), parameters['general']['type'], children, sources, createdLocation, create_properties_link_string(properties_link_array))
                    )
                    result_create_object = await client.execute_async(mutation_create_object)
                    
                    logger.debug("💡 Result for mutation creating monitor object for RPC: {}".format(result_create_object))
                    
                    if result_create_object['createObjectWithProperties']['uuid'] is None:
                        devices_errors += 1
                        
                    i += 1
                else:   
                    devices_errors += 1
                    
                if devices_processed in [int(len(result_find_devices['objects']) * 0.25), int(len(result_find_devices['objects']) * 0.50), int(len(result_find_devices['objects']) * 0.75)]:
                    
                    logger.info("🧾 Add monitor objects RPC report. Processed: {} out of {}, Already linked: {}, Errors: {}".format(devices_processed, len(result_find_devices['objects']), devices_already_linked, devices_errors, "false" if devices_errors == 0 else "true"))
                    
                    mutation_report = gql(
                    """
                    mutation Report_AddMonitorObjectsRpc {{
                    createControlExecutionReport(
                        input: {{
                        linkedControlId: {}
                        report: \"Processed: {} out of {}, Already linked: {}, Errors: {}\"
                        reportDetails: \"{{}}\"
                        done: false
                        error: {}
                    }}) {{
                        integer
                        }}
                    }}
                    """.format(event["id"], devices_processed, len(result_find_devices['objects']), devices_already_linked, devices_errors, "false" if devices_errors == 0 else "true")
                    )
                    mutation_report_result = await client.execute_async(mutation_report)
                    
                    logger.debug("💡 Result for mutation creating report for RPC AddMonitorObjects: {}".format(mutation_report_result))
            
            logger.info("🧾 Add monitor objects RPC complete. Processed: {} out of {}, Already linked: {}, Errors: {}".format(devices_processed, len(result_find_devices['objects']), devices_already_linked, devices_errors, "false" if devices_errors == 0 else "true"))
            
            mutation_done = gql(
            """
            mutation FinalReport_AddMonitorObjectsRpc {{
            createControlExecutionReport(
                input: {{
                linkedControlId: {}
                report: \"Processed: {} out of {}, Already linked: {}, Errors: {}\"
                reportDetails: \"{{}}\"
                done: true
                error: {}
            }}) {{
                integer
                }}
            }}
            """.format(event["id"], devices_processed, len(result_find_devices['objects']), devices_already_linked, devices_errors, "false" if devices_errors == 0 else "true")
            )
            mutation_done_result = await client.execute_async(mutation_done)
            
            logger.debug("💡 Result for mutation creating final report for RPC AddMonitorObjects: {}".format(mutation_done_result))
        
    except Exception as e:
        logger.error(e)