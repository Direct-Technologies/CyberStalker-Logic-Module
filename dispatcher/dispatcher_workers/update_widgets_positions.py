import logging
import time
import logging.config 
import yaml
import os
import json
from envyaml import EnvYAML
import asyncio
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
# Import the workers of each application from the workers submodules
from admin_workers import * 
from board_workers import *
from dispatcher_workers import *
from user_management_workers import *

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("controls")

async def update_widgets_positions(gql_address, jwt, event):
    """
    """
    try:
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        if event["name"] == "UpdateWidgetsPositions" and event["type"] == "RPC_STEALTH":
            # The parameter should be a json array [{id: ..., position: ...}]
            group_id = event['objectId']#json.loads(event['params'])['GROUP_ID']
            
            query_group = gql(
                """
                query GetGroup {{
                  object(id: "{}") {{
                      id
                      name
                      order: property(propertyName: "General/Order")
                  }}  
                }}
                """.format(group_id)
            )
            query_group_result = await client.execute_async(query_group)
            
            widgets = query_group_result['object']['order']
            
            #logger.info(widgets)
            
            for idx, widget in enumerate(widgets):
                optionsArrayPayload = "[{ groupName: \"Status\", property: \"Position\", value: " + str(idx + 1) + "}]"
                mutation_update_position = gql(
                    """
                    mutation UpdateWidgetPosition {{
                    updateObjectPropertiesByName(input: {{
                        objectId: "{}"
                        transactionId: "{}"
                        propertiesArray: {}
                        }}){{
                            boolean
                        }}
                    }}
                    """.format(widget, round(time.time() * 1000), optionsArrayPayload)
                )
                mutation_update_position_result = await client.execute_async(mutation_update_position)
            
                logger.debug("💡 Result of the mutation that updates a widget position: {}".format(mutation_update_position_result))
            
        elif event["name"] == "UpdateWidgetsPositions" and event["type"] == "RPC":
            mutation_acknowledge = gql(
                """
                mutation AckRPC {{
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
            
            logger.debug("💡 Result of the mutation for RPC execution ACK: {}".format(mutation_acknowledge_result))
            
            # The parameter should be a json array [{id: ..., position: ...}]
            group_id = event['objectId']#json.loads(event['params'])['GROUP_ID']
            
            query_group = gql(
                """
                query GetGroup {{
                  object(id: "{}") {{
                      id
                      name
                      order: property(propertyName: "General/Order")
                  }}  
                }}
                """.format(group_id)
            )
            query_group_result = await client.execute_async(query_group)
            
            widgets = query_group_result['object']['order']
            
            #logger.info(widgets)
            
            for idx, widget in enumerate(widgets):
                optionsArrayPayload = "[{ groupName: \"Status\", property: \"Position\", value: " + str(idx + 1) + "}]"
                mutation_update_position = gql(
                    """
                    mutation UpdateWidgetPosition {{
                    updateObjectPropertiesByName(input: {{
                        objectId: "{}"
                        transactionId: "{}"
                        propertiesArray: {}
                        }}){{
                            boolean
                        }}
                    }}
                    """.format(widget, round(time.time() * 1000), optionsArrayPayload)
                )
                mutation_update_position_result = await client.execute_async(mutation_update_position)
            
                logger.debug("💡 Result of the mutation that updates a widget position: {}".format(mutation_update_position_result))
                
                # mutation_test_report = gql(
                #     """
                #     mutation RPCReport {{
                #     createControlExecutionReport(
                #         input: {{
                #         linkedControlId: {}
                #         report: \"Task cancelled\"
                #         reportDetails: \"{{}}\"
                #         done: false
                #         error: false
                #     }}) {{
                #         integer
                #         }}
                #     }}
                #     """.format(event["id"])
                # )
                # mutation_test_report_result = await client.execute_async(mutation_test_report)
            
            mutation_done = gql(
                """
                mutation RPCFinalReport {{
                createControlExecutionReport(
                    input: {{
                    linkedControlId: {}
                    report: \"Task restarted\"
                    reportDetails: \"{{}}\"
                    done: true
                    error: false
                }}) {{
                    integer
                    }}
                }}
                """.format(event["id"])
            )
            mutation_done_result = await client.execute_async(mutation_done)
            
            logger.debug("💡 Result of the mutation for RPC execution done: {}".format(mutation_done_result))
            
    except Exception as e:
        logger.error(e)
    return 0