import logging
import time
import logging.config 
import yaml
from envyaml import EnvYAML
import asyncio
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("dispatcher")

async def reset_access_tokens(gql_address, jwt, event):
    """Reset all the workers modules access tokens

        RPC for the dispatcher module.
        
        Currently a demo RPC. Does not execute reset.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt : str
            GraphQL access token.
            
        event : str
            Event received from a subscription.
    """
    
    transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    #logger.debug(event)
    if event["name"] == "ResetAccessTokens" and event["type"] == "RPC_STEALTH":
        logger.debug("RPC STEALTH Reset Access Tokens DONE")
    elif event["name"] == "ResetAccessTokens" and event["type"] == "RPC":
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
        
        logger.debug("RPC Reset Access Tokens ACK")
        
        mutation_report = gql(
            """
            mutation RPCReport {{
            createControlExecutionReport(
                input: {{
                linkedControlId: {}
                report: \"Started Processing\"
                reportDetails: \"{{}}\"
                done: false
                error: false
            }}) {{
                integer
                }}
            }}
            """.format(event["id"])
        )
        mutation_report_result = await client.execute_async(mutation_report)
        
        logger.debug("RPC Reset Access Tokens REPORT")
        
        mutation_done = gql(
            """
            mutation RPCFinalReport {{
            createControlExecutionReport(
                input: {{
                linkedControlId: {}
                report: \"Finished\"
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
        logger.debug("RPC Reset Access Tokens DONE")
    return 0