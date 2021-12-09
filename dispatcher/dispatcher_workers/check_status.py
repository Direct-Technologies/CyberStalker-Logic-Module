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

async def check_status(gql_address, jwt, event):
    """Check status of the dispatcher module

        RPC called to check if the dispatcher module is running.

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
    
    if event["name"] == "CheckStatus" and event["type"] == "RPC_STEALTH":
        logger.debug("RPC STEALTH Check Status DONE")
    elif event["name"] == "CheckStatus" and event["type"] == "RPC":
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
        
        logger.debug("RPC Check Status ACK")
        
        mutation_done = gql(
            """
            mutation RPCFinalReport {{
            createControlExecutionReport(
                input: {{
                linkedControlId: {}
                report: \"Online\"
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
        
        logger.debug("RPC Check Status DONE")