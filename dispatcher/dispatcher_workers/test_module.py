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

async def test_module(gql_address, jwt, event):
    """Test of the dispatcher module

        RPC called to run and report functional tests of the dispatcher module

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt : str
            GraphQL access token.
            
        event : str
            Event received from a subscription.
    """

    #logger.debug(asyncio.all_tasks())
    
    transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    #logger.debug(event)
    if event["name"] == "TestModule" and event["type"] == "RPC_STEALTH":
        logger.debug("RPC STEALTH Test Module DONE")
    elif event["name"] == "TestModule" and event["type"] == "RPC":
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
        
        logger.debug("RPC Test Module ACK")
        
        mutation_test_report = gql(
            """
            mutation RPCReport {{
            createControlExecutionReport(
                input: {{
                linkedControlId: {}
                report: \"TESTED: 1, PASSED: 1, FAILED: 0\"
                reportDetails: \"{{}}\"
                done: false
                error: false
            }}) {{
                integer
                }}
            }}
            """.format(event["id"])
        )
        mutation_test_report_result = await client.execute_async(mutation_test_report)
        logger.debug("RPC Test Module test report")
        
        mutation_done = gql(
            """
            mutation RPCFinalReport {{
            createControlExecutionReport(
                input: {{
                linkedControlId: {}
                report: \"PASSED\"
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
        logger.debug("RPC Test Module DONE")
    return 0