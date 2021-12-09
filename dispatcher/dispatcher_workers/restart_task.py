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

async def restart_task(gql_address, jwt, event):
    """Restart a dispatcher task

        ...

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt : str
            GraphQL access token.
            
        event : str
            Event received from a subscription.
    """
    try:
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        if event["name"] == "RestartTask" and event["type"] == "RPC_STEALTH":
            # Name of the task to be restarted
            task_name = json.loads(event['params'])['TASK_NAME']
            logger.debug(task_name)
            # Check if the task is currently running
            task, = [task for task in asyncio.all_tasks() if task.get_name() == task_name]
            logger.debug(task)
            # If the task is already running cancel it
            if task is not None:
                task.cancel()
            # Restart the task
            loop = asyncio.get_event_loop()
            loop.create_task(eval("{}()".format(task_name)), name = task_name)
            # TODO: Add checks to make sure the task_name is valid by checking which tasks are provisioned in the dispatcher
            logger.debug("RPC STEALTH Restart Task DONE")
        elif event["name"] == "RestartTask" and event["type"] == "RPC":
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
            
            logger.debug("RPC Restart Task ACK")
            
            # Name of the task to be restarted
            task_name = json.loads(event['params'])['TASK_NAME']
            # Check if the task is currently running
            task = [task for task in asyncio.all_tasks() if task.get_name() == task_name]
            # If the task is already running cancel it
            if len(task) > 0:
                task[0].cancel()
            
            mutation_test_report = gql(
                """
                mutation RPCReport {{
                createControlExecutionReport(
                    input: {{
                    linkedControlId: {}
                    report: \"Task cancelled\"
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
            
            # Restart the task
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(eval("{}()".format(task_name)), name = task_name)
            except Exception as e:
                logger.error(e)
                mutation_done = gql(
                    """
                    mutation RPCFinalReport {{
                    createControlExecutionReport(
                        input: {{
                        linkedControlId: {}
                        report: \"{}\"
                        reportDetails: \"{{}}\"
                        done: true
                        error: true
                    }}) {{
                        integer
                        }}
                    }}
                    """.format(event["id"], e)
                )
                mutation_done_result = await client.execute_async(mutation_done)
                logger.debug("RPC Restart Task FAILED")
                return 0
            
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
            logger.debug("RPC Restart Task DONE")
    except Exception as e:
        logger.error(e)
    return 0