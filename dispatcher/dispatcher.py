""" dispatcher.py

Entrypoint to the dispatcher module. Starts all the top level processes and subscriptions necessary for the business logic workers to function.

"""
import asyncio
import authenticate as auth
import dateutil.parser
from envyaml import EnvYAML
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import logging
import logging.config 
import os
import pandas as pd
import subscribe as sub
import sys
import time
# Import the workers of each application from the workers submodules
from admin_workers import * 
from board_workers import *
from dispatcher_workers import *
from user_management_workers import *

# Load dispatcher config
# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("dispatcher")
    
def init_jwt_environment(application, user, password):
    """Initialise the JWT access token for an application.

        The access token is placed in an environment variable representing a dictionnary
        with keys of the form JWT_ENV_{NAME OF APPLICATION FROM CONFIG} and values the
        access tokens.

        Parameters
        ----------
        application : str
            Name of the application as per the configuration file.
            
        user : str
            Username of the application.
            
        password : str
            Password of the application.
            
        Side effects
        ------------
            Sets JWT_ENV_APPLICATION variable in the environment to a valid JWT access token for the application.
            
        Returns
        -------
        JWT env key : str
            Key used to retrieve the active JWT from the environment variables. 
            Format JWT_ENV_{NAME OF APPLICATION FROM CONFIG}.
    """
    
    if application == 'dispatcher':
        jwt = auth.authenticate(config["gql"]["address"], user, password, tags = 'profileTags: ["app profile", "application", "{}"]'.format(user))
    else:
        jwt = auth.authenticate(config["gql"]["address"], user, password, tags = "")
        
    try:
        # Check if the key is already in the environment
        jwt_env = os.environ["JWT_ENV_"+application.upper()]
    except:
        # If not create it
        os.environ["JWT_ENV_"+application.upper()] = jwt
        jwt_env = os.environ["JWT_ENV_"+application.upper()]
        
    return "JWT_ENV_"+application.upper()
    
async def refresh_jwt_environment(application, user, password, timeout):
    """Refresh the access token for an application at regular time intervals.

        Runs as a periodic task in the async loop.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        application : str
            Name of the application as per the configuration file.
            
        user : str
            Username of the application.
            
        password : str
            Password of the application.
            
        timeout : int
            In seconds, how long until the refresh must happen.
            
        Side effects
        ------------
            Sets JWT_ENV_APPLICATION variable in the environment to a valid JWT access token for the application.
            
        Returns
        -------
            Nothing.
    """
    
    while True:
        await asyncio.sleep(timeout)
        jwt = auth.authenticate(config["gql"]["address"], user, password, tags = 'profileTags: ["app profile", "application", "{}"]'.format(user))
        try:
            jwt_env = os.environ["JWT_ENV_"+application.upper()]
        except:
            os.environ["JWT_ENV_"+application.upper()] = jwt
            jwt_env = os.environ["JWT_ENV_"+application.upper()]

def current_milli_time():
    return round(time.time() * 1000)

async def alive_status(gql_address, application, status_timeout):
    """Send the alive status message for the dispatcher module

        Runs as a periodic task in the async loop.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        application : str
            Name of the application.
            
        status_timeout : int
            How often the alive status will be sent in seconds.
    """
    
    # GQL 
    jwt_init = os.environ["JWT_ENV_"+application.upper()]
    transport_init = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt_init})
    client_init = Client(transport=transport_init, fetch_schema_from_transport=False)
    
    # Get the uuid of the dispatcher object
    query_uuid = gql(
        """
        query GetDispatcherObjectId {
            getUserProfileId
        }
        """
    )
    query_uuid_result = await client_init.execute_async(query_uuid)
    
    if query_uuid_result['getUserProfileId'] == None:
        logger.error("🔴 Dispatcher object not found")
        sys.exit(1)
    
    while True:
        await asyncio.sleep(status_timeout)
        try:
            # GQL
            jwt = os.environ["JWT_ENV_"+application.upper()]
            transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
            client = Client(transport=transport, fetch_schema_from_transport=False)
            
            uuid = query_uuid_result['getUserProfileId']
            
            # Send the alive message
            propertiesArray = "[{ groupName: \"HealthCheck\", property: \"Message\", value: \"Dispatcher is alive.\"}]"
            mutation_alive_status = gql(
                """
                mutation DispatcherIsAlive {{
                updateObjectPropertiesByName(input: {{
                    objectId: "{}"
                    transactionId: "{}"
                    propertiesArray: {}
                    }}){{
                        boolean
                    }}
                }}
                """.format(uuid, current_milli_time(), propertiesArray)
            )
            mutation_alive_status_result = await client.execute_async(mutation_alive_status)
            
            logger.debug("💡 Result for mutation sending dispatcher is alive message: {}".format(mutation_alive_status_result))
            
        except Exception as e:
            logger.error("🔴 alive_status(): {}".format(e))
            sys.exit(1)

async def check_online_status(gql_address, application, check_status_timeout):
    """Check and report the status of applications on the platform

        Runs as a periodic task in the async loop.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        application : str
            Name of the application.
            
        check_status_timeout : int
            How often the online status will be checked in seconds.
    """
    
    while True:
        await asyncio.sleep(check_status_timeout)
        jwt = os.environ["JWT_ENV_"+application.upper()]
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # Get all the application objects on the platform
        # Needs to be queried on each loop as applications can come in and out of the platform at any point in time
        query_applications = gql(
            """
            query GetApplicationsProfiles_CheckOnlineStatus {
            objects(filter: { 
                schema: { 
                    type: { equalTo: APPLICATION }
                    mTags: { contains: ["app profile"]}    
                }
            }){
                id
                name
                objectProperties {
                    id
                    groupName
                    property
                    value
                    updatedAt
                }
            }
            }
            """
        )
        query_applications_result = await client.execute_async(query_applications)
        
        logger.debug("💡 Result for query looking for applications profiles on the platform: {}".format(query_applications_result))
        
        if len(query_applications_result['objects']) == 0:
            logger.debug("💡 No applications found.")
            continue
        
        for a in query_applications_result['objects']:
            
            logger.debug("💡 Application id {}, application name {}.".format(a['id'], a['name']))
            
            try:
                uuid = a['id']
                name = a['name']
                # Extract the properties as a dataframe
                a_props = pd.DataFrame(a['objectProperties'])
                # Get timestamp of last message and convert it to epoch time
                msg_timestamp = a_props[(a_props["groupName"] == "HealthCheck") & (a_props["property"] == "Message")][["updatedAt"]].values[0][0]
                msg_timestamp_epoch = dateutil.parser.isoparse(msg_timestamp).timestamp() #datetime.fromisoformat(msg_timestamp).timestamp()
                # Get the timeout that will trigger the application to be declared offline
                msg_timeout = a_props[(a_props["groupName"] == "HealthCheck") & (a_props["property"] == "Timeout")][["value"]].values[0][0]
                # Get last reported status of the application
                current_status = a_props[(a_props["groupName"] == "HealthCheck") & (a_props["property"] == "Status")][["value"]].values[0][0]
                
            except Exception as e:
                logger.error("🔴 Check online status of applications error: {}".format(e))
                continue
            
            try:
                # If the status timed out and the current status is not already offline, update the status to offline
                if time.time() - msg_timestamp_epoch > int(msg_timeout) and current_status != False:
                    optionsArrayPayload = "[{ groupName: \"HealthCheck\", property: \"Status\", value: false}]"
                    mutation_update_status = gql(
                        """
                        mutation UpdateApplicationOnlineStatus {{
                        updateObjectPropertiesByName(input: {{
                            objectId: "{}"
                            transactionId: "{}"
                            propertiesArray: {}
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(uuid, current_milli_time(), optionsArrayPayload)
                    )
                    mutation_update_status_result = await client.execute_async(mutation_update_status)
                    logger.debug("💡 Application {} status is updated to {}.".format(name, "offline"))
                # If the status is not online update the status to online
                elif time.time() - msg_timestamp_epoch <= int(msg_timeout) and current_status != True:
                    optionsArrayPayload = "[{ groupName: \"HealthCheck\", property: \"Status\", value: true}]"
                    mutation_update_status = gql(
                        """
                        mutation UpdateApplicationOnlineStatus {{
                        updateObjectPropertiesByName(input: {{
                            objectId: "{}"
                            transactionId: "{}"
                            propertiesArray: {}
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(uuid, current_milli_time(), optionsArrayPayload)
                    )
                    mutation_update_status_result = await client.execute_async(mutation_update_status)
                    logger.debug("💡 Application {} status is updated to {}.".format(name, "online"))
                    
            except Exception as e:
                logger.error("🔴 Check online status of applications error: {}".format(e))
                continue
    
def start_tasks():
    """Start the tasks of the dispatcher

    """
    try:
        # GQL
        jwt = os.environ["JWT_ENV_DISPATCHER"]
        transport = AIOHTTPTransport(url="http://{}/graphql".format(config["gql"]["address"]), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # Get the uuid of the dispatcher worker object
        query_uuid = gql(
            """
            query GetDispatcherObjectId {
                getUserProfileId
            }
            """
            )
        query_uuid_result = client.execute(query_uuid)
        
        if query_uuid_result['getUserProfileId'] == None:
            logger.error("🔴 Dispatcher object not found")
            sys.exit(1)
            
        # Get the profile object
        query_profile = gql(
            """
            query GetDispatcherProfile {{
                objectProperties(filter: {{
                    objectId: {{equalTo: "{}"}}
                }}){{
                    groupName
                    property
                    value
                }}
            }}
            """.format(query_uuid_result['getUserProfileId'])
        )
        profile = client.execute(query_profile)
        
        for p in profile['objectProperties']:
            if p['groupName'] in ['admin', 'board', 'dispatcher', 'user_management']:
                task_params = p['value']
                if task_params['enabled'] == False:
                    logger.info("🟠 Task Configuration - App: {} | Task: {} | Params: {}".format(p['groupName'], p['property'], p['value']))
                elif task_params['enabled'] and task_params['tasktype'] == 'job':
                    logger.info("🟢 Task Configuration - App: {} | Task: {} | Params: {}".format(p['groupName'], p['property'], p['value']))
                    loop.create_task(eval("{}()".format(p['property'])), name = p['property'])
                elif task_params['enabled'] and task_params['tasktype'] == 'worker':
                    logger.info("🟢 Task Configuration - App: {} | Task: {} | Params: {}".format(p['groupName'], p['property'], p['value']))
                    loop.create_task(eval("{}()".format(p['property'])), name = p['property'])
        
    except Exception as e:
        logger.error("🔴 Error on startup: {}".format(e))
        sys.exit(1)
        
    return 0
    
if __name__ == "__main__":
    
    # Async events loop
    loop = asyncio.get_event_loop()
    
    # Generate access tokens for each component of the dispatcher
    for key, value in config["apps"].items():
        user = value["user"]
        password = value["password"]
        
        # Authenticate
        jwt_env_key = init_jwt_environment(key, user, password)
        logger.info("🟢 Generated token for {}.".format(key))
        
        # Create authenticate refresh process
        loop.create_task(refresh_jwt_environment(key, user, password, config["gql"]["refresh_token_timeout"]), name="refresh_jwt_environment")
        logger.info("🟢 Started refresh token job for {}.".format(key))
        
    # Subcribe to controls in order to handle the dispatcher's RPCs
    loop.create_task(sub.subscribe_on_topic("dispatcher", "controls"), name="dispatcher_controls_subscription")
    
    # Create alive status process for the dispatcher
    loop.create_task(alive_status(config["gql"]["address"], "dispatcher", config["processes"]["is_alive_timeout"]), name="dispatcher_is_alive_job")
    logger.info("🟢 Started 'is alive status' job for the dispatcher.")
    
    # Create check online status process for the applications
    loop.create_task(check_online_status(config["gql"]["address"], "dispatcher", config["processes"]["check_status_timeout"]), name="health_check_job")
    logger.info("🟢 Started 'check apps status' job.")
    
    # Get workers info from the dispatcher object and start jobs / subscriptions for each of those.
    logger.info("🟢 Starting dispatcher tasks...")
    
    start_tasks()
    
    logger.info("🟢 Dispatcher running...")
    #
    loop.run_forever()