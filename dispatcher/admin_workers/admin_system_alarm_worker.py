""" System Alarm Worker

The goal of this worker is to create a notification anytime a device is switched to a no response status or a battery low status.

This is achieved by creating a GQL subscription for property changes on objects targetted at the measurement properties BATTERY_LOW and RESPONSE_STATUS.

Any event received creates an async task responsible to create a notification corresponding to the alarms in the notification table if necessary.

The notifications created for these alarms are tagged as 'admin', 'alert' and 'system' with:
    * Messages 'Offline' or 'Battery low'.
    * Specs of the form {type: ----, alarm: ----, property: ----}
"""

import asyncio
import backoff
from envyaml import EnvYAML
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import logging
import logging.config 
import os

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("admin")

async def admin_system_alarm_worker(event):
    """Create a notification when a system alarm is raised

        Parameters
        ----------
        event : json
            Event received from a subscription.
    """
    try:
        # Default parameters for notification
        trigger_notification = False
        spec = r'''{\"type\": \"alert\", \"alarm\": \"ON\", \"property\": \"UNKNOWN\"}'''
        message = ""
        
        # Cases for alarm types
        if event['property'] == 'RESPONSE_STATUS':
            if event['value'] == 'false':
                trigger_notification = True 
                spec = r'''{\"type\": \"SYSTEM_ALARM\", \"alarm\": \"OFFLINE\", \"property\": \"RESPONSE_STATUS\"}'''
                message = "Offline"
        elif event['property'] == 'BATTERY_LOW':
            if event['value'] == 'true':
                trigger_notification = True
                spec = r'''{\"type\": \"SYSTEM_ALARM\", \"alarm\": \"LOW BATTERY\", \"property\": \"BATTERY_LOW\"}'''
                message = "Battery low"
                
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_ADMIN"
        jwt = os.environ[jwt_env_key]
                
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # GQL mutation to create notification
        mutation = gql(
            """
            mutation{{
                createNotification(input: {{
                    notification: {{
                        subjectType: "device"
                        subject: "{}"
                        tags: ["admin", "alert", "system"]
                        message: "{}"
                        spec: "{}"
                        }}
                    }}){{
                            notification {{
                                id
                            }}
                        }}
                    }}
            """.format(event['objectId'], message, spec)
        )
        
        # Only create a notification if the object is enabled
        if event['object']['enabled'] and trigger_notification:
            result = await client.execute_async(mutation)
            logger.info("🟢 System alarm notification created: {}".format(result))
            
    except Exception as e:
        logger.error("🔴 {}".format(e))

@backoff.on_exception(backoff.expo, Exception)
async def admin_system_alarm_subscription():
    """Subscription for system alarm events
    
        Creates a filtered subscription which targets the specific events that should trigger
        the admin system alarm worker.
        
        Upon receiving an event signalling a change on either Measurements/BATTERY_LOW or Measurements/RESPONSE_STATUS
        an admin_system_alarm_woker task is created to process the event.
    
    """
    try:
        # Grab async event loop 
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_ADMIN"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            
            # GQL subscription for property changes triggering alarm events
            subscription = gql(
                """
                subscription SystemAlarmSubscription {
                    Objects(filterA: {
                        propertyChanged: [{groupName: "Measurements", property: "RESPONSE_STATUS"}, {groupName: "Measurements", property: "BATTERY_LOW"}]
                    }){
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
                                        object {
                                            id
                                            schemaType
                                            name
                                            enabled
                                            tags
                                            schemaId
                                        }
                                    }
                                }
                        }
                    }
                """
            )
            
            logger.info("Admin system alarm worker is subscribed.")
            
            # Send alarm events to be processed by the worker
            async for event in session.subscribe(subscription):
                logger.debug(event)
                loop.create_task(admin_system_alarm_worker(event['Objects']['relatedNode']))
                
    except Exception as e:
        logger.error(e)
        