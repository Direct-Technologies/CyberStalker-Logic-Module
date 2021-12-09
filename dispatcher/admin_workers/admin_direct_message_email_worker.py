""" Email delivery worker for direct messages

"""

import asyncio
import authenticate as auth
import backoff
from email.message import EmailMessage
from envyaml import EnvYAML
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import logging
import logging.config
import os
import smtplib

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("admin")

async def admin_direct_message_email_worker(event):
    """Send email notifications when a notification event is received

        Parameters
        ----------
        event : str
            Event received from a subscription.
            
        Raises
        ------
        No users found
            If no users are found the worker is terminated.
    """
    try:
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_ADMIN"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # GQL query to get email delivery configurations 
        query_email_configurations = gql(
            """
            query getEmailDeliveryConfigurations{
                objects(filter: {
                    schemaTags: {
                    contains: ["application", "admin", "notification", "email configuration"]
                    }
                }){
                    id
                    name
                    enabled
                    description
                    schemaId
                    tags
                    schemaName
                    schemaTags
                    schemaType
                    address: property(propertyName: "Credentials/ADDRESS")
                    port: property(propertyName: "Credentials/PORT")
                    smtp: property(propertyName: "Credentials/SMTP")
                    token: property(propertyName: "Credentials/TOKEN")
                    body: property(propertyName: "Settings/BODY_TEMPLATE")
                    from: property(propertyName: "Settings/FROM")
                    subject: property(propertyName: "Settings/SUBJECT")
                }
            }
            """
        )
        email_configurations = await client.execute_async(query_email_configurations)
        
        logger.debug(email_configurations)
        
        # Raise an error if the email template is missing
        if len(email_configurations['objects']) < 1:
            raise Exception("🟠 Email delivery of direct message, error: no email delivery configurations were found.")
        
        # GQL query to get user data
        query_user = gql(
        """
        query GetUser {{
            user(id: "{}")
                    {{
                        id
                        login
                        enabled
                        mName
                        mEmail
                        mPhone
                        mTags
                        activated
                        userProfiles(filter: {{
                            object : {{
                                schemaTags: {{
                                    equalTo: ["application", "admin", "user profile"]
                                }}
                            }}
                        }}) {{
                            id
                            object {{
                                id
                                name
                                schemaTags
                                tags
                                receiveNotificationsFromUsers: property(propertyName: "Notifications/Accept from users")
                                receiveNotificationsViaEmail: property(propertyName: "Notifications/Email notifications")
                            }}
                        }}    
                    }}
                }}
        """.format(event['to'])
        )
        user = await client.execute_async(query_user)
        
        # Skip delivery if the user has no email address
        if user['user']['mEmail'] == None:
            logger.info("🟠 User has no email address: {}".format(user['user']['login']))
            return 0
        
        # Skip delivery if the user is not activated
        if user['user']['activated'] == False:
            logger.info("🟠 User is not activated: {}".format(user['user']['login']))
            return 0
        
        # Skip delivery if the user is not enabled
        if user['user']['enabled'] == False:
            logger.info("🟠 User is not enabled: {}".format(user['user']['login']))
            return 0
        
        # Check profile
        if len(user['user']['userProfiles']) == 0:
            logger.info("🟠 User has no valid profile: {}".format(user['user']['login']))
            return 0
        
        admin_profile = user['user']['userProfiles'][0]
        
        # Skip delivery if the user opted out of direct messages
        if admin_profile['object']['receiveNotificationsFromUsers'] == False:
            logger.info("🟠 User opted out of direct messages notifications: {}".format(user['user']['login']))
            return 0
        
        # Skip delivery if the user opted out of email deliveries
        if admin_profile['object']['receiveNotificationsViaEmail'] == False:
            logger.info("🟠 User opted out of email notifications: {}".format(user['user']['login']))
            return 0
            
        for email_configuration in email_configurations:    
            
            # No delivery for configurations that are not enabled
            if email_configuration['enabled'] == False:
                continue                            
            
            # Build email
            msg_content = msg_content = "{}".format(event['message'])
            msg = EmailMessage()
            msg.set_content(msg_content)
            msg['Subject'] = email_configuration['subject']
            msg['From'] = email_configuration['from']
            msg['To'] = user['user']['mEmail'] 
            
            try:
                # Send email   
                server = smtplib.SMTP_SSL(email_configuration['smtp'], email_configuration['port'])
                server.login(email_configuration['address'], email_configuration['token'])
                server.send_message(msg)
                server.quit()
                error_message = ""
                delivery_status = "delivered: true"
                
                # GQL mutation to generate a record of delivery in case of success
                mutation_create_notification_delivery = gql(
                    """
                    mutation{{
                        createNotificationDelivery(input: {{
                            notificationDelivery: {{
                            target: "{}"
                            targetName: "{}"
                            notificationId: "{}"
                            {}
                            {}
                            deliveryPath: "EMAIL"
                            message: "{}"
                            deliveryObjectId: "{}"
                            }}
                        }}){{    
                            notificationDelivery {{
                            id
                            }}
                        }}
                        }}
                    """.format(user['user']['id'], user['user']['login'], event['id'], delivery_status, error_message, msg_content, email_configuration['id'])
                )
                
            except Exception as e:
                # Build error message if the delivery failed
                error_message = "error: \"" + str(e) + "\""
                delivery_status = "delivered: false"
                
                # GQL mutation to generate a record of delivery in case of failure
                mutation_create_notification_delivery = gql(
                    """
                    mutation{{
                        createNotificationDelivery(input: {{
                            notificationDelivery: {{
                            target: "{}"
                            targetName: "{}"
                            notificationId: "{}"
                            {}
                            {}
                            deliveryPath: "EMAIL"
                            message: "{}"
                            deliveryObjectId: "{}"
                            }}
                        }}){{    
                            notificationDelivery {{
                            id
                            }}
                        }}
                        }}
                    """.format(user['user']['id'], user['user']['login'], event['id'], delivery_status, error_message, msg_content, email_configuration['id'])
                )
            
            create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)
            
            logger.debug("Notification delivery created: {}".format(create_notification_delivery))
    
    except Exception as e:
        logger.error(e)

@backoff.on_exception(backoff.expo, Exception)
async def admin_direct_message_email_subscription():
    """Subscription for delivery of direct messages.
    
        Listen on notification events and triggers an email delivery worker 
        if a notification containing the 'direct message' tag is received.
    
    """
    try:
        # Grab the async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_ADMIN"
        jwt = os.environ[jwt_env_key]
        token_id = await auth.get_token_id(gql_address, jwt_env_key)
        topic = "notifications:"+token_id
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            subscription = gql(
                """
                subscription onNotification{
                Notifications {
                    event 
                    relatedNodeId
                    relatedNode {
                        ... on Notification {
                            id
                            subjectType
                            subject
                            subjectName
                            tags
                            by
                            spec
                            message
                            to 
                        }
                    }
                }
                }
                """
            )
             
            logger.info("Listening on {} for {}".format("direct message notifications", "admin"))
            
            # Send notifications to be delivered via email when they are directed to a user
            async for event in session.subscribe(subscription):
                logger.debug(event)
                if event['Notifications']['relatedNode']['to'] != None:
                    loop.create_task(admin_direct_message_email_worker(event['Notifications']['relatedNode']))
                
    except Exception as e:
        logger.error(e)