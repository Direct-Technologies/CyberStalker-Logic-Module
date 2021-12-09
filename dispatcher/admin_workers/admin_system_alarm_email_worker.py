""" Email delivery worker for system alarms

The goal of this worker is to send email notifications for system alerts to admin users.

This is achieved by creating a GQL subscription for notifications with the tags 'admin', 'alert' and 'system'.

Any event received creates an async task responsible to deliver email notifications to users.

Emails are generated for each user eligible to receive them using email configuration objects.
Those email configuration objects are identified via the schema tags 'application', 'admin', 'noitification' and 'email configuration'.
A properly constructured email configuration object must have the following properties:
* Credentials/ADDRESS
* Credentials/PORT
* Credentials/SMTP
* Credentials/TOKEN
* Settings/BODY_TEMPLATE
* Settings/FROM
* Settings/SUBJECT

"""

import asyncio
import authenticate as auth
import backoff
from envyaml import EnvYAML
from email.message import EmailMessage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import json
import logging
import logging.config 
import os
import smtplib

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 

# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("admin")

# Mapping of naming conventions
admin_device_dict = {"BATTERY_LOW": "DEV_LOWBAT", "RESPONSE_STATUS": "DEV_STATUS"}

async def admin_system_alarm_email_worker(event):
    """Send email notifications when a notification event is received

        Specific to the admin application.
        Relies on email templates with hardcoded uuid.

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
            raise Exception("🟠 Email delivery of system alarms, error: no email delivery configurations were found.")
        
        # GQL query to get user data
        query_users = gql(
        """
        query GetUsers {{
            users(filter: {
                enabled: {
                    equalTo: true
                }
                mActivated: {
                    equalTo: true
                }
            })
                    {
                        id
                        login
                        enabled
                        mName
                        mEmail
                        mPhone
                        mTags
                        activated
                        userProfiles(filter: {
                            object : {
                                schemaTags: {
                                    equalTo: ["application", "admin", "user profile"]
                                }
                            }
                        }) {
                            id
                            object {
                                id
                                name
                                schemaTags
                                tags
                                generalNotificationsMode: property(propertyName: "Notifications/GENERAL")
                                receiveNotificationsFromUsers: property(propertyName: "Notifications/Accept from users")
                                receiveNotificationsViaEmail: property(propertyName: "Notifications/Email notifications")
                                receiveNotificationsFromLowBattery: property(propertyName: "Alerts/LOW_BATTERY")
                                receiveNotificationsFromResponseStatus: property(propertyName: "Alerts/STATUS")
                                
                            }
                        }    
                    }
                }
        """
        )
        users = await client.execute_async(query_users)
        
        # Skip delivery if there are no users on the platform
        if len(users['users']) == 0:
            raise Exception("🟠 Email delivery of system alarms, error: no users found on the platform.")
        
        # For each user find out if they should receive an email notification
        for user in users['users']:
            
            # Skip delivery if the user has no email address
            if user['mEmail'] == None:
                logger.info("🟠 User has no email address: {}".format(user['login']))
                continue
            
            # Skip delivery if the user is not activated
            if user['activated'] == False:
                logger.info("🟠 User is not activated: {}".format(user['login']))
                continue
            
            # Skip delivery if the user is not enabled
            if user['enabled'] == False:
                logger.info("🟠 User is not enabled: {}".format(user['login']))
                continue
            
            # Check profile
            if len(user['userProfiles']) == 0:
                logger.info("🟠 User has no valid profile: {}".format(user['login']))
                continue
            
            admin_profile = user['userProfiles'][0]
            
            # Skip delivery if the user opted out of email deliveries
            if admin_profile['object']['receiveNotificationsViaEmail'] == False:
                logger.info("🟠 User opted out of email notifications: {}".format(user['login']))
                continue
            
            #Skip delivery if the user opted out of low battery notifications
            if admin_profile['object']['receiveNotificationsFromLowBattery'] == False and json.loads(event['spec'])['property'] == 'BATTERY_LOW':
                logger.info("🟠 User opted out of low battery notifications: {}".format(user['login']))
                continue
            
            #Skip delivery if the user opted out of response status notifications
            if admin_profile['object']['receiveNotificationsFromResponseStatus'] == False and json.loads(event['spec'])['property'] == 'RESPONSE_STATUS':
                logger.info("🟠 User opted out of response status notifications: {}".format(user['login']))
                continue
            
            # Skip delivery if the user has muted notifications
            if admin_profile['object']['generalNotificationsMode'] == 'Muted':
                logger.info("🟠 User opted out of all notifications: {}".format(user['login']))
                continue
            
            #TODO: Skip delivery if the user opted in for notifications on favourites only
            if admin_profile['object']['receiveNotificationsViaEmail'] == False:
                logger.info("🟠 User opted out of email notifications: {}".format(user['login']))
                continue
            
            for email_configuration in email_configurations:    
            
                # No delivery for configurations that are not enabled
                if email_configuration['enabled'] == False:
                    continue    
            
                # Build email
                msg_content = "{} alarm {} on {}.".format(json.loads(event['spec'])['property'], json.loads(event['spec'])['alarm'], event['subjectName'])
                msg = EmailMessage()
                msg.set_content(msg_content)
                msg['Subject'] = email_configuration['subject']
                msg['From'] = email_configuration['from']
                msg['To'] = user['mEmail'] 
                
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
                        """.format(user['id'], user['login'], event['id'], delivery_status, error_message, msg_content, email_configuration['id'])
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
                                user: "{}"
                                userLogin: "{}"
                                notificationId: "{}"
                                delivered: true
                                {}
                                deliveryPath: "APPLICATIONS"
                                message: "{}"
                                deliveryObjectId: "bcba7224-fe0e-4da4-aa2f-9e79314f3906"
                                }}
                            }}){{    
                                notificationDelivery {{
                                id
                                }}
                            }}
                            }}
                        """.format(user['id'], user['login'], event['id'], event['message'])
                    )
                
                create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)
                
                logger.debug("Notification delivery created: {}".format(create_notification_delivery))

    except Exception as e:
        logger.error(e)

@backoff.on_exception(backoff.expo, Exception)
async def admin_system_alarm_email_subscription():
    """Subscription for system alarm delivery
    
        Listen on notification events and triggers an email delivery worker 
        if a notification containing the 'system' tag is received.
    
    """
    try:
        # Grab the async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_ADMIN"
        jwt = os.environ[jwt_env_key]
        token_id = await auth.get_token_id(gql_address, jwt_env_key)
        # topic = "notifications:"+token_id
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            subscription = gql(
                """
                subscription onNotification{
                    Notifications(filterA: {
                                    tags: ["admin", "alert", "system"]
                                }){
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
            
            logger.info("👂 Listening on {} for {}".format("notifications", "admin"))
            
            # Send system alarm notifications to be delivered via email
            async for event in session.subscribe(subscription):
                logger.debug(event)
                if event['Notifications']['relatedNode']['to'] == None:
                    loop.create_task(admin_system_alarm_email_worker(event['Notifications']['relatedNode']))
                
    except Exception as e:
        logger.error(e)