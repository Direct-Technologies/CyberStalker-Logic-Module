""" Notification delivery

Handles delivering notifications to users on the platform through different channels:
* Platform apps
* Email
* SMS
* WhatsApp

"""
import asyncio
import backoff
from datetime import datetime
from envyaml import EnvYAML
from email.message import EmailMessage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import json
import logging
import logging.config 
import os
import pandas as pd
import smtplib
import time
import twilio.rest
from twilio.base.exceptions import TwilioRestException
import yaml

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("notification delivery")

async def notification_delivery_app_worker(event):
    """ Notification delivery to apps
    
    Creates notification deliveries to be consumed by the applications.
    
    Args:
    =====
        event: json object
            A notification event.
            
    Returns:
    ========
        none
    
    """
    try:
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        
        # GQL query to get user data
        query_users = gql(
        """
        query GetUsers {
            users   {
                        id
                        login
                        enabled
                        mName
                        mEmail
                        mPhone
                        mTags
                        activated
                        userProfiles(filter: {
                            object: {
                                schemaTags: {
                                    contains: ["user profile"]
                                }
                            }
                            }){
                            id
                            object {
                                id
                                name
                                schemaTags
                                tags
                                generalNotificationsMode: property(propertyName: "Notifications/GENERAL")
                                appName: property(propertyName: "Program/Name")
                            }
                        }    
                    }
                }
        """
        )
        users = await client.execute_async(query_users)
        
        # Skip delivery if there are no users on the platform
        if len(users['users']) == 0:
            raise Exception("🟠 No users found on the platform.")
        
        for user in users['users']:
            
            #logger.debug(user)
            
            # Skip delivery if the user is not activated
            # if user['activated'] == False:
            #     logger.info("🟠 User is not activated: {}".format(user['login']))
            #     continue
            # Skip delivery if the user is not enabled
            if user['enabled'] == False:
                logger.info("🟠 User is not enabled: {}".format(user['login']))
                continue
            # Check profile
            if len(user['userProfiles']) == 0:
                logger.info("🟠 User has no valid profile: {}".format(user['login']))
                continue
            
            for profile in user['userProfiles']:
                
                #logger.debug(profile)
            
                # Skip delivery if the user has muted notifications
                if profile['object']['generalNotificationsMode'] == 'Muted':
                    logger.info("🟠 User opted out of all notifications: {}".format(user['login']))
                    continue
                
                # GQL mutation to generate a record of delivery in case of success
                mutation_create_notification_delivery = gql(
                    """
                    mutation{{
                        createNotificationDelivery(input: {{
                            notificationDelivery: {{
                            delivered: true
                            user: "{}"
                            userLogin: "{}"
                            notificationId: "{}"
                            deliveryPath: "{}"
                            message: "{}"
                            
                            }}
                        }}){{    
                            notificationDelivery {{
                            id
                            }}
                        }}
                        }}
                    """.format(user['id'], user['login'], event['id'], profile['object']['appName'], event['message'])
                )
                create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)
        
    except Exception as e:
        logger.info("🟠 Could not deliver notification {} to {} error {}.".format(event['id'], user['login'], e)) 

async def notification_delivery_email_worker(event):
    """ Notification delivery for emails
    
    Sends email notifications to eligible users of the platform. Creates a notification delivery to report on the attempt.
    
    Args:
    =====
        event: json object
            A notification event.
            
    Returns:
    ========
        none
    
    """
    try:
        
        logger.debug(event)
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # GQL query to get user data
        query_users = gql(
        """
        query GetUsers {
            users   {
                        id
                        login
                        enabled
                        mName
                        mEmail
                        mPhone
                        mTags
                        activated
                        userProfiles(filter: {
                            object: {
                                schemaTags: {
                                    contains: ["user profile"]
                                }
                            }
                            }){
                            id
                            object {
                                id
                                name
                                schemaTags
                                tags
                                generalNotificationsMode: property(propertyName: "Notifications/GENERAL")
                                receiveNotificationsViaEmail: property(propertyName: "Notifications/Email notifications")
                                appName: property(propertyName: "Program/Name")
                            }
                        }    
                    }
                }
        """
        )
        users = await client.execute_async(query_users)
        
        # Skip delivery if there are no users on the platform
        if len(users['users']) == 0:
            raise Exception("🟠 No users found on the platform.")
        
        for user in users['users']:
            
            # Skip delivery if the user is not activated
            if user['activated'] == False:
                logger.debug("🟠 User is not activated: {}".format(user['login']))
                continue
            # Skip delivery if the user is not enabled
            if user['enabled'] == False:
                logger.debug("🟠 User is not enabled: {}".format(user['login']))
                continue
            # Check email
            if user['mEmail'] == None:
                logger.debug("🟠 User has no email address: {}".format(user['login']))
                continue
            # Check profile
            if len(user['userProfiles']) == 0:
                logger.debug("🟠 User has no valid profile: {}".format(user['login']))
                continue
            
            for profile in user['userProfiles']:
                
                # Skip delivery if the user has muted notifications
                if profile['object']['generalNotificationsMode'] == 'Muted':
                    logger.debug("🟠 User opted out of all notifications: {}".format(user['login']))
                    continue
                # Skip delivery if user opted out of email
                if profile['object']['receiveNotificationsViaEmail'] == False:
                    logger.debug("🟠 User opted out of email notifications: {}".format(user['login']))
                    continue
                
                # Find all the email delivery configurations
                find_email_configs_query = gql(
                    """
                    query FindEmailConfigs{
                        objects(filter: {
                            schemaTags: {
                                equalTo: ["application", "dispatcher", "notification", "email", "configuration"]
                            }
                        }) {
                            id
                            name
                            schemaTags
                            port: property(propertyName: "Credentials/PORT")
                            smtp: property(propertyName: "Credentials/SMTP")
                            address: property(propertyName: "Credentials/ADDRESS")
                            token: property(propertyName: "Credentials/TOKEN")
                            from: property(propertyName: "Settings/FROM")
                            subject: property(propertyName: "Settings/SUBJECT")
                            status: property(propertyName: "HealthCheck/Status")
                            tagsFilter: property(propertyName: "Notifications filter/TAGS_FILTER")
                        }
                    }
                    """
                )
                find_email_configs_result = await client.execute_async(find_email_configs_query)
                
                if len(find_email_configs_result['objects']) == 0:
                    logger.info("🟠 No email delivery configuations provisioned.")
                    continue
                
                for email_config in find_email_configs_result['objects']:
                    
                    logger.debug(email_config)
                    
                    if len(email_config['tagsFilter']) > 0:
                        if all([config_tag in event['tags'] for config_tag in email_config['tagsFilter']]) == False:
                            logger.info("🟠 Notification filtered out. It does not contain all tags in {}.".format(email_config['tagsFilter']))
                            continue
                    
                    # Build email
                    msg_content = event['message']
                    msg = EmailMessage()
                    msg.set_content(msg_content)
                    msg['Subject'] = email_config['subject'] + " " + " ".join(event['tags'])
                    msg['From'] = email_config['from']
                    msg['To'] = user['mEmail'] 
                    
                    # Attempt delivery
                    try: 
                        if any([email_config[k] is None for k in email_config]):
                            raise Exception('🔴 Invalid email delivery configuration {}/{}'.format(email_config['name'], email_config['id']))
                        
                        server = smtplib.SMTP_SSL(email_config['smtp'], email_config['port'])
                        server.login(email_config['address'], email_config['token'])
                        server.send_message(msg)
                        server.quit()
                        error_message = ""
                        delivery_status = "delivered: true"
                        
                        mutation_create_notification_delivery = gql(
                        """
                        mutation{{
                            createNotificationDelivery(input: {{
                                notificationDelivery: {{
                                user: "{}"
                                userLogin: "{}"
                                notificationId: "{}"
                                {}
                                {}
                                deliveryPath: "Email"
                                message: "{}"
                                deliveryConfigId: "{}"
                                }}
                            }}){{    
                                notificationDelivery {{
                                id
                                }}
                            }}
                            }}
                        """.format(user['id'], user['login'], event['id'], delivery_status, error_message, msg_content, email_config['id'])
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
                                    {}
                                    {}
                                    deliveryPath: "Email"
                                    message: "{}"
                                    deliveryConfigId: "{}"
                                    }}
                                }}){{    
                                    notificationDelivery {{
                                    id
                                    }}
                                }}
                                }}
                            """.format(user['id'], user['login'], event['id'], delivery_status, error_message, msg_content, email_config['id'])
                        )
                        
                    create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)   
                
                    if delivery_status == "delivered: false" and email_config['status'] != False:
                        optionsArrayPayload = "[{ groupName: \"HealthCheck\", property: \"Status\", value: false}]"
                        mutation_update_status = gql(
                        """
                        mutation UpdateStatus {{
                        updateObjectPropertiesByName(input: {{
                            objectId: "{}"
                            transactionId: "{}"
                            propertiesArray: {}
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(email_config['id'], round(time.time() * 1000), optionsArrayPayload)
                        )
                        mutation_update_status_result = await client.execute_async(mutation_update_status)
                        logger.info("🔴 Configuration {} status is updated to {}.".format(email_config['name'], "error")) 
                    elif delivery_status == "delivered: true" and email_config['status'] != True:
                        optionsArrayPayload = "[{ groupName: \"HealthCheck\", property: \"Status\", value: true}]"
                        mutation_update_status = gql(
                        """
                        mutation UpdateStatus {{
                        updateObjectPropertiesByName(input: {{
                            objectId: "{}"
                            transactionId: "{}"
                            propertiesArray: {}
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(email_config['id'], round(time.time() * 1000), optionsArrayPayload)
                        )
                        mutation_update_status_result = await client.execute_async(mutation_update_status)
                        logger.info("🟢 Configuration {} status is updated to {}.".format(email_config['name'], "operational"))
                
    except Exception as e:
        logger.info("🔴 Could not deliver notification {} to {} error {}.".format(event['id'], user['login'], e)) 

async def notification_delivery_twilio_worker(event):
    """ Notification delivery for SMS and WhatsApp via Twilio
    
    Send notifications through SMS and WhatsApp using the Twilio API to eligible users of the platform. Creates a notification delivery to report on the event.
    
    Args:
    =====
        event: json object
            A notification event.
            
    Returns:
    ========
        none
    
    """
    try:
        
        logger.debug(event)
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        # GQL query to get user data
        query_users = gql(
        """
        query GetUsers {
            users   {
                        id
                        login
                        enabled
                        mName
                        mEmail
                        mPhone
                        mTags
                        activated
                        userProfiles(filter: {
                            object: {
                                schemaTags: {
                                    contains: ["user profile"]
                                }
                            }
                            }){
                            id
                            object {
                                id
                                name
                                schemaTags
                                tags
                                generalNotificationsMode: property(propertyName: "Notifications/GENERAL")
                                receiveNotificationsViaEmail: property(propertyName: "Notifications/Email notifications")
                                receiveNotificationsViaSms: property(propertyName: "Notifications/SMS notifications")
                                receiveNotificationsViaWhatsApp: property(propertyName: "Notifications/WhatsApp notifications")
                                appName: property(propertyName: "Program/Name")
                            }
                        }    
                    }
                }
        """
        )
        users = await client.execute_async(query_users)
        
        # Skip delivery if there are no users on the platform
        if len(users['users']) == 0:
            raise Exception("🟠 No users found on the platform.")
        
        for user in users['users']:
            
            # Skip delivery if the user is not activated
            if user['activated'] == False:
                logger.debug("🟠 User is not activated: {}".format(user['login']))
                continue
            # Skip delivery if the user is not enabled
            if user['enabled'] == False:
                logger.debug("🟠 User is not enabled: {}".format(user['login']))
                continue
            # Check phone
            if user['mPhone'] == None:
                logger.debug("🟠 User has no phone number: {}".format(user['login']))
                continue
            # Check profile
            if len(user['userProfiles']) == 0:
                logger.debug("🟠 User has no valid profile: {}".format(user['login']))
                continue
            
            for profile in user['userProfiles']:
                
                # Skip delivery if the user has muted notifications
                if profile['object']['generalNotificationsMode'] == 'Muted':
                    logger.debug("🟠 User opted out of all notifications: {}".format(user['login']))
                    continue
                # Skip delivery if user opted out of email
                if profile['object']['receiveNotificationsViaSms'] == False:
                    logger.debug("🟠 User opted out of SMS notifications: {}".format(user['login']))
                    continue
                
                # Find all the email delivery configurations
                find_twilio_configs_query = gql(
                    """
                    query FindEmailConfigs{
                        objects(filter: {
                            schemaTags: {
                                equalTo: ["application", "dispatcher", "notification", "twilio", "configuration"]
                            }
                        }) {
                            id
                            name
                            schemaTags
                            accountSid: property(propertyName: "Credentials/ACCOUNT_SID") 
                            authToken: property(propertyName: "Credentials/AUTH_TOKEN")
                            from: property(propertyName: "Settings/FROM")
                            whatsapp: property(propertyName: "Settings/WHATSAPP")
                            status: property(propertyName: "HealthCheck/Status")
                            tagsFilter: property(propertyName: "Notifications filter/TAGS_FILTER")
                        }
                    }
                    """
                )
                find_twilio_configs_result = await client.execute_async(find_twilio_configs_query)
                
                if len(find_twilio_configs_result['objects']) == 0:
                    logger.info("🟠 No Twilio delivery configuations provisioned.")
                    continue
                
                for twilio_config in find_twilio_configs_result['objects']:
                    
                    logger.debug(twilio_config)
                    
                    if len(twilio_config['tagsFilter']) > 0:
                        if all([config_tag in event['tags'] for config_tag in twilio_config['tagsFilter']]) == False:
                            logger.info("🟠 Notification filtered out. It does not contain all tags in {}.".format(twilio_config['tagsFilter']))
                            continue
                    
                    # Attempt delivery
                    try: 
                        if any([twilio_config[k] is None for k in twilio_config]):
                            raise Exception('🔴 Invalid Twilio delivery configuration {}/{}'.format(twilio_config['name'], twilio_config['id']))
                        
                        client = twilio.rest.Client(twilio_config['accountSid'], twilio_config['authToken'])
                        
                        try:
                            message = client.messages.create(
                                to="{}{}".format('whatsapp:' if twilio_config['whatsapp'] else '', user['mPhone']), 
                                from_="{}{}".format('whatsapp:' if twilio_config['whatsapp'] else '', twilio_config['from']),
                                body=event['message'])
                        except TwilioRestException as e:
                            raise Exception('🔴 Twilio delivery configuration {}/{} error {}'.format(twilio_config['name'], twilio_config['id'], e))
                        
                        if message.sid['error_message'] is None:
                            error_message = ""
                            delivery_status = "delivered: true"
                            
                            mutation_create_notification_delivery = gql(
                            """
                            mutation{{
                                createNotificationDelivery(input: {{
                                    notificationDelivery: {{
                                    user: "{}"
                                    userLogin: "{}"
                                    notificationId: "{}"
                                    {}
                                    {}
                                    deliveryPath: "Email"
                                    message: "{}"
                                    deliveryConfigId: "{}"
                                    }}
                                }}){{    
                                    notificationDelivery {{
                                    id
                                    }}
                                }}
                                }}
                            """.format(user['id'], user['login'], event['id'], delivery_status, error_message, event['message'], twilio_config['id'])
                            )
                        else:
                            # Build error message if the delivery failed
                            error_message = "error: \"" + message.sid['error_code'] + " "+ message.sid['error_message'] + "\""
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
                                        {}
                                        {}
                                        deliveryPath: "Email"
                                        message: "{}"
                                        deliveryConfigId: "{}"
                                        }}
                                    }}){{    
                                        notificationDelivery {{
                                        id
                                        }}
                                    }}
                                    }}
                                """.format(user['id'], user['login'], event['id'], delivery_status, error_message, event['message'], twilio_config['id'])
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
                                    {}
                                    {}
                                    deliveryPath: "Email"
                                    message: "{}"
                                    deliveryConfigId: "{}"
                                    }}
                                }}){{    
                                    notificationDelivery {{
                                    id
                                    }}
                                }}
                                }}
                            """.format(user['id'], user['login'], event['id'], delivery_status, error_message, event['message'], twilio_config['id'])
                        )
                        
                    create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)   
                
                    if delivery_status == "delivered: false" and twilio_config['status'] != False:
                        optionsArrayPayload = "[{ groupName: \"HealthCheck\", property: \"Status\", value: false}]"
                        mutation_update_status = gql(
                        """
                        mutation UpdateStatus {{
                        updateObjectPropertiesByName(input: {{
                            objectId: "{}"
                            transactionId: "{}"
                            propertiesArray: {}
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(twilio_config['id'], round(time.time() * 1000), optionsArrayPayload)
                        )
                        mutation_update_status_result = await client.execute_async(mutation_update_status)
                        logger.info("🔴 Configuration {} status is updated to {}.".format(twilio_config['name'], "error")) 
                    elif delivery_status == "delivered: true" and twilio_config['status'] != True:
                        optionsArrayPayload = "[{ groupName: \"HealthCheck\", property: \"Status\", value: true}]"
                        mutation_update_status = gql(
                        """
                        mutation UpdateStatus {{
                        updateObjectPropertiesByName(input: {{
                            objectId: "{}"
                            transactionId: "{}"
                            propertiesArray: {}
                            }}){{
                                boolean
                            }}
                        }}
                        """.format(twilio_config['id'], round(time.time() * 1000), optionsArrayPayload)
                        )
                        mutation_update_status_result = await client.execute_async(mutation_update_status)
                        logger.info("🟢 Configuration {} status is updated to {}.".format(twilio_config['name'], "operational"))
                
    except Exception as e:
        logger.info("🔴 Could not deliver notification {} to {} error {}.".format(event['id'], user['login'], e)) 

@backoff.on_exception(backoff.expo, Exception)
async def notification_delivery_subscription():
    """ Subscribes on notification events
    
    Listens for notification events and dispatch them through to the different workers representing the delivery paths.
    
    Args:
    =====
        none
        
    Returns:
    ========
        none

    """
    try:
        # Grab the async event loop
        loop = asyncio.get_event_loop()
        
        # GQL parameters
        gql_address = config['gql']['address']
        jwt_env_key = "JWT_ENV_DISPATCHER"
        jwt = os.environ[jwt_env_key]
        
        # GQL client
        transport = WebsocketsTransport(url="ws://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt}, keep_alive_timeout=300)
        async with Client(
            transport=transport, fetch_schema_from_transport=False,
        ) as session:
            subscription = gql(
                """
                subscription onNotification{
                    Notifications{
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
            
            logger.info("👂 Listening on {} for {}".format("notifications", "deliveries"))
            
            # Send system alarm notifications to be delivered via email
            async for event in session.subscribe(subscription):
                #logger.debug(event)
                if event['Notifications']['relatedNode'] is not None:
                    loop.create_task(notification_delivery_app_worker(event['Notifications']['relatedNode']))
                    loop.create_task(notification_delivery_email_worker(event['Notifications']['relatedNode']))
                    loop.create_task(notification_delivery_twilio_worker(event['Notifications']['relatedNode']))
                # Dispatch to the appropriate worker based on tags
                #if all([t in event['Notifications']['tags'] for t in ['application', 'monitor'] ]):
                #    logger.debug("Process monitor app notification delivery")
                    # event['notifications']['relatedNode]
                #if event['Notifications']['relatedNode']['to'] == None:
                #    loop.create_task(admin_system_alarm_email_worker(event['Notifications']['relatedNode']))
                
    except Exception as e:
        logger.error(e)