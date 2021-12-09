import asyncio
import backoff
from email.message import EmailMessage
from envyaml import EnvYAML
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import json
import logging
import logging.config 
import pandas as pd
import smtplib
import yaml

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("admin")

# Mapping of naming conventions
admin_device_dict = {"BATTERY_LOW": "DEV_LOWBAT", "RESPONSE_STATUS": "DEV_STATUS"}

async def admin_mass_notification_email_worker(gql_address, jwt, event):
    """Send email notifications when a direct message notification is generated

        Specific to the admin application.
        Relies on email templates with hardcoded uuid.

        Parameters
        ----------
        gql_address : str
            GraphQL API URL
            
        jwt : str
            GraphQL access token.
            
        event : str
            Event received from a subscription.
            
        Raises
        ------
        No users found
            If no users are found the worker is terminated.
    """
    transport = AIOHTTPTransport(url="http://{}/graphql".format(gql_address), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    # Check that there is an addressee
    if event['spec'] != None:
        #raise Exception("Missing addressee ('to' spec field).")
        #logger.error("Missing addressee ('to' spec field).", exc_info=True)
        return 0
    # Get the admin users and profile ids to decide who should receive the notification
    mutation_admin_users = gql(
        """
        query{
                    getUsersAndProfiles(
                        application: "admin"
                    ){
                        usersProfiles {
                        userId
                        profileId
                        }
                    }
                }
        """
    )
    admin_users = await client.execute_async(mutation_admin_users)
    logger.debug(admin_users)
    if admin_users['getUsersAndProfiles']['usersProfiles'] == None:
        raise Exception("No admin users were found.")
        logger.error("Admin email worker error.", exc_info=True)
        return 1
    
    # For each admin user find out if they should receive an email notification
    for userProfile in admin_users['getUsersAndProfiles']['usersProfiles']:
        query_user = gql(
        """
        query GetUser {{
            user(id: "{}"
                    ){{
                        id
                        login
                        mName
                        mPhone
                        mEmail
                    }}
                }}
        """.format(userProfile['userId'])
        )
        user = await client.execute_async(query_user)
        logger.debug(user)
        # Skip if the user has no email address
        if user['user']['mEmail'] == None:
            logger.debug("User has no email address: {}".format(user['user']))
            continue
        
        query_profile = gql(
            """
            query GetAdminProfile {{
                                        objectProperties(filter: {{
                                            objectId: {{equalTo: "{}"}}
                                        }}){{
                                            groupName
                                            property
                                            value
                                        }}
                                    }}
            """.format(userProfile['profileId'])
        )
        profile = await client.execute_async(query_profile)
        logger.debug(profile)
        # Skip if the user has an empty profile
        if profile['objectProperties'] == None:
            logger.debug("User has no profile: {}".format(user['user']))
            continue
        # TODO: Refactor using dataframe representation instead of loop
        # Default email object
        query_email_template = gql(
            """
            query{
            objectProperties( filter: {
            objectId: {equalTo: "e43adb21-9117-4242-964e-0e3fbf7cbd31"}
            }){
                objectId
                groupName
                property
                value
                id
            }
            }
            """
        )
        email_template = await client.execute_async(query_email_template)
        logger.debug(email_template)
        # Raise an error if the email template is missing
        if email_template['objectProperties'] == None:
            raise Exception("Email template missing.")
            logger.error("Admin email worker error.", exc_info=True)
            return 0
        
        # Convert the list of email properties to a dataframe
        email_df = pd.DataFrame(email_template['objectProperties'])
        # Build email
        msg_content = "{}".format(event['message'])#"Message from {}: {}".format(json.loads(event['actorName'], event['message']))
        msg = EmailMessage()
        msg.set_content(msg_content)
        msg['Subject'] = email_df[email_df["property"] == "SUBJECT"][['value']].values[0][0]
        msg['From'] = email_df[email_df["property"] == "FROM"][['value']].values[0][0]
        msg['To'] = user['user']['mEmail'] 
        logger.debug(msg)
        # Send email
        try:
            server = smtplib.SMTP_SSL(email_df[email_df["property"] == "SMTP"][['value']].values[0][0],email_df[email_df["property"] == "PORT"][['value']].values[0][0] )
            server.login(email_df[email_df["property"] == "ADDRESS"][['value']].values[0][0], email_df[email_df["property"] == "TOKEN"][['value']].values[0][0])
            server.send_message(msg)
            server.quit()
            error_message = ""
            delivery_status = "delivered: true"
            # Report
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
                """.format(user['user']['id'], user['user']['login'], event['id'], delivery_status, error_message, msg_content, email_df['objectId'].values[0])
            )
        except Exception as e:
            #logger.error(e)
            #error_code = e.smtp_code
            error_message = "error: \"" + str(e) + "\""
            #error_message = "error: \"" + str(e) + "\""
            delivery_status = "delivered: false"
            # Report
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
                """.format(user['user']['id'], user['user']['login'], event['id'], delivery_status, error_message, msg_content, email_df['objectId'].values[0])
            )
        create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)
        logger.debug("Notification delivery created: {}".format(create_notification_delivery))            
    return 0

@backoff.on_exception(backoff.expo, Exception)
async def admin_mass_notification_email_subscription():
    return 0