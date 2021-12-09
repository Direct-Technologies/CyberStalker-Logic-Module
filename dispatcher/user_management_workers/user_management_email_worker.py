import logging
import logging.config 
import yaml
from envyaml import EnvYAML
import sys
import asyncio
import smtplib
import pandas as pd
from email.message import EmailMessage
import json
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport

# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml") 
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("user_management")

async def user_management_email_worker(gql_address, jwt, event):
    """Send emails for user management actions
    
        Responsible to send onboarding and password reset emails.
        Relies on email templates with hardcoded uuid.

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
    
    # For user onboarding actions
    if event['actorType'] == 'USER' and json.loads(event['message'])['type'] == 'USER_ONBOARDING':
        query_onboarding_email_template = gql(
                    """
                    query{
                    objectProperties( filter: {
                    objectId: {equalTo: "9fc15a72-42fe-45c4-892e-b5f9349cf6ac"}
                    }){
                        objectId
                        groupName
                        property
                        value
                        id
                        flag
                        type
                        tags
                        
                    }
                    }
                    """
                )
        email_template = await client.execute_async(query_onboarding_email_template)
        
        # Raise error if the email template is missing
        if email_template['objectProperties'] == None:
            raise Exception("Email template missing.")
            logger.error("Email worker error.", exc_info=True)
            return 0
        
        # Convert the list of email properties to a dataframe
        email_df = pd.DataFrame(email_template['objectProperties'])
        # Build email
        msg_content = "Follow the link to activate you account."
        msg = EmailMessage()
        msg.set_content(msg_content)
        msg['Subject'] = email_df[email_df["property"] == "SUBJECT"][['value']].values[0][0]
        msg['From'] = email_df[email_df["property"] == "FROM"][['value']].values[0][0]
        msg['To'] = json.loads(event['message'])['email'] 
        # Send email
        try:
            server = smtplib.SMTP_SSL(email_df[email_df["property"] == "SMTP"][['value']].values[0][0],email_df[email_df["property"] == "PORT"][['value']].values[0][0] )
            server.login(email_df[email_df["property"] == "ADDRESS"][['value']].values[0][0], email_df[email_df["property"] == "TOKEN"][['value']].values[0][0])
            server.send_message(msg)
            server.quit()
            error_message = ""
            delivery_status = "delivered: true"
        except Exception as e:
            error_code = e.smtp_code
            error_message = "error: \"" + e.smtp_error.decode("utf-8") + "\""
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
            """.format(json.loads(event['message'])['id'], json.loads(event['message'])['login'], event['id'], delivery_status, error_message, msg_content, email_df['objectId'].values[0])
        )
        create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)
        # DEBUG
        #print(create_notification_delivery)
    # For user password reset actions
    elif event['actorType'] == 'USER' and json.loads(event['message'])['type'] == 'USER_PASSWORD_RESET':
        query_password_reset_email_template = gql(
                """
                query{
                objectProperties( filter: {
                objectId: {equalTo: "122c302b-cd9b-4ba1-969d-34c6cbf17184"}
                }){
                    objectId
                    groupName
                    property
                    value
                    id
                    flag
                    type
                    tags
                    
                }
                }
                """
            )
        email_template = await client.execute_async(query_password_reset_email_template)
        
        # Raise error if the email template is missing
        if email_template['objectProperties'] == None:
            raise Exception("Email template missing.")
            logger.error("Email worker error.", exc_info=True)
            return 0
        
        # Convert the list of email properties to a dataframe
        email_df = pd.DataFrame(email_template['objectProperties'])
        # Build email
        msg_content = "Follow the link to activate you account."
        msg = EmailMessage()
        msg.set_content(msg_content)
        msg['Subject'] = email_df[email_df["property"] == "SUBJECT"][['value']].values[0][0]
        msg['From'] = email_df[email_df["property"] == "FROM"][['value']].values[0][0]
        msg['To'] = json.loads(event['message'])['email']
        # Send email
        try:
            server = smtplib.SMTP_SSL(email_df[email_df["property"] == "SMTP"][['value']].values[0][0],email_df[email_df["property"] == "PORT"][['value']].values[0][0] )
            server.login(email_df[email_df["property"] == "ADDRESS"][['value']].values[0][0], email_df[email_df["property"] == "TOKEN"][['value']].values[0][0])
            server.send_message(msg)
            server.quit()
            error_message = ""
            delivery_status = "delivered: true"
        except Exception as e:
            error_code = e.smtp_code
            error_message = "error: \"" + e.smtp_error.decode("utf-8") + "\""
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
            """.format(json.loads(event['message'])['id'], json.loads(event['message'])['login'], event['id'], delivery_status, error_message, msg_content, email_df['objectId'].values[0])
        )
        create_notification_delivery = await client.execute_async(mutation_create_notification_delivery)
        logger.debug("Notification delivery created: {}".format(create_notification_delivery))
    return 0

async def user_management_email_subscription():
    return 0