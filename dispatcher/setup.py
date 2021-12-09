import logging
import logging.config 
import glob
import string
import yaml
from envyaml import EnvYAML
import json
import sys
import asyncio
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.websockets import WebsocketsTransport
import authenticate as auth
import regex

# Load dispatcher config
# EnvYAML necessary to parse environment vairables for the configuration file
config = EnvYAML("./docs/conf.yaml")
# Logging
logging.config.dictConfig(config["logging"])
logger = logging.getLogger("setup")

def setup_dispatcher_module(config):
    """Setup / sync the schemas and objects for the dispatcher module

        The dispatcher module relies on the existence of a set of schemas and objects
        with fixed uuids provisioned in the platform. This function makes sure those objects
        are there and creates them if necessary.

        Parameters
        ----------
        config : json
            Configuration file for the dispatcher.
    """
    
    # 1. Use the configuration file to access GraphQL as the application
    jwt = auth.authenticate(config['gql']['address'], config['apps']['dispatcher']['user'], config['apps']['dispatcher']['password'], "")
    transport = AIOHTTPTransport(url="http://{}/graphql".format(config['gql']['address']), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    # 2. Go through the schemas in the module schemas folder
    schemas = glob.glob("./docs/dispatcher_templates/schemas/*.json")
    for schema in schemas:
        logger.info("Import schema {}".format(schema))
        with open(schema, 'r') as stream:
            #schema_template = yaml.safe_load(stream)
            #jsonSchema = "{}".format(json.dumps(schema_template).replace('"',"\\\""))
            jsonSchema = "{}".format(json.dumps(stream.read()).replace('"',"\""))
            #jsonSchema = json.dumps(json.loads(stream.read()))
            #logger.info(jsonSchema)
            # re.sub(r'"(.*?)"(?=:)', r'\1', json_data)
            # replaceAll("\"(\\w+)\"(\\s*:\\s*)", "$1$2")
            # logger.info(re.sub(r'"(.*?)"(?=:)', r'\1', jsonSchema))
            
            #logger.info(regex.sub(r'(?<=, |{)"(.*?)"(?=: )', r'\1', jsonSchema))
            
            query_import_schema = gql(
                """
                mutation {{
                    importSchema(input: {{
                        jsonSchema: {}
                        
                    }}) {{
                        uuid
                    }}
                }}
                """.format(jsonSchema)
            )
            result_import_schema = client.execute(query_import_schema)
            
    #3. Deal with copies
    copy_schemas = glob.glob("./docs/dispatcher_templates/schemas/*.copy")
    for schema in copy_schemas:
        logger.info("Import schema {}".format(schema))
        with open(schema, 'r') as stream:
            #schema_template = yaml.safe_load(stream)
            #jsonSchema = "{}".format(json.dumps(schema_template).replace('"',"\\\""))
            
            original_schema = json.loads(stream.read())
            
            #logger.info("Original schema")
            #logger.info(original_schema)
            
            del original_schema['schema']['id']
            
            jsonSchema = "{}".format(original_schema).replace('"', '\\\\\\\"').replace("'", '\\"').replace('None', 'null').replace('True', 'true').replace('False', 'false')
            
            #logger.info("Modified schema")
            #logger.info(jsonSchema)
            
            copy_schema = original_schema
            #del copy_schema['schema']['name']
            m_tags = copy_schema['schema']['m_tags']
            del m_tags[-1]
            copy_schema['schema']['m_tags'] = m_tags
            #copySchema = "{}".format(copy_schema).replace('"', '\\\"').replace("'", '\\"').replace('None', 'null').replace('True', 'true').replace('False', 'false')
            copySchema = "{}".format(copy_schema).replace('"', '\\\\\\\"').replace("'", '\\"').replace('None', 'null').replace('True', 'true').replace('False', 'false')
            
            #logger.info("Copied schema")
            #logger.info(copySchema)
            
            # Old version
            #jsonSchema = "{}".format(json.dumps(original_schema).replace('\"',"\\\""))
            
            logger.info("Importing original...")
            query_import_schema = gql(
                """
                mutation {{
                    importSchema(input: {{
                        jsonSchema: "{}"
                        
                    }}) {{
                        uuid
                    }}
                }}
                """.format(jsonSchema)
            )
            result_import_schema = client.execute(query_import_schema)
            
            logger.info("Importing copy...")
            query_import_schema = gql(
                """
                mutation {{
                    importSchema(input: {{
                        jsonSchema: "{}"
                        updateOnly: true
                    }}) {{
                        uuid
                    }}
                }}
                """.format(copySchema)
            )
            result_import_schema = client.execute(query_import_schema)
    
    # 3. Go through the objects in the module objects folder
    # objects = glob.glob("./docs/dispatcher_templates/objects/*.yaml")
    # for obj in objects:
    #     uuid = obj.split("/")[-1].split(".")[0]
    #     query_object = gql(
    #         """
    #         query {{
    #             object(id: "{}"){{
    #                 id
    #             }}
    #         }}
    #         """.format(uuid)
    #     )
    #     result = client.execute(query_object)
    #     if result['object'] == None:
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object =  gql(
    #             """
    #             query {{
    #                 object(id: "{}"){{
    #                     id
    #                 }}
    #             }}
    #             """.format(uuid)
    #         )
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object = gql(
    #             """
    #             mutation cObj {{
    #                 createObject(input: {{
    #                     object: {{
    #                     id: "{}"
    #                     name: "{}"
    #                     enabled: true
    #                     description: "{}"
    #                     editorgroup: "{}"
    #                     usergroup: "{}"
    #                     readergroup: "{}"
    #                     schemaId: "{}"
    #                     tags: {}
    #                     }}
    #                 }}){{
    #                     object {{
    #                     id
    #                     }}
    #                 }}
    #             }}
    #             """.format(object_template['id'], object_template['name'], object_template['description'], object_template['editorgroup'], object_template['usergroup'], object_template['readergroup'], object_template['schemaId'], json.dumps(object_template['tags']))
    #         )
    #         result_create_object = client.execute(query_create_object)
    return 0

def setup_user_management_module(config):
    """Setup / sync the schemas and objects for the user management module

        The user management module relies on the existence of a set of schemas and objects
        with fixed uuids provisioned in the platform. This function makes sure those objects
        are there and creates them if necessary.

        Parameters
        ----------
        config : json
            Configuration file for the dispatcher.
    """
    
    # 1. Use the configuration file to access GraphQL as the application
    jwt = auth.authenticate(config['gql']['address'], config['apps']['user_management']['user'], config['apps']['user_management']['password'], "")
    transport = AIOHTTPTransport(url="http://{}/graphql".format(config['gql']['address']), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    # 2. Go through the schemas in the module schemas folder
    schemas = glob.glob("./docs/user_management_templates/schemas/*.json")
    for schema in schemas:
        logger.info("Import schema {}".format(schema))
        with open(schema, 'r') as stream:
            #schema_template = yaml.safe_load(stream)
            #jsonSchema = "{}".format(json.dumps(schema_template).replace('"',"\\\""))
            jsonSchema = "{}".format(json.dumps(stream.read()).replace('"',"\""))
            query_import_schema = gql(
                """
                mutation {{
                    importSchema(input: {{
                        jsonSchema: {}
                        
                    }}) {{
                        uuid
                    }}
                }}
                """.format(jsonSchema)
            )
            result_import_schema = client.execute(query_import_schema)
        
    # 3. Go through the objects in the module objects folder
    # objects = glob.glob("./docs/user_management_templates/objects/*.yaml")
    # for obj in objects:
    #     uuid = obj.split("/")[-1].split(".")[0]
    #     query_object = gql(
    #         """
    #         query {{
    #             object(id: "{}"){{
    #                 id
    #             }}
    #         }}
    #         """.format(uuid)
    #     )
    #     result = client.execute(query_object)
    #     if result['object'] == None:
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object =  gql(
    #             """
    #             query {{
    #                 object(id: "{}"){{
    #                     id
    #                 }}
    #             }}
    #             """.format(uuid)
    #         )
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object = gql(
    #             """
    #             mutation cObj {{
    #                 createObject(input: {{
    #                     object: {{
    #                     id: "{}"
    #                     name: "{}"
    #                     enabled: true
    #                     description: "{}"
    #                     editorgroup: "{}"
    #                     usergroup: "{}"
    #                     readergroup: "{}"
    #                     schemaId: "{}"
    #                     tags: {}
    #                     }}
    #                 }}){{
    #                     object {{
    #                     id
    #                     }}
    #                 }}
    #             }}
    #             """.format(object_template['id'], object_template['name'], object_template['description'], object_template['editorgroup'], object_template['usergroup'], object_template['readergroup'], object_template['schemaId'], json.dumps(object_template['tags']))
    #         )
    #         result_create_object = client.execute(query_create_object)
    return 0
    
def setup_admin_module(config):
    """Setup / sync the schemas and objects for the admin module

        The user management module relies on the existence of a set of schemas and objects
        with fixed uuids provisioned in the platform. This function makes sure those objects
        are there and creates them if necessary.

        Parameters
        ----------
        config : json
            Configuration file for the dispatcher.
    """
    
    # 1. Use the configuration file to access GraphQL as the application
    jwt = auth.authenticate(config['gql']['address'], config['apps']['admin']['user'], config['apps']['admin']['password'], "")
    transport = AIOHTTPTransport(url="http://{}/graphql".format(config['gql']['address']), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    # 2. Go through the schemas in the module schemas folder
    schemas = glob.glob("./docs/admin_templates/schemas/*.json")
    for schema in schemas:
        logger.info("Import schema {}".format(schema))
        with open(schema, 'r') as stream:
            #schema_template = yaml.safe_load(stream)
            #jsonSchema = "{}".format(json.dumps(schema_template).replace('"',"\\\""))
            jsonSchema = "{}".format(json.dumps(stream.read()).replace('"',"\""))
            query_import_schema = gql(
                """
                mutation {{
                    importSchema(input: {{
                        jsonSchema: {}
                        
                    }}) {{
                        uuid
                    }}
                }}
                """.format(jsonSchema)
            )
            result_import_schema = client.execute(query_import_schema)
        
    # # 3. Go through the objects in the module objects folder
    # objects = glob.glob("./docs/admin_templates/objects/*.yaml")
    # for obj in objects:
    #     uuid = obj.split("/")[-1].split(".")[0]
    #     query_object = gql(
    #         """
    #         query {{
    #             object(id: "{}"){{
    #                 id
    #             }}
    #         }}
    #         """.format(uuid)
    #     )
    #     result = client.execute(query_object)
    #     if result['object'] == None:
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object =  gql(
    #             """
    #             query {{
    #                 object(id: "{}"){{
    #                     id
    #                 }}
    #             }}
    #             """.format(uuid)
    #         )
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object = gql(
    #             """
    #             mutation cObj {{
    #                 createObject(input: {{
    #                     object: {{
    #                     id: "{}"
    #                     name: "{}"
    #                     enabled: true
    #                     description: "{}"
    #                     editorgroup: "{}"
    #                     usergroup: "{}"
    #                     readergroup: "{}"
    #                     schemaId: "{}"
    #                     tags: {}
    #                     }}
    #                 }}){{
    #                     object {{
    #                     id
    #                     }}
    #                 }}
    #             }}
    #             """.format(object_template['id'], object_template['name'], object_template['description'], object_template['editorgroup'], object_template['usergroup'], object_template['readergroup'], object_template['schemaId'], json.dumps(object_template['tags']))
    #         )
    #         result_create_object = client.execute(query_create_object)
    return 0

def setup_board_module(config):
    """Setup / sync the schemas and objects for the board module

        Parameters
        ----------
        config : json
            Configuration file for the dispatcher.
    """
    
    # 1. Use the configuration file to access GraphQL as the application
    jwt = auth.authenticate(config['gql']['address'], config['apps']['board']['user'], config['apps']['board']['password'], "")
    transport = AIOHTTPTransport(url="http://{}/graphql".format(config['gql']['address']), headers={'Authorization': 'Bearer ' + jwt})
    client = Client(transport=transport, fetch_schema_from_transport=False)
    
    # 2. Go through the schemas in the module schemas folder
    schemas = glob.glob("./docs/board_templates/schemas/*.json")
    for schema in schemas:
        logger.info("Import schema {}".format(schema))
        with open(schema, 'r') as stream:
            #schema_template = yaml.safe_load(stream)
            #jsonSchema = "{}".format(json.dumps(schema_template).replace('"',"\\\""))
            jsonSchema = "{}".format(json.dumps(stream.read()).replace('"',"\""))
            query_import_schema = gql(
                """
                mutation {{
                    importSchema(input: {{
                        jsonSchema: {}
                        
                    }}) {{
                        uuid
                    }}
                }}
                """.format(jsonSchema)
            )
            result_import_schema = client.execute(query_import_schema)
        
    # 3. Go through the objects in the module objects folder
    # objects = glob.glob("./docs/board_templates/objects/*.yaml")
    # for obj in objects:
    #     uuid = obj.split("/")[-1].split(".")[0]
    #     query_object = gql(
    #         """
    #         query {{
    #             object(id: "{}"){{
    #                 id
    #             }}
    #         }}
    #         """.format(uuid)
    #     )
    #     result = client.execute(query_object)
    #     if result['object'] == None:
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object =  gql(
    #             """
    #             query {{
    #                 object(id: "{}"){{
    #                     id
    #                 }}
    #             }}
    #             """.format(uuid)
    #         )
    #         with open(obj, 'r') as stream:
    #             object_template = yaml.safe_load(stream)
    #         query_create_object = gql(
    #             """
    #             mutation cObj {{
    #                 createObject(input: {{
    #                     object: {{
    #                     id: "{}"
    #                     name: "{}"
    #                     enabled: true
    #                     description: "{}"
    #                     editorgroup: "{}"
    #                     usergroup: "{}"
    #                     readergroup: "{}"
    #                     schemaId: "{}"
    #                     tags: {}
    #                     }}
    #                 }}){{
    #                     object {{
    #                     id
    #                     }}
    #                 }}
    #             }}
    #             """.format(object_template['id'], object_template['name'], object_template['description'], object_template['editorgroup'], object_template['usergroup'], object_template['readergroup'], object_template['schemaId'], json.dumps(object_template['tags']))
    #         )
    #         result_create_object = client.execute(query_create_object)
    return 0

def check_pix_core_api(config):
    """...

        ...

        Parameters
        ----------
        config : json
            Configuration file for the dispatcher.
    """
    try:
        jwt = auth.authenticate(config['gql']['address'], config['apps']['dispatcher']['user'], config['apps']['dispatcher']['password'], "")
        transport = AIOHTTPTransport(url="http://{}/graphql".format(config['gql']['address']), headers={'Authorization': 'Bearer ' + jwt})
        client = Client(transport=transport, fetch_schema_from_transport=False)
        
        query_check_api = gql(
            """
            query getVersion{
                getVersion{
                    short
                    long
                }
            }
            """
        )
        result_check_api = client.execute(query_check_api)
        
        logger.info("🟢 Connected to {}".format(result_check_api['getVersion']['long']))
        
    except Exception as e:
        logger.info("🔴 Cannot reach pix-core-api:")
        logger.error("{}".format(e))
        sys.exit(1)

if __name__ == "__main__":
    logger.info("Check pix-core-api")
    check_pix_core_api(config)
    logger.info("Initialize Dispatcher module")
    setup_dispatcher_module(config)
    logger.info("Initialize User Management module")
    setup_user_management_module(config)
    logger.info("Initialize Admin module")
    setup_admin_module(config)
    logger.info("Initialize Board module")
    setup_board_module(config)
    logger.info("Dispatcher setup complete")