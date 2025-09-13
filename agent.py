from strands import Agent, tool
from strands.models.openai import OpenAIModel
from strands_tools import calculator, current_time
import os
from dotenv import load_dotenv
import requests
from typing import Optional, Dict
import boto3
import logging
import traceback
# Add this import at the top
from decimal import Decimal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('insurance_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# API KEYS
load_dotenv()
COHERE_API_KEY = os.getenv("COHERE_API_KEY")

def get_federato_token():
    """Get authentication token from Federato API"""
    url = "https://product-federato.us.auth0.com/oauth/token"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "client_id": "7IjhreW9OQLKqYw5POfVAYvbuIoMd08S",
        "client_secret": "tWVDHGDOKx2Gw0izNqXdLh0ISd-7oHViTZ0uzGEXRdTamiXVE5dYZuN6-yRpiZK3",
        "audience": "https://product.federato.ai/core-api",
        "grant_type": "client_credentials"
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            token_data = response.json()
            return token_data.get("access_token")
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Network error getting token: {e}")

@tool
def read_underwriting_rules(file_path: str = "rules.txt") -> Optional[str]:
    """Read underwriting rules from a text file"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        return content
    except FileNotFoundError:
        return f"Error: File '{file_path}' not found. Please make sure the file exists."
    except Exception as e:
        return f"Error reading rules file: {e}"

def get_all_policies() -> Dict:
    """
    Fetches policies from the Federato API and returns the first policy only.
    """
    try:
        token = get_federato_token()
        url = "https://product.federato.ai/integrations-api/handlers/all-pollicies?outputOnly=true"
        headers = {"Authorization": f"Bearer {token}"}

        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"Full API response structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        
        # Debug the response structure
        if "output" in data:
            print(f" Output length: {len(data['output'])}")
            if len(data["output"]) > 0:
                first_item = data["output"][0]
                print(f" First output item keys: {list(first_item.keys()) if isinstance(first_item, dict) else type(first_item)}")
                
                if "data" in first_item:
                    data_field = first_item["data"]
                    print(f" Data field type: {type(data_field)}")
                    
                    # Check if data is an array of policies
                    if isinstance(data_field, list) and len(data_field) > 0:
                        policy = data_field[0]  # Get the first policy from the array
                        print(f" First policy keys: {list(policy.keys()) if isinstance(policy, dict) else type(policy)}")
                        print(f" Policy ID: {policy.get('id', 'NOT FOUND')}")
                        return policy
                    elif isinstance(data_field, dict):
                        # If data is a single policy object
                        print(f" Policy keys: {list(data_field.keys())}")
                        print(f" Policy ID: {data_field.get('id', 'NOT FOUND')}")
                        return data_field
                    else:
                        return {"error": f"Unexpected data field type: {type(data_field)}"}
                else:
                    # Maybe the policy data is directly in the output item
                    print(f" No 'data' field, trying direct access")
                    return first_item
            else:
                return {"error": "No policies found in API response - output array is empty"}
        else:
            return {"error": "No 'output' field found in API response"}
            
    except Exception as e:
        print(f"Exception in get_all_policies: {str(e)}")
        return {"error": f"Failed to fetch policies: {str(e)}"}
     
@tool
def get_and_save_all_policies_to_db(table_name: str = 'unpolishedData') -> str:
    """Fetch ALL policies from Federato API and save to DynamoDB"""
    try:
        token = get_federato_token()
        url = "https://product.federato.ai/integrations-api/handlers/all-pollicies?outputOnly=true"
        headers = {"Authorization": f"Bearer {token}"}

        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Extract all policies from the response
        if "output" in data and len(data["output"]) > 0:
            first_item = data["output"][0]
            if "data" in first_item and isinstance(first_item["data"], list):
                all_policies = first_item["data"]
                print(f"Found {len(all_policies)} policies to process")
            else:
                return "Error: Could not find policies array in API response"
        else:
            return "Error: No output found in API response"

        # Set up DynamoDB connection
        session = boto3.Session(
            aws_access_key_id='fakeMyKeyId',
            aws_secret_access_key='fakeSecretAccessKey',
            region_name="us-west-2"
        )
        dynamodb = session.resource(
            'dynamodb',
            endpoint_url='http://localhost:8123'
        )

        # Check if table exists, create if not
        try:
            existing_tables = [t.name for t in dynamodb.tables.all()]
            if table_name not in existing_tables:
                table = dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
                    BillingMode='PAY_PER_REQUEST'
                )
                table.wait_until_exists()
            else:
                table = dynamodb.Table(table_name)
        except Exception as e:
            return f"Error with DynamoDB table operations: {e}"

        # Conversion function for DynamoDB compatibility
        def convert_floats_to_decimals(obj):
            if isinstance(obj, dict):
                return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats_to_decimals(v) for v in obj]
            elif isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, str):
                try:
                    if '.' in obj and obj.replace('.', '').replace('-', '').isdigit():
                        return Decimal(obj)
                except:
                    pass
                return obj
            else:
                return obj

        # Process each policy
        saved_count = 0
        errors = []
        
        for i, policy in enumerate(all_policies):
            try:
                # Get policy ID
                policy_id = str(policy.get('id', f'policy_{i}'))
                
                # Convert data types
                converted_policy = convert_floats_to_decimals(policy)
                item = {"id": policy_id, **converted_policy}
                
                # Ensure ID is string and not duplicated
                if 'id' in converted_policy:
                    item['id'] = policy_id
                
                # Save to DynamoDB
                table.put_item(Item=item)
                saved_count += 1
                print(f"Saved policy {policy_id} ({i+1}/{len(all_policies)})")
                
            except Exception as e:
                error_msg = f"Error saving policy {i+1}: {str(e)}"
                errors.append(error_msg)
                print(error_msg)

        # Return summary
        result_msg = f"Successfully saved {saved_count}/{len(all_policies)} policies to {table_name}"
        if errors:
            result_msg += f"\nErrors encountered: {len(errors)}"
            for error in errors[:3]:  # Show first 3 errors
                result_msg += f"\n- {error}"
            if len(errors) > 3:
                result_msg += f"\n- ... and {len(errors)-3} more errors"
        
        return result_msg

    except Exception as e:
        return f"Error processing policies: {str(e)}"

# Check if required environment variable is set
if not COHERE_API_KEY:
    logger.error("COHERE_API_KEY not found in environment variables")
    print(" Error: COHERE_API_KEY not found in environment variables")
    print("Please set your Cohere API key in your .env file")
    exit(1)

try:
    logger.info(" Initializing OpenAI model with Cohere API...")
    model = OpenAIModel(
        client_args={
            "api_key": COHERE_API_KEY,
            "base_url": "https://api.cohere.ai/compatibility/v1"
        },
        model_id="command-a-03-2025",
        params={
            "max_tokens": 1000
        }
    )

    logger.info(" Creating agent with tools...")
    agent = Agent(model=model, tools=[read_underwriting_rules, get_and_save_all_policies_to_db])

    print(" Insurance Underwriting Agent Ready!")
    print("Available tools:")
    print("- read_underwriting_rules: Read rules from a text file")
    print("- get_and_save_policies_to_db: Fetch and save policies from Federato API")
    print("\nYou can ask me to:")
    print("- Read underwriting rules from a file")
    print("- Fetch and save policy data")
    print("- Answer questions about insurance policies")
    print(f"\n Logs are being saved to: insurance_agent.log")
    
    logger.info(" Agent ready for user interaction")
    
    while True:
        user_input = input("\n Ask me something (or 'quit' to exit): ")
        if user_input.lower() in ['quit', 'exit', 'q']:
            logger.info(" User requested exit")
            print(" Goodbye!")
            break
        
        try:
            logger.info(f"  User input: {user_input}")
            response = agent(user_input)
            logger.info(f" Agent response length: {len(str(response))} characters")
            logger.debug(f"Agent response: {response}")
            print(f" {response}")
        except Exception as e:
            error_msg = f"Error processing request: {e}"
            logger.error(f"{error_msg}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            print(f"{error_msg}")

except Exception as e:
    error_msg = f"Error initializing agent: {e}"
    logger.error(f" {error_msg}")
    logger.error(f"Stack trace: {traceback.format_exc()}")
    print(f" {error_msg}")
    print("Please check your API key and dependencies")