from strands import Agent, tool
from strands.models.openai import OpenAIModel
import os
from dotenv import load_dotenv
import boto3
from typing import Optional, Dict, List
from decimal import Decimal
import logging
import json
import traceback
from datetime import datetime

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_underwriter.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API KEYS
load_dotenv()
COHERE_API_KEY = os.getenv("COHERE_API_KEY")

def get_dynamodb_table(table_name: str = 'unpolishedData'):
    """Get DynamoDB table connection"""
    session = boto3.Session(
        aws_access_key_id='fakeMyKeyId',
        aws_secret_access_key='fakeSecretAccessKey',
        region_name="us-west-2"
    )
    dynamodb = session.resource(
        'dynamodb',
        endpoint_url='http://localhost:8123'
    )
    return dynamodb.Table(table_name)

def apply_underwriting_rules(policy_data: dict, rules_content: str) -> tuple:
    """Apply underwriting rules to a policy and return (decision, reasoning)"""
    
    # Extract key policy attributes
    tiv = policy_data.get('tiv', 0)
    construction = policy_data.get('construction_type', '').lower()
    state = policy_data.get('primary_risk_state', '')
    oldest_building = policy_data.get('oldest_building', 2024)
    winnability = policy_data.get('winnability', 0)
    line_of_business = policy_data.get('line_of_business', '')
    total_premium = policy_data.get('total_premium', 0)
    
    reasoning_parts = []
    risk_factors = []
    
    # Calculate premium to TIV ratio
    premium_ratio = (float(total_premium) / float(tiv)) * 100 if tiv > 0 else 0
    
    # Apply automatic decline criteria
    if tiv > 100000000:
        return "NOT SAFE", f"TIV of ${tiv:,} exceeds $100M limit without special approval"
    
    if oldest_building < 1950 and state in ['CA', 'FL', 'TX']:
        return "NOT SAFE", f"Building from {oldest_building} in high-risk state {state} - too old"
    
    if winnability < 50:
        return "NOT SAFE", f"Winnability score of {winnability} is below minimum threshold of 50"
    
    if premium_ratio < 0.3:
        return "NOT SAFE", f"Premium ratio of {premium_ratio:.2f}% is below 0.3% - inadequate pricing"
    
    # Check risk combinations
    if 'frame' in construction and oldest_building < 1970 and state == 'CA':
        return "NOT SAFE", "Frame construction + pre-1970 building + California = high earthquake risk"
    
    # Evaluate individual risk factors
    # TIV assessment
    if 1000000 <= tiv <= 100000000:
        reasoning_parts.append(f"✅ TIV of ${tiv:,} is within acceptable range")
    elif tiv < 1000000:
        risk_factors.append(f"⚠️ Low TIV of ${tiv:,} may indicate insufficient coverage")
    
    # Construction type assessment
    safe_construction = ['masonry', 'concrete', 'steel', 'non-combustible']
    if any(safe_type in construction for safe_type in safe_construction):
        reasoning_parts.append(f"✅ {construction.title()} construction is low-risk")
    elif 'frame' in construction:
        if state not in ['CA', 'FL', 'TX']:
            reasoning_parts.append(f"⚠️ Frame construction acceptable in {state}")
        else:
            risk_factors.append(f"⚠️ Frame construction in high-risk state {state}")
    
    # Building age assessment
    current_year = datetime.now().year
    building_age = current_year - oldest_building
    if oldest_building >= 1980:
        reasoning_parts.append(f"✅ Building from {oldest_building} is relatively new")
    elif oldest_building >= 1960:
        reasoning_parts.append(f"⚠️ Building from {oldest_building} is older but acceptable")
    else:
        risk_factors.append(f"⚠️ Building from {oldest_building} is very old - high risk")
    
    # Geographic risk
    if state in ['CA', 'FL', 'TX']:
        risk_factors.append(f"⚠️ {state} is moderate-to-high risk state")
    else:
        reasoning_parts.append(f"✅ {state} is low-risk geographic area")
    
    # Winnability assessment
    if winnability >= 80:
        reasoning_parts.append(f"✅ Winnability score of {winnability} is excellent")
    elif winnability >= 60:
        reasoning_parts.append(f"⚠️ Winnability score of {winnability} is moderate")
    else:
        risk_factors.append(f"❌ Winnability score of {winnability} is concerning")
    
    # Premium ratio assessment
    if 0.5 <= premium_ratio <= 5.0:
        reasoning_parts.append(f"✅ Premium ratio of {premium_ratio:.2f}% is appropriate")
    elif premium_ratio < 0.5:
        risk_factors.append(f"⚠️ Premium ratio of {premium_ratio:.2f}% may be too low")
    else:
        risk_factors.append(f"⚠️ Premium ratio of {premium_ratio:.2f}% is high - indicates risk")
    
    # Make final decision based on risk factors
    if len(risk_factors) == 0:
        decision = "SAFE"
        reasoning = "Policy meets all safety criteria:\n" + "\n".join(reasoning_parts)
    elif len(risk_factors) <= 2 and winnability >= 70:
        decision = "SAFE"
        reasoning = "Policy acceptable despite minor risk factors:\n" + "\n".join(reasoning_parts + risk_factors)
    else:
        decision = "NOT SAFE"
        reasoning = "Policy has too many risk factors:\n" + "\n".join(risk_factors + reasoning_parts)
    
    return decision, reasoning

@tool
def auto_underwrite_all_policies(table_name: str = 'unpolishedData', results_table: str = 'underwritingResults') -> str:
    """Automatically underwrite all policies and save decisions to database"""
    try:
        # Get all policies
        table = get_dynamodb_table(table_name)
        response = table.scan()
        policies = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            policies.extend(response['Items'])
        
        if not policies:
            return f"No policies found in table {table_name}"
        
        # Read underwriting rules
        try:
            with open("rules.txt", "r", encoding="utf-8") as f:
                rules_content = f.read()
        except FileNotFoundError:
            rules_content = "Default rules: Basic risk assessment applied"
        
        # Set up results table
        session = boto3.Session(
            aws_access_key_id='fakeMyKeyId',
            aws_secret_access_key='fakeSecretAccessKey',
            region_name="us-west-2"
        )
        dynamodb = session.resource('dynamodb', endpoint_url='http://localhost:8123')
        
        try:
            existing_tables = [t.name for t in dynamodb.tables.all()]
            if results_table not in existing_tables:
                results_table_obj = dynamodb.create_table(
                    TableName=results_table,
                    KeySchema=[{'AttributeName': 'policy_id', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'policy_id', 'AttributeType': 'S'}],
                    BillingMode='PAY_PER_REQUEST'
                )
                results_table_obj.wait_until_exists()
            else:
                results_table_obj = dynamodb.Table(results_table)
        except Exception as e:
            return f"Error setting up results table: {e}"
        
        # Process each policy automatically
        results_summary = {
            'total_processed': 0,
            'safe_count': 0,
            'not_safe_count': 0,
            'errors': []
        }
        
        for policy in policies:
            try:
                policy_id = str(policy.get('id', 'unknown'))
                
                # Convert Decimal objects
                def convert_decimals(obj):
                    if isinstance(obj, dict):
                        return {k: convert_decimals(v) for k, v in obj.items()}
                    elif isinstance(obj, Decimal):
                        return float(obj)
                    else:
                        return obj
                
                policy_data = convert_decimals(policy)
                
                # Apply underwriting rules automatically
                decision, reasoning = apply_underwriting_rules(policy_data, rules_content)
                
                # Save decision to database
                results_table_obj.put_item(Item={
                    'policy_id': policy_id,
                    'policy_data': json.dumps(policy_data, default=str),
                    'classification': decision,
                    'reasoning': reasoning,
                    'timestamp': datetime.now().isoformat(),
                    'rules_applied': 'Automatic rule-based assessment'
                })
                
                # Update counters
                results_summary['total_processed'] += 1
                if decision == 'SAFE':
                    results_summary['safe_count'] += 1
                else:
                    results_summary['not_safe_count'] += 1
                
                print(f"Policy {policy_id}: {decision}")
                
            except Exception as e:
                error_msg = f"Error processing policy {policy.get('id', 'unknown')}: {str(e)}"
                results_summary['errors'].append(error_msg)
                logger.error(error_msg)
        
        # Generate summary
        summary = f"""
AUTOMATIC UNDERWRITING COMPLETED
================================
Total Policies Processed: {results_summary['total_processed']}
✅ SAFE: {results_summary['safe_count']}
❌ NOT SAFE: {results_summary['not_safe_count']}
❌ Errors: {len(results_summary['errors'])}

Results saved to table: {results_table}
"""
        
        if results_summary['errors']:
            summary += "\nErrors encountered:\n" + "\n".join(results_summary['errors'][:3])
        
        return summary
        
    except Exception as e:
        return f"Error in automatic underwriting: {str(e)}"

@tool
def get_underwriting_summary(results_table: str = 'underwritingResults') -> str:
    """Get a summary of all underwriting decisions"""
    try:
        session = boto3.Session(
            aws_access_key_id='fakeMyKeyId',
            aws_secret_access_key='fakeSecretAccessKey',
            region_name="us-west-2"
        )
        dynamodb = session.resource('dynamodb', endpoint_url='http://localhost:8123')
        table = dynamodb.Table(results_table)
        
        response = table.scan()
        results = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            results.extend(response['Items'])
        
        if not results:
            return f"No underwriting results found in table {results_table}"
        
        # Summarize results
        safe_count = len([r for r in results if r.get('classification') == 'SAFE'])
        not_safe_count = len([r for r in results if r.get('classification') == 'NOT SAFE'])
        
        summary = f"""
UNDERWRITING SUMMARY
===================
Total Policies: {len(results)}
✅ SAFE: {safe_count} ({safe_count/len(results)*100:.1f}%)
❌ NOT SAFE: {not_safe_count} ({not_safe_count/len(results)*100:.1f}%)

DETAILED RESULTS:
"""
        
        for result in results:
            classification = result.get('classification', 'UNKNOWN')
            emoji = "✅" if classification == "SAFE" else "❌"
            summary += f"\n{emoji} Policy {result.get('policy_id')}: {classification}"
            if result.get('reasoning'):
                summary += f"\n   {result.get('reasoning', '')[:80]}..."
        
        return summary
        
    except Exception as e:
        return f"Error getting underwriting summary: {str(e)}"

# Check if required environment variable is set
if not COHERE_API_KEY:
    logger.error("COHERE_API_KEY not found in environment variables")
    print("Error: COHERE_API_KEY not found in environment variables")
    exit(1)

try:
    logger.info("Initializing Automatic Underwriting Agent...")
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

    agent = Agent(model=model, tools=[
        auto_underwrite_all_policies,
        get_underwriting_summary
    ])

    print("🤖 AUTOMATIC Insurance Underwriting Agent Ready!")
    print("\nThis agent will automatically make SAFE/NOT SAFE decisions")
    print("\nAvailable commands:")
    print("- 'Underwrite all policies automatically'")
    print("- 'Show underwriting summary'")
    
    logger.info("Automatic underwriting agent ready")
    
    while True:
        user_input = input("\n🤖 Auto-underwriter command (or 'quit' to exit): ")
        if user_input.lower() in ['quit', 'exit', 'q']:
            logger.info("User requested exit")
            print("👋 Goodbye!")
            break
        
        try:
            logger.info(f"User input: {user_input}")
            response = agent(user_input)
            logger.info(f"Agent response length: {len(str(response))} characters")
            print(f"🤖 {response}")
        except Exception as e:
            error_msg = f"Error processing request: {e}"
            logger.error(f"{error_msg}")
            print(f"❌ {error_msg}")

except Exception as e:
    error_msg = f"Error initializing agent: {e}"
    logger.error(f"{error_msg}")
    print(f"❌ {error_msg}")