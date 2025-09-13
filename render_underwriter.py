from strands import Agent, tool
from strands.models.openai import OpenAIModel
import os
from dotenv import load_dotenv
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from typing import Optional, Dict, List
from decimal import Decimal
import logging
import traceback
from datetime import datetime

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('render_underwriter.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
# Add these to your .env file:
POSTGRES_URL = os.getenv("POSTGRES_URL")  # Your Render PostgreSQL connection string

def get_postgres_connection():
    """Get PostgreSQL connection to Render database"""
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise

def setup_database_tables():
    """Create necessary tables if they don't exist"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Create policies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS policies (
                id VARCHAR(50) PRIMARY KEY,
                tiv BIGINT,
                total_premium DECIMAL(15,2),
                line_of_business VARCHAR(100),
                construction_type VARCHAR(50),
                primary_risk_state VARCHAR(10),
                oldest_building INTEGER,
                winnability INTEGER,
                renewal_or_new_business VARCHAR(20),
                loss_value DECIMAL(15,2),
                created_at TIMESTAMP,
                effective_date TIMESTAMP,
                expiration_date TIMESTAMP,
                account_name VARCHAR(200),
                raw_data JSONB,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create underwriting results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS underwriting_results (
                id SERIAL PRIMARY KEY,
                policy_id VARCHAR(50) REFERENCES policies(id),
                classification VARCHAR(20) NOT NULL,
                reasoning TEXT,
                tiv BIGINT,
                total_premium DECIMAL(15,2),
                line_of_business VARCHAR(100),
                construction_type VARCHAR(50),
                primary_risk_state VARCHAR(10),
                oldest_building INTEGER,
                renewal_or_new_business VARCHAR(20),
                rules_version VARCHAR(50),
                underwritten_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(policy_id)
            );
        """)
        
        # Create indexes for better performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_policies_state ON policies(primary_risk_state);
            CREATE INDEX IF NOT EXISTS idx_policies_tiv ON policies(tiv);
            CREATE INDEX IF NOT EXISTS idx_policies_line_of_business ON policies(line_of_business);
            CREATE INDEX IF NOT EXISTS idx_underwriting_classification ON underwriting_results(classification);
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database tables created/verified successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up database tables: {e}")
        return False

def get_dynamodb_table(table_name: str = 'unpolishedData'):
    """Get DynamoDB table connection (for reading existing policies)"""
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
    """Apply underwriting rules based on your specific rules.txt"""
    
    # Extract key policy attributes
    tiv = policy_data.get('tiv', 0)
    total_premium = policy_data.get('total_premium', 0)
    line_of_business = policy_data.get('line_of_business', '').upper()
    construction_type = policy_data.get('construction_type', '').lower()
    state = policy_data.get('primary_risk_state', '')
    oldest_building = policy_data.get('oldest_building', 2024)
    renewal_or_new = policy_data.get('renewal_or_new_business', '').upper()
    loss_value = float(policy_data.get('loss_value', 0))
    
    reasoning_parts = []
    
    # Rule 1: Submission Type
    if renewal_or_new == 'RENEWAL':
        return "NOT SAFE", "Renewal Business is Not Acceptable per guidelines"
    elif renewal_or_new == 'NEW BUSINESS':
        reasoning_parts.append("✅ New Business is acceptable")
    else:
        reasoning_parts.append("⚠️ Unknown submission type")
    
    # Rule 2: Line of Business
    if 'PROPERTY' not in line_of_business:
        return "NOT SAFE", f"Line of Business '{line_of_business}' is not acceptable - only Property Line is accepted"
    else:
        reasoning_parts.append("✅ Property Line of Business is acceptable")
    
    # Rule 3: Primary Risk State
    acceptable_states = ['OH', 'PA', 'MD', 'CO', 'CA', 'FL', 'NC', 'SC', 'GA', 'VA', 'UT']
    target_states = ['OH', 'PA', 'MD', 'CO', 'CA', 'FL']
    
    if state not in acceptable_states:
        return "NOT SAFE", f"State '{state}' is not in acceptable states list"
    elif state in target_states:
        reasoning_parts.append(f"✅ State '{state}' is in target states")
    else:
        reasoning_parts.append(f"✅ State '{state}' is acceptable")
    
    # Rule 4: TIV Limits
    if tiv > 150000000:
        return "NOT SAFE", f"TIV of ${tiv:,} exceeds $150M limit"
    elif 50000000 <= tiv <= 100000000:
        reasoning_parts.append(f"✅ TIV of ${tiv:,} is in target range ($50M-$100M)")
    elif tiv <= 150000000:
        reasoning_parts.append(f"✅ TIV of ${tiv:,} is acceptable (under $150M)")
    
    # Rule 5: Total Premium
    if total_premium < 50000 or total_premium > 175000:
        return "NOT SAFE", f"Total Premium of ${total_premium:,} is outside acceptable range ($50K-$175K)"
    elif 75000 <= total_premium <= 100000:
        reasoning_parts.append(f"✅ Premium of ${total_premium:,} is in target range ($75K-$100K)")
    else:
        reasoning_parts.append(f"✅ Premium of ${total_premium:,} is acceptable ($50K-$175K)")
    
    # Rule 6: Building Age  
    if oldest_building <= 1990:
        return "NOT SAFE", f"Building from {oldest_building} is older than 1990 - not acceptable"
    elif oldest_building >= 2010:
        reasoning_parts.append(f"✅ Building from {oldest_building} is newer than 2010 (target)")
    else:
        reasoning_parts.append(f"✅ Building from {oldest_building} is newer than 1990 (acceptable)")
    
    # Rule 7: Construction Type
    high_quality_types = ['jm', 'non combustible', 'steel', 'masonry non combustible', 'masonry', 'concrete']
    # Note: This rule is complex as it mentions "greater than 50%" but we only have one construction type
    # Interpreting as: acceptable construction types vs not acceptable
    if any(quality_type in construction_type.lower() for quality_type in high_quality_types):
        reasoning_parts.append(f"✅ Construction type '{construction_type}' is acceptable")
    else:
        return "NOT SAFE", f"Construction type '{construction_type}' is not acceptable - must be JM, Non Combustible/Steel, or Masonry Non Combustible"
    
    # Rule 8: Loss Value
    if loss_value > 100000:
        return "NOT SAFE", f"Loss value of ${loss_value:,} exceeds $100K limit"
    else:
        reasoning_parts.append(f"✅ Loss value of ${loss_value:,} is under $100K")
    
    # If we get here, policy passes all criteria
    decision = "SAFE"
    reasoning = "Policy meets all underwriting criteria:\n" + "\n".join(reasoning_parts)
    
    return decision, reasoning

@tool
def migrate_policies_to_postgres(dynamo_table: str = 'unpolishedData') -> str:
    """Migrate policies from DynamoDB to Render PostgreSQL"""
    try:
        # Setup database tables
        if not setup_database_tables():
            return "Failed to setup database tables"
        
        # Get policies from DynamoDB
        table = get_dynamodb_table(dynamo_table)
        response = table.scan()
        policies = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            policies.extend(response['Items'])
        
        if not policies:
            return f"No policies found in DynamoDB table {dynamo_table}"
        
        # Connect to PostgreSQL
        conn = get_postgres_connection()
        cursor = conn.cursor()
        migrated_count = 0
        errors = []
        
        for policy in policies:
            try:
                # Convert Decimal objects
                def convert_decimals(obj):
                    if isinstance(obj, dict):
                        return {k: convert_decimals(v) for k, v in obj.items()}
                    elif isinstance(obj, Decimal):
                        return float(obj)
                    else:
                        return obj
                
                policy_data = convert_decimals(policy)
                
                # Insert into PostgreSQL
                cursor.execute("""
                    INSERT INTO policies (
                        id, tiv, total_premium, line_of_business, construction_type,
                        primary_risk_state, oldest_building, winnability, 
                        renewal_or_new_business, loss_value, created_at, 
                        effective_date, expiration_date, account_name, raw_data
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (id) DO UPDATE SET
                        tiv = EXCLUDED.tiv,
                        total_premium = EXCLUDED.total_premium,
                        line_of_business = EXCLUDED.line_of_business,
                        raw_data = EXCLUDED.raw_data
                """, (
                    str(policy_data.get('id')),
                    policy_data.get('tiv'),
                    policy_data.get('total_premium'),
                    policy_data.get('line_of_business'),
                    policy_data.get('construction_type'),
                    policy_data.get('primary_risk_state'),
                    policy_data.get('oldest_building'),
                    policy_data.get('winnability'),
                    policy_data.get('renewal_or_new_business'),
                    policy_data.get('loss_value'),
                    policy_data.get('created_at'),
                    policy_data.get('effective_date'),
                    policy_data.get('expiration_date'),
                    policy_data.get('account_name'),
                    json.dumps(policy_data)
                ))
                
                migrated_count += 1
                
            except Exception as e:
                error_msg = f"Error migrating policy {policy.get('id')}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        result = f"""
MIGRATION COMPLETED
==================
Total Policies Migrated: {migrated_count}
Errors: {len(errors)}
Target Database: Render PostgreSQL
"""
        
        if errors:
            result += "\nFirst few errors:\n" + "\n".join(errors[:3])
        
        return result
        
    except Exception as e:
        return f"Error migrating to PostgreSQL: {str(e)}"

@tool
def auto_underwrite_all_policies_postgres() -> str:
    """Automatically underwrite all policies and save to Render PostgreSQL"""
    try:
        # Setup database tables
        if not setup_database_tables():
            return "Failed to setup database tables"
        
        # Read underwriting rules
        try:
            with open("rules.txt", "r", encoding="utf-8") as f:
                rules_content = f.read()
        except FileNotFoundError:
            return "Error: rules.txt file not found"
        
        # Connect to PostgreSQL
        conn = get_postgres_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all policies from PostgreSQL
        cursor.execute("SELECT * FROM policies")
        policies = cursor.fetchall()
        
        if not policies:
            return "No policies found in PostgreSQL database. Run migration first."
        
        # Process each policy
        results_summary = {
            'total_processed': 0,
            'safe_count': 0,
            'not_safe_count': 0,
            'errors': []
        }
        
        for policy in policies:
            try:
                policy_id = str(policy['id'])
                
                # Apply underwriting rules
                decision, reasoning = apply_underwriting_rules(dict(policy), rules_content)
                
                # Save decision to database
                cursor.execute("""
                    INSERT INTO underwriting_results (
                        policy_id, classification, reasoning, tiv, total_premium,
                        line_of_business, construction_type, primary_risk_state,
                        oldest_building, renewal_or_new_business, rules_version
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (policy_id) DO UPDATE SET
                        classification = EXCLUDED.classification,
                        reasoning = EXCLUDED.reasoning,
                        underwritten_at = CURRENT_TIMESTAMP
                """, (
                    policy_id, decision, reasoning, policy['tiv'], 
                    policy['total_premium'], policy['line_of_business'],
                    policy['construction_type'], policy['primary_risk_state'],
                    policy['oldest_building'], policy['renewal_or_new_business'],
                    'v1.0'
                ))
                
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
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Generate summary
        summary = f"""
AUTOMATIC UNDERWRITING COMPLETED
================================
Total Policies Processed: {results_summary['total_processed']}
✅ SAFE: {results_summary['safe_count']} ({results_summary['safe_count']/results_summary['total_processed']*100:.1f}%)
❌ NOT SAFE: {results_summary['not_safe_count']} ({results_summary['not_safe_count']/results_summary['total_processed']*100:.1f}%)
❌ Errors: {len(results_summary['errors'])}

Results saved to Render PostgreSQL
"""
        
        if results_summary['errors']:
            summary += "\nErrors encountered:\n" + "\n".join(results_summary['errors'][:3])
        
        return summary
        
    except Exception as e:
        return f"Error in automatic underwriting: {str(e)}"

@tool
def get_underwriting_summary_postgres() -> str:
    """Get underwriting summary from Render PostgreSQL"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get summary statistics
        cursor.execute("""
            SELECT 
                classification,
                COUNT(*) as count,
                ROUND(AVG(tiv)) as avg_tiv,
                ROUND(AVG(total_premium), 2) as avg_premium
            FROM underwriting_results 
            GROUP BY classification
            ORDER BY classification
        """)
        
        summary_stats = cursor.fetchall()
        
        if not summary_stats:
            return "No underwriting results found in database"
        
        # Get detailed results
        cursor.execute("""
            SELECT policy_id, classification, tiv, total_premium, 
                   line_of_business, primary_risk_state, reasoning,
                   underwritten_at
            FROM underwriting_results 
            ORDER BY underwritten_at DESC
        """)
        
        detailed_results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Format summary
        total_policies = sum(stat['count'] for stat in summary_stats)
        
        summary = f"""
UNDERWRITING SUMMARY (PostgreSQL)
=================================
Total Policies: {total_policies}

BREAKDOWN BY CLASSIFICATION:
"""
        
        for stat in summary_stats:
            emoji = "✅" if stat['classification'] == "SAFE" else "❌"
            percentage = (stat['count'] / total_policies) * 100
            summary += f"{emoji} {stat['classification']}: {stat['count']} ({percentage:.1f}%)\n"
            summary += f"   Avg TIV: ${stat['avg_tiv']:,} | Avg Premium: ${stat['avg_premium']:,}\n"
        
        summary += "\nSAMPLE RESULTS:\n"
        for result in detailed_results[:10]:  # Show first 10
            emoji = "✅" if result['classification'] == "SAFE" else "❌"
            summary += f"{emoji} Policy {result['policy_id']} ({result['primary_risk_state']}) - ${result['tiv']:,} TIV\n"
        
        if len(detailed_results) > 10:
            summary += f"... and {len(detailed_results) - 10} more results"
        
        return summary
        
    except Exception as e:
        return f"Error getting summary from PostgreSQL: {str(e)}"

# Check environment variables
if not COHERE_API_KEY:
    print("Error: COHERE_API_KEY not found in environment variables")
    exit(1)

if not POSTGRES_URL:
    print("Error: POSTGRES_URL not found in environment variables")
    print("Add your Render PostgreSQL connection string to .env file:")
    print("POSTGRES_URL=postgresql://username:password@hostname:port/database")
    exit(1)

try:
    logger.info("Initializing Render PostgreSQL Underwriting Agent...")
    
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
        migrate_policies_to_postgres,
        auto_underwrite_all_policies_postgres,
        get_underwriting_summary_postgres
    ])

    print("🐘 Render PostgreSQL Underwriting Agent Ready!")
    print("\nAvailable commands:")
    print("- 'Migrate policies to PostgreSQL' - Move data from DynamoDB")
    print("- 'Underwrite all policies' - Run underwriting analysis") 
    print("- 'Show underwriting summary' - View results")
    print("\nMake sure your .env file has:")
    print("POSTGRES_URL=postgresql://username:password@hostname:port/database")
    
    while True:
        user_input = input("\n🐘 PostgreSQL command (or 'quit' to exit): ")
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("👋 Goodbye!")
            break
        
        try:
            response = agent(user_input)
            print(f"🐘 {response}")
        except Exception as e:
            print(f"❌ Error: {e}")

except Exception as e:
    print(f"❌ Error initializing agent: {e}")