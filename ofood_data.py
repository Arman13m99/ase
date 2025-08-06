"""
OFOOD Data Access - Complete Fixed Version with Saved Question Support
=====================================================================
Single-line data access interface for live Metabase data with full question support
"""

import pandas as pd
from typing import Optional
import os
from metabase_clickhouse_app import MetabaseClient, MetabaseConfig, VendorDataExtractor
from query_warehouse import QueryRegistry, CoreQueries


class OFoodConfig:
    DEFAULT_URL = "https://metabase.ofood.cloud"
    DEFAULT_USERNAME = "a.mehmandoost@OFOOD.CLOUD"
    DEFAULT_PASSWORD = None
    DEFAULT_TEAM = "growth"
    
    @classmethod
    def get_config(cls, team: str = None, password: str = None) -> MetabaseConfig:
        team = team or cls.DEFAULT_TEAM
        password = password or cls.DEFAULT_PASSWORD or os.getenv('METABASE_PASSWORD')
        
        if not password:
            raise ValueError(
                "Password not provided. Either:\n"
                "1. Set METABASE_PASSWORD environment variable, or\n"
                "2. Pass password parameter, or\n"
                "3. Update DEFAULT_PASSWORD in ofood_data.py"
            )
        
        return MetabaseConfig.create_with_team_db(
            url=cls.DEFAULT_URL,
            username=cls.DEFAULT_USERNAME,
            password=password,
            team=team
        )


def _execute_query(query_func, team: str = None, password: str = None, use_pagination: bool = True, 
                  optimization_mode: str = "auto", **query_params) -> Optional[pd.DataFrame]:
    config = OFoodConfig.get_config(team=team, password=password)
    client = MetabaseClient(config)
    
    try:
        if client.authenticate():
            extractor = VendorDataExtractor(client)
            
            if query_params:
                df = extractor.execute_query_from_warehouse(
                    query_func, 
                    use_pagination=use_pagination,
                    optimization_mode=optimization_mode,
                    **query_params
                )
            else:
                df = extractor.execute_query_from_warehouse(
                    query_func, 
                    use_pagination=use_pagination,
                    optimization_mode=optimization_mode
                )
            
            return df
        else:
            print("❌ Authentication failed")
            return None
            
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return None
        
    finally:
        client.logout()


# ============================================================================
# WAREHOUSE QUERIES
# ============================================================================

def get_vendors(team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Get latest vendor data with location information"""
    return _execute_query(QueryRegistry.X_MAP_VENDOR, team=team, password=password)


def get_orders(team: str = None, password: str = None, fast: bool = True) -> Optional[pd.DataFrame]:
    """Get comprehensive order mapping with customer analysis"""
    optimization_mode = "fast" if fast else "auto"
    return _execute_query(QueryRegistry.X_MAP_ORDER, team=team, password=password, optimization_mode=optimization_mode)


def get_vdom(city_id: Optional[int] = None, jalali_year: int = 1403, jalali_month: int = 7, 
             team: str = None, password: str = None, fast: bool = True) -> Optional[pd.DataFrame]:
    """Get Vendor DOM (Discount on Marketplace) analysis"""
    optimization_mode = "fast" if fast else "auto"
    return _execute_query(
        CoreQueries.x_vdom, 
        team=team, 
        password=password,
        optimization_mode=optimization_mode,
        city_id=city_id, 
        jalali_year=jalali_year, 
        jalali_month=jalali_month
    )


def get_geo(city_id: Optional[int] = None, team: str = None, password: str = None, fast: bool = True) -> Optional[pd.DataFrame]:
    """Get geolocation-based order analysis"""
    optimization_mode = "fast" if fast else "auto"
    return _execute_query(
        CoreQueries.x_geo, 
        team=team, 
        password=password,
        optimization_mode=optimization_mode,
        city_id=city_id
    )


def get_vouchers(team: str = None, password: str = None, fast: bool = True) -> Optional[pd.DataFrame]:
    """Get comprehensive voucher analysis with order data"""
    optimization_mode = "fast" if fast else "auto"
    return _execute_query(QueryRegistry.X_NET_LIVE_VOUCHERS, team=team, password=password, optimization_mode=optimization_mode)


def get_tf_vendors(team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Get TapsiFood vendor mapping with SnappFood cross-reference"""
    return _execute_query(QueryRegistry.TF_VENDORS, team=team, password=password)


def get_tf_menu(team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Get TapsiFood menu items with pricing and discounts"""
    return _execute_query(QueryRegistry.TF_MENU, team=team, password=password)


# ============================================================================
# ULTRA-FAST FUNCTIONS
# ============================================================================

def get_orders_fast(team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Ultra-fast order retrieval using parallel processing (3M+ rows in 3-4 minutes)"""
    print("🚀 Using ultra-fast parallel processing for orders...")
    return _execute_query(QueryRegistry.X_MAP_ORDER, team=team, password=password, optimization_mode="parallel")


def get_vouchers_fast(team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Ultra-fast voucher retrieval using parallel processing"""
    print("🚀 Using ultra-fast parallel processing for vouchers...")
    return _execute_query(QueryRegistry.X_NET_LIVE_VOUCHERS, team=team, password=password, optimization_mode="parallel")


def get_large_dataset(query_name: str, team: str = None, password: str = None, **params) -> Optional[pd.DataFrame]:
    """Generic ultra-fast retrieval for any large dataset"""
    query_map = {
        'orders': (QueryRegistry.X_MAP_ORDER, {}),
        'vouchers': (QueryRegistry.X_NET_LIVE_VOUCHERS, {}),
        'vdom': (CoreQueries.x_vdom, params),
        'geo': (CoreQueries.x_geo, params),
    }
    
    if query_name not in query_map:
        raise ValueError(f"Unknown query: {query_name}. Available: {list(query_map.keys())}")
    
    query_func, default_params = query_map[query_name]
    query_params = {**default_params, **params}
    
    print(f"🚀 Ultra-fast retrieval for {query_name} with parallel processing...")
    return _execute_query(query_func, team=team, password=password, optimization_mode="parallel", **query_params)


# ============================================================================
# SAVED METABASE QUESTIONS (NEW!)
# ============================================================================

def get_question_data(question_id: int, team: str = None, password: str = None, fast: bool = True) -> Optional[pd.DataFrame]:
    """
    Get data from existing Metabase question
    
    Args:
        question_id: Metabase question ID (from URL, e.g., 3132 from /question/3132-x-net)
        team: Database team ('growth', 'data', 'product') - optional
        password: Metabase password - optional if set in environment
        fast: Use optimization for large datasets
    
    Returns:
        DataFrame with question results (gets ALL rows, not just 2K limit!)
    
    Examples:
        # Get data from question ID 3132
        df = get_question_data(3132)
        
        # With specific team and fast processing
        df = get_question_data(3132, team="growth", fast=True)
    """
    
    config = OFoodConfig.get_config(team=team, password=password)
    client = MetabaseClient(config)
    
    try:
        if client.authenticate():
            if fast:
                return client.execute_saved_question_optimized(question_id, optimization_mode="auto")
            else:
                return client.execute_saved_question(question_id)
        else:
            print("❌ Authentication failed")
            return None
            
    except Exception as e:
        print(f"❌ Error executing question: {e}")
        return None
        
    finally:
        client.logout()


def get_question_data_fast(question_id: int, team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """
    Ultra-fast execution of existing Metabase question
    Gets ALL rows with parallel processing optimization!
    
    Args:
        question_id: Metabase question ID
        team: Database team - optional
        password: Metabase password - optional
    
    Examples:
        # Your question: https://metabase.ofood.cloud/question/3132-x-net
        df = get_question_data_fast(3132)
        
        # With team specification
        df = get_question_data_fast(3132, team="growth")
    """
    print(f"🚀 Executing question {question_id} with optimization...")
    return get_question_data(question_id, team=team, password=password, fast=True)


def get_multiple_questions(question_ids: list, team: str = None, password: str = None) -> dict:
    """
    Get data from multiple Metabase questions at once
    
    Args:
        question_ids: List of question IDs
        team: Database team - optional
        password: Metabase password - optional
    
    Returns:
        Dictionary with question_id as key and DataFrame as value
    
    Examples:
        results = get_multiple_questions([3132, 1234, 5678])
        df_3132 = results[3132]
        df_1234 = results[1234]
    """
    
    results = {}
    
    for question_id in question_ids:
        print(f"📊 Processing question {question_id}...")
        df = get_question_data_fast(question_id, team=team, password=password)
        results[question_id] = df
        
        if df is not None:
            print(f"   ✅ Question {question_id}: {len(df):,} rows")
        else:
            print(f"   ❌ Question {question_id}: Failed")
    
    return results


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_active_vendors(team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Get only active vendors (open=1 and visible=1)"""
    vendors_df = get_vendors(team=team, password=password)
    if vendors_df is not None:
        return vendors_df[(vendors_df['open'] == 1) & (vendors_df['visible'] == 1)]
    return None


def get_vendors_by_city(city_id: int, team: str = None, password: str = None) -> Optional[pd.DataFrame]:
    """Get vendors for a specific city"""
    vendors_df = get_vendors(team=team, password=password)
    if vendors_df is not None:
        return vendors_df[vendors_df['city_id'] == city_id]
    return None


def get_all_data(team: str = None, password: str = None) -> dict:
    """Get all main datasets in one call"""
    print("📊 Fetching all OFOOD datasets...")
    
    results = {}
    
    datasets = [
        ('vendors', get_vendors),
        ('orders', get_orders), 
        ('tf_vendors', get_tf_vendors),
        ('tf_menu', get_tf_menu),
        ('vouchers', get_vouchers)
    ]
    
    for name, func in datasets:
        print(f"   Fetching {name}...")
        df = func(team=team, password=password)
        results[name] = df
        
        if df is not None:
            print(f"   ✅ {name}: {len(df):,} records")
        else:
            print(f"   ❌ {name}: Failed")
    
    return results


# ============================================================================
# CONFIGURATION AND UTILITIES
# ============================================================================

def setup_credentials(password: str, team: str = "growth"):
    """One-time setup for credentials"""
    import os
    os.environ['METABASE_PASSWORD'] = password
    OFoodConfig.DEFAULT_TEAM = team
    print(f"✅ Credentials configured for {team} team database")


def test_connection(team: str = None, password: str = None) -> bool:
    """Test connection to Metabase"""
    try:
        config = OFoodConfig.get_config(team=team, password=password)
        client = MetabaseClient(config)
        
        if client.authenticate():
            client.logout()
            print("✅ Connection test successful!")
            return True
        else:
            print("❌ Authentication failed")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False


def print_usage_examples():
    """Print usage examples for quick reference"""
    examples = """
🚀 OFOOD Data Access - Usage Examples
====================================

1. BASIC SETUP (One time):
   from ofood_data import setup_credentials
   setup_credentials("your_password", "growth")

2. SINGLE LINE DATA ACCESS:
   from ofood_data import get_vendors, get_orders, get_vdom
   
   vendors_df = get_vendors()                    # Latest vendor data
   orders_df = get_orders()                      # Order data (auto-optimized)
   active_vendors_df = get_active_vendors()      # Only active vendors

3. SAVED METABASE QUESTIONS (NEW!):
   from ofood_data import get_question_data, get_question_data_fast
   
   # Get data from existing question (URL: /question/3132-x-net)
   df = get_question_data(3132)                  # Gets ALL rows, not just 2K!
   df = get_question_data_fast(3132)             # With parallel optimization
   
   # Multiple questions at once
   results = get_multiple_questions([3132, 1234, 5678])
   df_3132 = results[3132]

4. ULTRA-FAST PROCESSING:
   from ofood_data import get_orders_fast, get_vouchers_fast
   
   orders_df = get_orders_fast()                 # 3M+ rows in 3-4 minutes
   vouchers_df = get_vouchers_fast()             # Large voucher dataset
   
   # Generic large dataset handler
   orders_df = get_large_dataset('orders')       # Ultra-fast parallel

5. PARAMETERIZED QUERIES:
   vdom_df = get_vdom(city_id=1)                 # VDOM for city 1
   vdom_df = get_vdom(city_id=1, jalali_month=8) # Custom month
   geo_df = get_geo(city_id=5)                   # Geo analysis for city 5

6. DIFFERENT TEAM DATABASES:
   vendors_df = get_vendors(team='data')         # Use Data Team DB
   orders_df = get_orders(team='product')        # Use Product Team DB
   question_df = get_question_data(3132, team='growth')  # Saved question

7. GET ALL DATA AT ONCE:
   from ofood_data import get_all_data
   
   data = get_all_data()
   vendors_df = data['vendors']
   orders_df = data['orders']

8. TEST CONNECTION:
   from ofood_data import test_connection
   
   if test_connection():
       vendors_df = get_vendors()

AVAILABLE FUNCTIONS:
===================

📋 WAREHOUSE QUERIES:
• get_vendors()           - Latest vendor data with locations
• get_orders()            - Comprehensive order mapping  
• get_vdom()              - Vendor DOM analysis (with params)
• get_geo()               - Geolocation order analysis (with params)
• get_vouchers()          - Voucher analysis with orders
• get_tf_vendors()        - TapsiFood vendor mapping
• get_tf_menu()           - TapsiFood menu items

💾 SAVED QUESTIONS (NEW!):
• get_question_data()     - Execute saved Metabase questions
• get_question_data_fast() - Ultra-fast question execution
• get_multiple_questions() - Multiple questions at once

🚀 ULTRA-FAST FUNCTIONS:
• get_orders_fast()       - 3M+ orders with parallel processing
• get_vouchers_fast()     - Large voucher dataset
• get_large_dataset()     - Generic parallel processing

🛠️ CONVENIENCE:
• get_active_vendors()    - Filtered active vendors
• get_vendors_by_city()   - Vendors in specific city
• get_all_data()          - All datasets in one call

⚙️ CONFIGURATION:
• setup_credentials()     - One-time setup
• test_connection()       - Verify connection works

PERFORMANCE FEATURES:
====================
✅ Auto-optimization based on data size
✅ Parallel processing (3-5x faster for large datasets)
✅ No 2,000 row limits - gets ALL data
✅ Multi-database team support
✅ Saved Metabase question support
✅ Memory-efficient processing
    """
    
    print(examples)


# ============================================================================
# MAIN EXECUTION AND TESTING
# ============================================================================

def quick_test():
    """Quick test of all major functions"""
    print("🧪 QUICK TEST OF OFOOD DATA SYSTEM")
    print("=" * 50)
    
    # Test basic functions
    test_functions = [
        ("Connection Test", lambda: test_connection()),
        ("Vendors", lambda: get_vendors()),
        ("Question 3132", lambda: get_question_data_fast(3132)),
    ]
    
    results = {}
    
    for test_name, test_func in test_functions:
        print(f"\n🔬 Testing: {test_name}")
        try:
            result = test_func()
            if isinstance(result, pd.DataFrame):
                results[test_name] = f"✅ Success: {len(result):,} rows"
            elif result is True:
                results[test_name] = "✅ Success"
            else:
                results[test_name] = "❌ Failed"
        except Exception as e:
            results[test_name] = f"❌ Error: {e}"
    
    print(f"\n📋 TEST RESULTS:")
    for test, result in results.items():
        print(f"   • {test}: {result}")
    
    return results


if __name__ == "__main__":
    print_usage_examples()
    print("\n" + "="*50)
    print("To run quick test: python ofood_data.py test")
    print("To see examples: python ofood_data.py")
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        quick_test()