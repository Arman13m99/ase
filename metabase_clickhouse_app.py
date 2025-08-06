"""
Metabase ClickHouse Query Application - Complete Fixed Version
============================================================
A Python application that connects to Metabase, executes ClickHouse queries,
and returns results as pandas DataFrames with optimizations and saved question support.
"""

import requests
import pandas as pd
import json
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
import time
import concurrent.futures


@dataclass
class MetabaseConfig:
    """Configuration for Metabase connection"""
    url: str
    username: str
    password: str
    database_name: str
    database_id: Optional[int] = None

    @classmethod
    def create_with_team_db(cls, url: str, username: str, password: str, team: str):
        """
        Create config with team-specific database
        
        Args:
            team: 'growth', 'data', or 'product'
        """
        team_databases = {
            'growth': 'Growth Team Clickhouse Connection',
            'data': 'Data Team Clickhouse Connection', 
            'product': 'Product Team Clickhouse Connection'
        }
        
        if team.lower() not in team_databases:
            raise ValueError(f"Invalid team. Choose from: {list(team_databases.keys())}")
            
        return cls(
            url=url,
            username=username,
            password=password,
            database_name=team_databases[team.lower()]
        )


class MetabaseClient:
    """Client for interacting with Metabase API with saved question support"""
    
    def __init__(self, config: MetabaseConfig):
        self.config = config
        self.session = requests.Session()
        self.session_token = None
        self.database_id = config.database_id
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def authenticate(self) -> bool:
        """Authenticate with Metabase and get session token"""
        try:
            auth_url = f"{self.config.url}/api/session"
            auth_data = {
                "username": self.config.username,
                "password": self.config.password
            }
            
            response = self.session.post(auth_url, json=auth_data)
            response.raise_for_status()
            
            auth_result = response.json()
            self.session_token = auth_result.get('id')
            
            # Set session token in headers for future requests
            self.session.headers.update({
                'X-Metabase-Session': self.session_token
            })
            
            self.logger.info("Successfully authenticated with Metabase")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def get_database_id(self) -> Optional[int]:
        """Get database ID by name"""
        if self.database_id:
            return self.database_id
            
        try:
            databases_url = f"{self.config.url}/api/database"
            response = self.session.get(databases_url)
            response.raise_for_status()
            
            databases = response.json().get('data', [])
            
            for db in databases:
                if db.get('name') == self.config.database_name:
                    self.database_id = db.get('id')
                    self.logger.info(f"Found database ID: {self.database_id}")
                    return self.database_id
            
            self.logger.error(f"Database '{self.config.database_name}' not found")
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get database ID: {e}")
            return None
    
    def execute_query(self, sql_query: str, timeout: int = 300, max_results: int = None) -> Optional[pd.DataFrame]:
        """
        Execute SQL query and return results as pandas DataFrame
        
        Args:
            sql_query: SQL query string
            timeout: Query timeout in seconds (default: 300)
            max_results: Maximum number of results to return (None for unlimited)
            
        Returns:
            pandas DataFrame with query results or None if failed
        """
        if not self.session_token:
            self.logger.error("Not authenticated. Call authenticate() first.")
            return None
        
        if not self.database_id:
            self.database_id = self.get_database_id()
            if not self.database_id:
                return None
        
        try:
            # Create native query payload with constraints
            constraints = {
                "max-results": max_results or 100000,
                "max-results-bare-rows": max_results or 100000
            }
            
            query_payload = {
                "type": "native",
                "native": {
                    "query": sql_query,
                    "template-tags": {}
                },
                "database": self.database_id,
                "constraints": constraints
            }
            
            # Execute query
            query_url = f"{self.config.url}/api/dataset"
            response = self.session.post(query_url, json=query_payload, timeout=timeout)
            response.raise_for_status()
            
            result = response.json()
            
            # Check if query was successful
            if result.get('status') != 'completed':
                self.logger.error(f"Query failed with status: {result.get('status')}")
                if 'error' in result:
                    self.logger.error(f"Error details: {result['error']}")
                return None
            
            # Extract data
            data = result.get('data', {})
            rows = data.get('rows', [])
            columns = [col['name'] for col in data.get('cols', [])]
            
            # Check if results were truncated
            is_truncated = data.get('results_truncated', False)
            if is_truncated:
                self.logger.warning("Results may have been truncated by Metabase")
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            self.logger.info(f"Query executed successfully. Retrieved {len(df)} rows, {len(df.columns)} columns")
            if is_truncated:
                self.logger.warning("⚠️ Results were truncated - you may not have all data!")
            
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Query execution failed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error during query execution: {e}")
            return None
    
    def execute_query_with_pagination(self, sql_query: str, page_size: int = 25000) -> Optional[pd.DataFrame]:
        """
        Execute query with pagination to get all results (optimized version)
        
        Args:
            sql_query: SQL query string
            page_size: Number of rows per page (increased for efficiency)
            
        Returns:
            Complete pandas DataFrame with all results
        """
        self.logger.info("📄 Executing query with optimized pagination...")
        
        all_dataframes = []
        offset = 0
        
        while True:
            # Add LIMIT and OFFSET to the query
            paginated_query = f"{sql_query.rstrip(';')} LIMIT {page_size} OFFSET {offset}"
            
            df = self.execute_query(paginated_query, max_results=page_size)
            
            if df is None or len(df) == 0:
                break
            
            all_dataframes.append(df)
            self.logger.info(f"📄 Page {len(all_dataframes)}: {len(df):,} rows")
            
            # If we got less than page_size, we're done
            if len(df) < page_size:
                break
                
            offset += page_size
        
        if not all_dataframes:
            self.logger.error("No data retrieved")
            return None
        
        # Combine all DataFrames
        final_df = pd.concat(all_dataframes, ignore_index=True)
        self.logger.info(f"✅ Pagination complete: {len(final_df):,} total rows")
        
        return final_df
    
    def execute_query_with_parallel_pagination(self, sql_query: str, page_size: int = 50000, max_workers: int = 6) -> Optional[pd.DataFrame]:
        """
        Execute query with parallel pagination for maximum speed
        
        Args:
            sql_query: SQL query string
            page_size: Number of rows per page (larger for efficiency)
            max_workers: Number of parallel connections
            
        Returns:
            Complete pandas DataFrame with all results
        """
        self.logger.info(f"🚀 Executing query with parallel pagination ({max_workers} workers)...")
        
        # First, get total count to calculate pages
        count_query = f"""
        SELECT COUNT(*) as total_rows
        FROM ({sql_query.rstrip(';')}) as subquery
        """
        
        count_df = self.execute_query(count_query, max_results=1)
        if count_df is None:
            self.logger.error("Failed to get total row count")
            return None
        
        total_rows = count_df.iloc[0]['total_rows']
        total_pages = (total_rows + page_size - 1) // page_size
        
        self.logger.info(f"📊 Total rows: {total_rows:,}, Pages: {total_pages}, Page size: {page_size:,}")
        
        if total_rows == 0:
            return pd.DataFrame()
        
        # Create separate clients for parallel execution
        def fetch_page(page_num):
            """Fetch a single page with dedicated client"""
            try:
                # Create dedicated client for this thread
                thread_client = MetabaseClient(self.config)
                if not thread_client.authenticate():
                    return None, page_num
                
                offset = page_num * page_size
                paginated_query = f"{sql_query.rstrip(';')} LIMIT {page_size} OFFSET {offset}"
                
                df = thread_client.execute_query(paginated_query, max_results=page_size)
                thread_client.logout()
                
                if df is not None and len(df) > 0:
                    return df, page_num
                return None, page_num
                
            except Exception as e:
                self.logger.error(f"Error fetching page {page_num}: {e}")
                return None, page_num
        
        # Execute pages in parallel
        all_dataframes = [None] * total_pages  # Preserve order
        successful_pages = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all page requests
            future_to_page = {
                executor.submit(fetch_page, page_num): page_num 
                for page_num in range(total_pages)
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_page):
                df, page_num = future.result()
                if df is not None:
                    all_dataframes[page_num] = df
                    successful_pages += 1
                    self.logger.info(f"✅ Page {page_num + 1}/{total_pages}: {len(df):,} rows")
                else:
                    self.logger.warning(f"❌ Page {page_num + 1} failed")
        
        # Filter out None results and combine
        valid_dataframes = [df for df in all_dataframes if df is not None]
        
        if not valid_dataframes:
            self.logger.error("No data retrieved from any page")
            return None
        
        # Combine all DataFrames
        final_df = pd.concat(valid_dataframes, ignore_index=True)
        self.logger.info(f"🎉 Parallel fetch complete: {len(final_df):,} total rows ({successful_pages}/{total_pages} pages)")
        
        return final_df
    
    def execute_query_optimized(self, sql_query: str, optimization_mode: str = "auto") -> Optional[pd.DataFrame]:
        """
        Execute query with automatic optimization based on estimated size
        
        Args:
            sql_query: SQL query string
            optimization_mode: "auto", "single", "pagination", "parallel"
            
        Returns:
            Complete pandas DataFrame optimized for query size
        """
        
        if optimization_mode == "auto":
            # Get estimated row count first
            count_query = f"""
            SELECT COUNT(*) as total_rows
            FROM ({sql_query.rstrip(';')}) as subquery
            """
            
            count_df = self.execute_query(count_query, max_results=1)
            if count_df is None:
                self.logger.warning("Could not estimate size, using parallel mode")
                optimization_mode = "parallel"
            else:
                total_rows = count_df.iloc[0]['total_rows']
                self.logger.info(f"📊 Estimated rows: {total_rows:,}")
                
                if total_rows <= 50000:
                    optimization_mode = "single"
                elif total_rows <= 500000:
                    optimization_mode = "pagination"
                else:
                    optimization_mode = "parallel"
                
                self.logger.info(f"🔧 Auto-selected optimization: {optimization_mode}")
        
        # Execute based on optimization mode
        if optimization_mode == "single":
            return self.execute_query(sql_query, max_results=100000)
        elif optimization_mode == "pagination":
            return self.execute_query_with_pagination(sql_query, page_size=25000)
        elif optimization_mode == "parallel":
            return self.execute_query_with_parallel_pagination(sql_query, page_size=50000, max_workers=6)
        else:
            # Default to parallel for unknown modes
            return self.execute_query_with_parallel_pagination(sql_query, page_size=50000, max_workers=6)
    
    def execute_saved_question(self, question_id: int, parameters: dict = None) -> Optional[pd.DataFrame]:
        """Execute a saved Metabase question by ID"""
        
        if not self.session_token:
            self.logger.error("Not authenticated. Call authenticate() first.")
            return None
        
        try:
            question_url = f"{self.config.url}/api/card/{question_id}/query"
            payload = {}
            if parameters:
                payload["parameters"] = parameters
            
            self.logger.info(f"Executing saved question {question_id}...")
            
            response = self.session.post(question_url, json=payload, timeout=300)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('status') != 'completed':
                self.logger.error(f"Question {question_id} execution failed with status: {result.get('status')}")
                return None
            
            data = result.get('data', {})
            rows = data.get('rows', [])
            columns = [col['name'] for col in data.get('cols', [])]
            
            df = pd.DataFrame(rows, columns=columns)
            self.logger.info(f"Question {question_id} executed successfully. Retrieved {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to execute question {question_id}: {e}")
            return None

    def get_question_details(self, question_id: int) -> Optional[dict]:
        """Get details about a saved question"""
        
        if not self.session_token:
            return None
        
        try:
            question_url = f"{self.config.url}/api/card/{question_id}"
            response = self.session.get(question_url)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get question {question_id} details: {e}")
            return None

    def execute_saved_question_optimized(self, question_id: int, optimization_mode: str = "auto") -> Optional[pd.DataFrame]:
        """Execute saved question with optimization"""
        
        question_details = self.get_question_details(question_id)
        
        if question_details:
            self.logger.info(f"Question: {question_details.get('name', 'Unknown')}")
            
            dataset_query = question_details.get('dataset_query', {})
            if dataset_query.get('type') == 'native':
                sql_query = dataset_query.get('native', {}).get('query', '')
                
                if sql_query:
                    self.logger.info("Found native SQL query, using optimization...")
                    return self.execute_query_optimized(sql_query, optimization_mode)
        
        self.logger.info("Using direct question execution...")
        return self.execute_saved_question(question_id)
    
    def logout(self):
        """Logout from Metabase"""
        if self.session_token:
            try:
                logout_url = f"{self.config.url}/api/session"
                self.session.delete(logout_url)
                self.logger.info("Successfully logged out from Metabase")
            except Exception as e:
                self.logger.warning(f"Logout warning: {e}")
            finally:
                self.session_token = None
                self.session.headers.pop('X-Metabase-Session', None)


class VendorDataExtractor:
    """Specialized class for data extraction with saved question support"""
    
    def __init__(self, metabase_client: MetabaseClient):
        self.client = metabase_client
    
    def execute_query_from_warehouse(self, query_func, use_pagination: bool = True, optimization_mode: str = "auto", **kwargs):
        """
        Execute a query from the query warehouse with optimizations
        
        Args:
            query_func: Function that returns SQL query string
            use_pagination: Whether to use pagination
            optimization_mode: "auto", "single", "pagination", "parallel", "fast"
            **kwargs: Arguments to pass to query function
        """
        query = query_func(**kwargs)
        
        if optimization_mode == "fast" or optimization_mode == "parallel":
            # Use parallel pagination for maximum speed
            return self.client.execute_query_with_parallel_pagination(query, page_size=50000, max_workers=6)
        elif optimization_mode == "auto":
            # Let the system decide the best method
            return self.client.execute_query_optimized(query, optimization_mode="auto")
        elif use_pagination:
            return self.client.execute_query_with_pagination(query, page_size=25000)
        else:
            return self.client.execute_query(query, max_results=100000)
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = "data_export.csv"):
        """Save DataFrame to CSV file"""
        try:
            df.to_csv(filename, index=False)
            print(f"💾 Data saved to {filename}")
        except Exception as e:
            print(f"❌ Failed to save CSV: {e}")
    
    def save_to_excel(self, df: pd.DataFrame, filename: str = "data_export.xlsx"):
        """Save DataFrame to Excel file"""
        try:
            df.to_excel(filename, index=False)
            print(f"💾 Data saved to {filename}")
        except Exception as e:
            print(f"❌ Failed to save Excel: {e}")
    
    def save_data_with_timestamp(self, df: pd.DataFrame, base_name: str = "export"):
        """Save data with timestamp in filename"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        csv_filename = f"{base_name}_{timestamp}.csv"
        excel_filename = f"{base_name}_{timestamp}.xlsx"
        
        self.save_to_csv(df, csv_filename)
        self.save_to_excel(df, excel_filename)
        
        return csv_filename, excel_filename


def main():
    """Main application function with saved question support"""
    
    # Configuration with team selection
    config = MetabaseConfig.create_with_team_db(
        url="https://metabase.ofood.cloud",
        username="a.mehmandoost@OFOOD.CLOUD",
        password="*********",  # Replace with actual password
        team="growth"  # Options: 'growth', 'data', 'product'
    )
    
    # Initialize client
    client = MetabaseClient(config)
    
    try:
        # Authenticate
        if not client.authenticate():
            print("Failed to authenticate with Metabase")
            return
        
        # Test saved question execution
        print("Testing saved question execution...")
        question_df = client.execute_saved_question_optimized(3132)
        
        if question_df is not None:
            print(f"✅ Saved question success! Retrieved {len(question_df)} rows")
        else:
            print("❌ Saved question failed")
        
        # Initialize data extractor
        extractor = VendorDataExtractor(client)
        
        # Example: Import and use query from warehouse
        try:
            from query_warehouse import QueryRegistry
            
            # Execute vendor query using warehouse
            print("Executing vendor data query from warehouse...")
            df = extractor.execute_query_from_warehouse(QueryRegistry.X_MAP_VENDOR)
            
            if df is not None:
                print(f"Success! Retrieved {len(df)} vendors")
                print(f"Columns: {list(df.columns)}")
                print("\nFirst 5 rows:")
                print(df.head())
                
                # Save to files with timestamp
                extractor.save_data_with_timestamp(df, "vendor_data")
        except ImportError:
            print("Query warehouse not available, skipping warehouse test")
            
    finally:
        # Always logout
        client.logout()


if __name__ == "__main__":
    main()