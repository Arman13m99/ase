# OFOOD Metabase Data Access System

A high-performance Python data access system for connecting to OFOOD's ClickHouse database through Metabase API with parallel processing capabilities.

## 🚀 Key Features

- **Ultra-fast data retrieval**: 3M+ rows in 3-4 minutes (5x faster than traditional methods)
- **Parallel processing**: Simultaneous multi-threaded data fetching
- **Auto-optimization**: Automatically selects best strategy based on data size
- **Saved question support**: Execute existing Metabase questions with full data access
- **Multi-team database support**: Growth, Data, and Product team databases
- **No row limits**: Gets ALL data, not just display limits
- **Production-ready**: Robust error handling and connection management

## 📊 Performance Comparison

| Method | 3M Rows | Time | API Calls | Connections |
|--------|---------|------|-----------|-------------|
| Traditional | 15+ min | 300+ | 1 | Sequential |
| **Parallel** | **3-4 min** | **60** | **6** | **Simultaneous** |

## 🛠️ Installation

```bash
# Clone or download the project files
# Required files:
# - ofood_data.py (main interface)
# - metabase_clickhouse_app.py (core client)
# - query_warehouse.py (SQL queries)

# Install dependencies
pip install pandas requests
```

## ⚡ Quick Start

### 1. Basic Setup (One Time)
```python
from ofood_data import setup_credentials
setup_credentials("your_metabase_password", "growth")
```

### 2. Single-Line Data Access
```python
from ofood_data import get_vendors, get_orders, get_vouchers

# Get latest vendor data
vendors_df = get_vendors()

# Get order data (auto-optimized)
orders_df = get_orders()

# Get voucher analysis
vouchers_df = get_vouchers()
```

### 3. Ultra-Fast Large Datasets
```python
from ofood_data import get_orders_fast, get_vouchers_fast

# 3M+ orders in 3-4 minutes with parallel processing
orders_df = get_orders_fast()

# Large voucher dataset with optimization
vouchers_df = get_vouchers_fast()
```

### 4. Saved Metabase Questions (NEW!)
```python
from ofood_data import get_question_data, get_question_data_fast

# Execute saved question (URL: /question/3132-x-net)
df = get_question_data(3132)  # Gets ALL rows, not just 2K limit!

# With parallel optimization
df = get_question_data_fast(3132)

# Multiple questions at once
results = get_multiple_questions([3132, 1234, 5678])
df_3132 = results[3132]
```

## 📋 Available Functions

### 🗺️ Core Data Functions
- `get_vendors()` - Latest vendor data with locations
- `get_orders()` - Comprehensive order mapping with customer analysis
- `get_vouchers()` - Voucher analysis with order data
- `get_tf_vendors()` - TapsiFood vendor mapping
- `get_tf_menu()` - TapsiFood menu items

### 📊 Parameterized Analysis
```python
# VDOM analysis for specific city and month
vdom_df = get_vdom(city_id=1, jalali_year=1403, jalali_month=8)

# Geolocation analysis for city
geo_df = get_geo(city_id=5)
```

### 💾 Saved Questions
- `get_question_data(question_id)` - Execute saved Metabase questions
- `get_question_data_fast(question_id)` - Ultra-fast question execution
- `get_multiple_questions([ids])` - Multiple questions at once

### 🚀 Ultra-Fast Functions
- `get_orders_fast()` - 3M+ orders with parallel processing
- `get_vouchers_fast()` - Large voucher datasets
- `get_large_dataset(query_name)` - Generic parallel processing

### 🛠️ Utilities
- `get_active_vendors()` - Only active vendors (filtered)
- `get_vendors_by_city(city_id)` - Vendors in specific city
- `get_all_data()` - All datasets in one call
- `test_connection()` - Verify connection works

## 🏗️ Architecture

### Connection Flow
```
Python Code ←→ Metabase API ←→ ClickHouse Database
```

### Parallel Processing Innovation
```
Traditional: Request → Wait → Request → Wait → ... (15 minutes)
Parallel:    6 Workers fetch different pages simultaneously (3 minutes)
```

### Auto-Optimization Strategy
- **< 50K rows**: Single query (5-10 seconds)
- **50K-500K rows**: Sequential pagination (30-120 seconds)
- **> 500K rows**: Parallel processing (60-240 seconds, 3-5x faster)

## 🔧 Configuration Options

### Team Database Selection
```python
# Different team databases
vendors_df = get_vendors(team='growth')    # Growth Team DB
orders_df = get_orders(team='data')        # Data Team DB
question_df = get_question_data(3132, team='product')  # Product Team DB
```

### Performance Modes
```python
# Auto-optimization (recommended)
df = get_orders()  # Automatically chooses best method

# Force specific optimization
df = get_orders_fast()  # Always use parallel processing
df = get_large_dataset('orders')  # Generic parallel handler
```

## 📈 Performance Features

### Intelligent Strategy Selection
The system automatically estimates data size and selects the optimal retrieval strategy:

1. **Size Estimation**: `COUNT(*)` query to determine total rows
2. **Strategy Selection**: Based on size, choose single/pagination/parallel
3. **Parallel Execution**: 6 workers fetch different page ranges simultaneously
4. **Data Combination**: Merge results in correct order

### Connection Management
- Each parallel worker maintains its own authenticated connection
- Proper session cleanup and error handling
- Memory-efficient processing with page-based combination

## 🗃️ Query Warehouse

The system includes a centralized query warehouse with pre-built, optimized queries:

```python
from query_warehouse import QueryRegistry, CoreQueries

# Pre-built queries
vendors_query = QueryRegistry.X_MAP_VENDOR()
orders_query = QueryRegistry.X_MAP_ORDER()

# Parameterized queries
vdom_query = CoreQueries.x_vdom(city_id=1, jalali_year=1403)
geo_query = CoreQueries.x_geo(city_id=5)
```

## 🔒 Security & Authentication

### Environment Variables (Recommended)
```bash
export METABASE_PASSWORD="your_password"
```

### Direct Configuration
```python
# Option 1: One-time setup
setup_credentials("your_password", "growth")

# Option 2: Per-function
df = get_vendors(password="your_password", team="growth")
```

## 🚨 Error Handling

The system includes comprehensive error handling:
- Authentication failures
- Network timeouts
- Data size estimation errors
- Parallel worker failures
- Connection cleanup

## 📚 Advanced Usage

### Custom Queries
```python
from metabase_clickhouse_app import MetabaseClient, MetabaseConfig

config = MetabaseConfig.create_with_team_db(
    url="https://metabase.ofood.cloud",
    username="your_username",
    password="your_password",
    team="growth"
)

client = MetabaseClient(config)
client.authenticate()

# Custom SQL with optimization
df = client.execute_query_optimized("SELECT * FROM live.vendors")
client.logout()
```

### Saved Questions with Parameters
```python
# Question with parameters
df = client.execute_saved_question(3132, parameters={"city_id": 1})
```

## 🔍 Monitoring & Logging

The system provides detailed logging for monitoring performance:
- Authentication status
- Query execution time
- Row counts retrieved
- Optimization decisions
- Parallel worker progress

## 📝 Data Outputs

All functions return pandas DataFrames with:
- Complete data (no artificial limits)
- Proper column names and types
- Memory-efficient structure
- Ready for analysis/export

## 🤝 Contributing

To add new queries:
1. Add query method to `CoreQueries` class in `query_warehouse.py`
2. Register in `QueryRegistry`
3. Create interface function in `ofood_data.py`
4. Update documentation

## 📞 Support

For questions about:
- **Usage**: Check function docstrings and examples
- **Performance**: Review optimization modes and strategies
- **Queries**: See query warehouse documentation
- **Errors**: Check logs for detailed error messages

## 🏷️ Version History

- **v1.0**: Basic Metabase connection and query execution
- **v2.0**: Added pagination support for large datasets
- **v3.0**: Parallel processing implementation (5x performance boost)
- **v4.0**: Saved question support and auto-optimization
- **v5.0**: Multi-team database support and query warehouse

## 🎯 Key Benefits

1. **Speed**: 3-5x faster data retrieval for large datasets
2. **Simplicity**: Single-line functions for complex data access
3. **Completeness**: No artificial row limits - get ALL your data
4. **Flexibility**: Support for both warehouse queries and saved questions
5. **Reliability**: Production-ready with error handling and cleanup
6. **Scalability**: Parallel processing scales with data size

---

**Ready to get started?** Try the quick setup and retrieve your first dataset in minutes!
