"""
Query Warehouse - Centralized Repository of ClickHouse Queries
============================================================
This module contains all pre-built queries organized by category.
Easy to add new queries and reuse across different scripts.
"""

from typing import Optional, List
from datetime import datetime, timedelta


class BaseQueries:
    """Base class for query categories"""
    
    @staticmethod
    def _format_date(date_obj: datetime) -> str:
        """Format date for ClickHouse queries"""
        return date_obj.strftime('%Y-%m-%d')
    
    @staticmethod
    def _get_date_range(days_back: int = 30):
        """Get date range for queries"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        return start_date, end_date


class CoreQueries(BaseQueries):
    """Core business queries for OFOOD"""
    
    @staticmethod
    def x_map_vendor() -> str:
        """
        Get latest vendor data with location information (renamed from original vendor query)
        Returns: vendor_code, vendor_name, city_id, radius, id, status_id, visible, open, latitude, longitude
        """
        return """
        WITH ranked_vendors AS (
          SELECT
            v.vendor_code,
            v.vendor_name,
            v.city_id,
            v.radius,
            v.id,
            v.status_id,
            v.visible,
            v.open,
            vl.latitude,
            vl.longitude,
            ROW_NUMBER()
              OVER (
                PARTITION BY v.vendor_code
                ORDER BY v.id DESC
              ) AS rn
          FROM live.vendors v
          LEFT JOIN live.vendor_location vl
            ON v.id = vl.id
          WHERE 1=1
        )
        SELECT
          vendor_code,
          vendor_name,
          city_id,
          radius,
          id,
          status_id,
          visible,
          open,
          latitude,
          longitude
        FROM ranked_vendors
        WHERE rn = 1
        ORDER BY vendor_code DESC
        """
    
    @staticmethod
    def x_map_order() -> str:
        """
        Comprehensive order mapping with customer analysis
        Returns: All order data with customer segmentation and business metrics
        """
        return """
        WITH all_orders AS (
            SELECT
                created_at,
                order_id,
                vendor_code,
                user_id,
                payable_price,
                city_id,
                business_line,
                marketing_area,
                voucher_value,
                voucher_id,
                dt_jalali_month_number,
                dt_jalali_year_number,
                dt_jalali_week_number,
                dt_jalali_weekday_number,
                dt_jalali_quarter_year,
                dt_jalali_day_number,
                tapsifood_share_discount,
                vendor_share_discount,
                packing_price,
                vendor_latitude,
                vendor_longitude,
                customer_latitude,
                customer_longitude,
                if(min(created_at) OVER (PARTITION BY user_id) = created_at, 1, 0) AS is_new_customer,
                if(min(created_at) OVER (PARTITION BY user_id) < created_at, 1, 0) AS returning,
                if(voucher_value = 0, 1, 0) AS organic,
                if(voucher_value > 0, 1, 0) AS non_organic,
                if(tapsifood_share_discount > 0, 1, 0) AS assisted,
                if(tapsifood_share_discount > 0 or vendor_share_discount > 0, 1, 0) AS dom,
                (total_price - vendor_share_discount + packing_price) AS aov_select
            FROM general_marts.gm_order
            WHERE final_payment_status = 'COMPLETED'
              AND final_order_status = 'SUCCESSFUL'
              AND is_test = 0
        ), filtered_orders AS (
            SELECT *
            FROM all_orders
            WHERE 1 = 1 
        )
        SELECT * FROM filtered_orders
        """
    
    @staticmethod
    def x_vdom(city_id: Optional[int] = None, jalali_year: int = 1403, jalali_month: int = 7) -> str:
        """
        Vendor DOM (Discount on Marketplace) analysis
        
        Args:
            city_id: Optional city filter
            jalali_year: Jalali year (default: 1403)
            jalali_month: Jalali month (default: 7)
        """
        city_filter = f"AND city_id = {city_id}" if city_id else ""
        
        return f"""
        WITH all_orders AS (
            SELECT
                created_at,
                order_id,
                user_id,
                total_price,
                payable_price,
                city_id,
                business_line,
                marketing_area,
                voucher_value,
                dt_jalali_month_number,
                dt_jalali_year_number,
                tapsifood_share_discount,
                vendor_share_discount,
                packing_price,
                vendor_code,
                vendor_name,
                if(min(created_at) OVER (PARTITION BY user_id) = created_at, 1, 0) AS is_new_customer,
                if(min(created_at) OVER (PARTITION BY user_id) < created_at, 1, 0) AS returning,
                if(voucher_value = 0, 1, 0) AS organic,
                if(voucher_value > 0, 1, 0) AS non_organic,
                (total_price - vendor_share_discount + packing_price) AS nmv_select_1,
                payable_price - vendor_share_discount AS nmv_select_2
            FROM general_marts.gm_order
            WHERE final_payment_status = 'COMPLETED'
              AND final_order_status = 'SUCCESSFUL'
              AND is_test = 0
        ), filtered_orders AS (
            SELECT *
            FROM all_orders
            WHERE 1 = 1 
            AND dt_jalali_year_number = {jalali_year}
            AND dt_jalali_month_number = {jalali_month}
            {city_filter}
        )
        SELECT * FROM filtered_orders
        """
    
    @staticmethod
    def x_net_live_vouchers() -> str:
        """
        Comprehensive voucher analysis with order data
        Returns: Orders with voucher details and usage constraints
        """
        return """
        WITH vouchers AS (
            SELECT
                CAST(id AS String) AS voucher_id,
                type,
                code,
                discount_strategy,
                discount_amount,
                discount_max_amount,
                used_count,
                start_at,
                end_at,
                JSONExtract(orders_constraints, 'minValue', 'UInt32') AS orders_minValue,
                JSONExtract(usage_constraints, 'total', 'UInt32') AS uc_usage_total
            FROM live.vouchers
        ),
        all_orders AS (
            SELECT 
                o.created_at,
                o.order_id,
                o.vendor_id,
                o.user_id,
                o.total_price,
                o.city_id,
                o.business_line,
                o.marketing_area,
                o.final_order_status,
                o.final_payment_status,
                o.is_test,
                o.voucher_value,
                o.voucher_id,
                o.dt_jalali_month_number,
                o.dt_jalali_year_number,
                o.dt_jalali_week_number,
                o.tapsifood_share_discount,
                o.vendor_share_discount,
                o.packing_price,
                CAST(CASE WHEN o.final_payment_status = 'COMPLETED' AND o.final_order_status = 'SUCCESSFUL' THEN 1 ELSE 0 END AS UInt8) AS net,
                CASE WHEN MIN(o.created_at) OVER (PARTITION BY o.user_id) = o.created_at THEN 1 ELSE 0 END AS is_new_customer,
                CASE WHEN MIN(o.created_at) OVER (PARTITION BY o.user_id) < o.created_at THEN 1 ELSE 0 END AS returning,
                ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.created_at) AS rn,
                CASE WHEN o.voucher_value = 0 THEN 1 ELSE 0 END AS organic,
                CASE WHEN o.voucher_value > 0 THEN 1 ELSE 0 END AS non_organic,
                CASE WHEN o.tapsifood_share_discount > 0 THEN 1 ELSE 0 END AS assisted,
                (o.total_price - o.vendor_share_discount + o.packing_price) AS aov_select,
                v.type,
                v.code,
                v.discount_strategy,
                v.discount_amount,
                v.discount_max_amount,
                v.used_count,
                v.start_at,
                v.end_at,
                v.orders_minValue,
                v.uc_usage_total
            FROM general_marts.gm_order o
            LEFT JOIN vouchers v ON o.voucher_id = v.voucher_id
            WHERE o.final_payment_status = 'COMPLETED' 
              AND o.final_order_status = 'SUCCESSFUL' 
              AND o.is_test = 0
        ),
        deduped_orders AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        created_at,
                        order_id,
                        vendor_id,
                        user_id,
                        total_price,
                        city_id,
                        business_line,
                        marketing_area,
                        final_order_status,
                        final_payment_status,
                        is_test,
                        voucher_value,
                        voucher_id,
                        dt_jalali_month_number,
                        dt_jalali_year_number,
                        dt_jalali_week_number,
                        tapsifood_share_discount,
                        vendor_share_discount,
                        packing_price,
                        net,
                        is_new_customer,
                        returning,
                        rn,
                        organic,
                        non_organic,
                        assisted,
                        aov_select,
                        type,
                        code,
                        discount_strategy,
                        discount_amount,
                        discount_max_amount,
                        start_at,
                        end_at,
                        orders_minValue,
                        uc_usage_total
                    ORDER BY used_count DESC
                ) AS row_num
            FROM all_orders
        )
        SELECT *
        FROM deduped_orders
        WHERE row_num = 1
        """
    
    @staticmethod
    def x_geo(city_id: Optional[int] = None) -> str:
        """
        Geolocation-based order analysis
        
        Args:
            city_id: Optional city filter
        """
        city_filter = f"WHERE city_id = {city_id}" if city_id else ""
        
        return f"""
        WITH all_orders AS (
            SELECT 
                created_at,
                order_id,
                vendor_id,
                user_id,
                total_price,
                city_id,
                business_line,
                marketing_area,
                voucher_value,
                voucher_id,
                dt_jalali_month_number,
                dt_jalali_year_number,
                dt_jalali_week_number,
                dt_jalali_weekday_number,
                dt_jalali_quarter_year,
                dt_jalali_day_number,
                tapsifood_share_discount,
                vendor_share_discount,
                packing_price,
                vendor_code,
                customer_longitude,
                customer_latitude,
                CAST(CASE WHEN final_payment_status = 'COMPLETED' AND final_order_status = 'SUCCESSFUL' THEN 1 ELSE 0 END AS UInt8) AS net,
                CAST(CASE WHEN final_payment_status IN ('COMPLETED', 'REVERSE', 'REFUNDED') AND (cancel_reason != 'NEW_ORDER_NEED_FOR_CALL_ORDER' OR cancel_reason IS NULL) THEN 1 ELSE 0 END AS UInt8) AS gross,
                CASE WHEN MIN(created_at) OVER (PARTITION BY user_id) = created_at THEN 1 ELSE 0 END AS is_new_customer,
                CASE WHEN MIN(created_at) OVER (PARTITION BY user_id) < created_at THEN 1 ELSE 0 END AS returning,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) AS rn,
                CASE WHEN voucher_value = 0 THEN 1 ELSE 0 END AS organic,
                CASE WHEN voucher_value > 0 THEN 1 ELSE 0 END AS non_organic,
                CASE WHEN tapsifood_share_discount > 0 THEN 1 ELSE 0 END AS assisted,
                (total_price - vendor_share_discount + packing_price) AS aov_select
            FROM 
                general_marts.gm_order
            WHERE 
                final_payment_status = 'COMPLETED' 
                AND final_order_status = 'SUCCESSFUL'
                AND is_test = 0
        ),
        filtered_orders AS (
            SELECT *
            FROM all_orders
            {city_filter}
        )
        SELECT * from filtered_orders
        """
    
    @staticmethod
    def tf_vendors() -> str:
        """
        TapsiFood vendor mapping with SnappFood cross-reference
        Returns: TF vendor data with SF codes and business details
        """
        return """
        SELECT
            any(c.id) AS id,
            c.vendor_code AS tf_code,
            anyIf(
                coalesce(
                    nullIf(extract(c.other_platform_link,'-r-([A-Za-z0-9]+)'),''),
                    nullIf(extract(c.other_platform_link,'-z-([A-Za-z0-9]+)'),''),
                    nullIf(extract(c.other_platform_link,'/menu/([A-Za-z0-9]+)'),'')
                ),c.other_platform_link LIKE '%snappfood%'
            ) AS sf_code,
            any(c.vendor_name) AS tf_name,
            any(c.city_id) AS city_id,
            any(v.latitude) AS tf_latitude,
            any(v.longitude) AS tf_longitude,
            any(c.business_type_id) AS business_type_id,
            any(c.marketing_area_name) AS marketing_area,
            any(c.minimum_basket) AS min_order,
            CASE any(c.business_type_id)
                WHEN 1 THEN 'Restaurant'
                WHEN 2 THEN 'Cafe'
                WHEN 3 THEN 'Bakery'
                WHEN 4 THEN 'Pastry'
                WHEN 5 THEN 'Meat Shop'
                WHEN 6 THEN 'Fruit Shop'
                WHEN 7 THEN 'Ice Cream and Juice Shop'
                ELSE 'Other'
            END AS business_line,
            any(e.address) AS address,
            anyIf(c.other_platform_link, c.other_platform_link LIKE '%snappfood%') AS other_platform_link
        FROM live.vendor_location AS v
        INNER JOIN live.vendors AS c
            ON c.id = v.id
        INNER JOIN live.vendor_extra_infos AS e
            ON e.id = c.id
        WHERE c.status_id = 5
        GROUP BY c.vendor_code
        """
    
    @staticmethod
    def tf_menu() -> str:
        """
        TapsiFood menu items with pricing and discounts
        Returns: Complete menu structure with items, categories, and pricing
        """
        return """
        SELECT
            vendor_code AS tf_code,
            vendor_product_header_id AS item_id,
            product_category_name AS category_name,
            product_category_id AS category_id,
            vendor_product_header_description AS item_description,
            product_price AS price,
            product_discount_ratio AS discount_ratio,
            (product_price - product_price_after_discount) AS discount_amount,
            CASE
              WHEN product_name = vendor_product_header_name THEN
                vendor_product_header_name
              WHEN startsWith(product_name, vendor_product_header_name) THEN
                concat(
                  vendor_product_header_name,
                  ' ',
                  trim(substring(
                    product_name,
                    lengthUTF8(vendor_product_header_name) + 1
                  ))
                )
              ELSE
                concat(vendor_product_header_name, ' ', product_name)
            END AS item_title
        FROM live.vw_product_vendor
        WHERE vendor_status_id = 5
          AND vendor_product_is_approved = 1
        """


# ============================================================================
# Query Registry - Easy access to all queries
# ============================================================================

class QueryRegistry:
    """Central registry of all available queries"""
    
    # Core Business Queries
    X_MAP_VENDOR = CoreQueries.x_map_vendor
    X_MAP_ORDER = CoreQueries.x_map_order
    X_VDOM = CoreQueries.x_vdom
    X_NET_LIVE_VOUCHERS = CoreQueries.x_net_live_vouchers
    X_GEO = CoreQueries.x_geo
    TF_VENDORS = CoreQueries.tf_vendors
    TF_MENU = CoreQueries.tf_menu
    
    @classmethod
    def list_all_queries(cls) -> dict:
        """Get all available queries organized by category"""
        return {
            'mapping': {
                'vendor': cls.X_MAP_VENDOR,
                'order': cls.X_MAP_ORDER,
            },
            'analysis': {
                'vdom': cls.X_VDOM,
                'geo': cls.X_GEO,
                'vouchers': cls.X_NET_LIVE_VOUCHERS,
            },
            'reference': {
                'tf_vendors': cls.TF_VENDORS,
                'tf_menu': cls.TF_MENU,
            }
        }
    
    @classmethod
    def print_available_queries(cls):
        """Print all available queries for easy reference"""
        
        print("📋 OFOOD Query Warehouse - Available Queries:")
        print("=" * 60)
        
        print("\n🗺️  MAPPING QUERIES:")
        print("   • X_MAP_VENDOR     : QueryRegistry.X_MAP_VENDOR")
        print("   • X_MAP_ORDER      : QueryRegistry.X_MAP_ORDER")
        
        print("\n📊 ANALYSIS QUERIES:")
        print("   • X_VDOM           : QueryRegistry.X_VDOM")
        print("   • X_GEO            : QueryRegistry.X_GEO")  
        print("   • X_NET_LIVE_VOUCHERS : QueryRegistry.X_NET_LIVE_VOUCHERS")
        
        print("\n📚 REFERENCE QUERIES:")
        print("   • TF_VENDORS       : QueryRegistry.TF_VENDORS")
        print("   • TF_MENU          : QueryRegistry.TF_MENU")
        
        print("\n💡 USAGE EXAMPLES:")
        print("-" * 30)
        print("Basic usage:")
        print("   extractor.execute_query_from_warehouse(QueryRegistry.X_MAP_VENDOR)")
        
        print("\nWith parameters:")
        print("   extractor.execute_query_from_warehouse(QueryRegistry.X_VDOM, city_id=1)")
        print("   extractor.execute_query_from_warehouse(QueryRegistry.X_GEO, city_id=5)")
        
        print("\nCustom parameters:")
        print("   extractor.execute_query_from_warehouse(")
        print("       CoreQueries.x_vdom, ")
        print("       city_id=1, jalali_year=1403, jalali_month=8")
        print("   )")
        
        print("\n🏷️  QUERY DESCRIPTIONS:")
        print("-" * 30)
        descriptions = {
            'X_MAP_VENDOR': 'Latest vendor data with locations',
            'X_MAP_ORDER': 'Comprehensive order mapping with customer analysis',
            'X_VDOM': 'Vendor DOM (Discount on Marketplace) analysis',
            'X_NET_LIVE_VOUCHERS': 'Comprehensive voucher analysis with orders',
            'X_GEO': 'Geolocation-based order analysis',
            'TF_VENDORS': 'TapsiFood vendors with SnappFood cross-reference',
            'TF_MENU': 'TapsiFood menu items with pricing and discounts'
        }
        
        for query, desc in descriptions.items():
            print(f"   • {query:<20}: {desc}")


if __name__ == "__main__":
    # Print all available queries when run directly
    QueryRegistry.print_available_queries()