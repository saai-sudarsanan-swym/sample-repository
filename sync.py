import os
import shopify
import sqlite3
from datetime import datetime
from typing import List, Dict, Any

# Initialize Shopify API
def init_shopify(shop_url: str, access_token: str) -> None:
    """Initialize the Shopify API client."""
    shopify.Session.setup(api_key=os.getenv('SHOPIFY_API_KEY'), secret=os.getenv('SHOPIFY_API_SECRET'))
    session = shopify.Session(shop_url, '2024-01', access_token)
    shopify.ShopifyResource.activate_session(session)

def get_all_products() -> List[Dict[Any, Any]]:
    """Fetch all products from Shopify."""
    products = []
    page = 1
    
    while True:
        batch = shopify.Product.find(limit=250, page=page)
        if not batch:
            break
            
        products.extend(batch)
        page += 1
        
    return products

def init_db(db_path: str = 'shopify_products.db') -> sqlite3.Connection:
    """Initialize SQLite database and create products table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            title TEXT,
            description TEXT,
            vendor TEXT,
            product_type TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            published_at TIMESTAMP,
            status TEXT,
            price REAL,
            compare_at_price REAL,
            sku TEXT,
            inventory_quantity INTEGER,
            last_synced_at TIMESTAMP
        )
    ''')
    
    conn.commit()
    return conn

def sync_products_to_db(products: List[Dict[Any, Any]], conn: sqlite3.Connection) -> None:
    """Sync products to the database."""
    cursor = conn.cursor()
    current_time = datetime.utcnow()
    
    for product in products:
        # Get the first variant for price information
        variant = product.variants[0] if product.variants else None
        
        cursor.execute('''
            INSERT OR REPLACE INTO products (
                id, title, description, vendor, product_type,
                created_at, updated_at, published_at, status,
                price, compare_at_price, sku, inventory_quantity,
                last_synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            product.id,
            product.title,
            product.body_html,
            product.vendor,
            product.product_type,
            product.created_at,
            product.updated_at,
            product.published_at,
            product.status,
            variant.price if variant else None,
            variant.compare_at_price if variant else None,
            variant.sku if variant else None,
            variant.inventory_quantity if variant else None,
            current_time
        ))
    
    conn.commit()

def get_product_details(product_id: int) -> Dict[Any, Any]:
    """
    Fetch detailed information about a specific product from Shopify.
    
    Args:
        product_id (int): The ID of the product to fetch
        
    Returns:
        Dict containing detailed product information including variants, images, and other metadata
    """
    try:
        product = shopify.Product.find(product_id)
        return {
            'id': product.id,
            'title': product.title,
            'description': product.body_html,
            'vendor': product.vendor,
            'product_type': product.product_type,
            'created_at': product.created_at,
            'updated_at': product.updated_at,
            'published_at': product.published_at,
            'status': product.status,
            'variants': [{
                'id': variant.id,
                'title': variant.title,
                'price': variant.price,
                'compare_at_price': variant.compare_at_price,
                'sku': variant.sku,
                'inventory_quantity': variant.inventory_quantity,
                'weight': variant.weight,
                'weight_unit': variant.weight_unit
            } for variant in product.variants],
            'images': [{
                'id': image.id,
                'src': image.src,
                'alt': image.alt,
                'position': image.position
            } for image in product.images],
            'tags': product.tags.split(', ') if product.tags else [],
            'options': [{
                'name': option.name,
                'values': option.values
            } for option in product.options]
        }
    except Exception as e:
        raise Exception(f"Failed to fetch product details for ID {product_id}: {str(e)}")

def main():
    # These should be set as environment variables
    shop_url = os.getenv('SHOPIFY_SHOP_URL')
    access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
    
    if not all([shop_url, access_token]):
        raise ValueError("Missing required environment variables: SHOPIFY_SHOP_URL, SHOPIFY_ACCESS_TOKEN")
    
    # Initialize Shopify API
    init_shopify(shop_url, access_token)
    
    # Initialize database
    conn = init_db()
    
    try:
        # Fetch products from Shopify
        products = get_all_products()
        
        # Sync products to database
        sync_products_to_db(products, conn)
        
        print(f"Successfully synced {len(products)} products to database")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
