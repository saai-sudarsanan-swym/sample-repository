import os
import shopify
import sqlite3
from datetime import datetime
from typing import List, Dict, Any
from flask import Flask, jsonify
import threading
import logging
from logging.handlers import RotatingFileHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add file handler for persistent logs
file_handler = RotatingFileHandler('shopify_sync.log', maxBytes=1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

app = Flask(__name__)

# Initialize Shopify API
def init_shopify(shop_url: str, access_token: str) -> None:
    """Initialize the Shopify API client."""
    try:
        shopify.Session.setup(api_key=os.getenv('SHOPIFY_API_KEY'), secret=os.getenv('SHOPIFY_API_SECRET'))
        session = shopify.Session(shop_url, '2024-01', access_token)
        shopify.ShopifyResource.activate_session(session)
        logger.info(f"Successfully initialized Shopify API for shop: {shop_url}")
    except Exception as e:
        logger.error(f"Failed to initialize Shopify API: {str(e)}")
        raise

def get_all_products() -> List[Dict[Any, Any]]:
    """Fetch all products from Shopify."""
    products = []
    page = 1
    
    try:
        while True:
            logger.info(f"Fetching products page {page}")
            batch = shopify.Product.find(limit=250, page=page)
            if not batch:
                break
                
            products.extend(batch)
            logger.info(f"Retrieved {len(batch)} products from page {page}")
            page += 1
            
        logger.info(f"Successfully fetched total {len(products)} products")
        return products
    except Exception as e:
        logger.error(f"Error fetching products: {str(e)}")
        raise

def init_db(db_path: str = 'shopify_products.db') -> sqlite3.Connection:
    """Initialize SQLite database and create products table if it doesn't exist."""
    try:
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
        logger.info("Successfully initialized database")
        return conn
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

def transform_product_data(product: Dict[Any, Any]) -> Dict[str, Any]:
    """
    Transform Shopify product data to match our database schema.
    
    Args:
        product (Dict): Raw Shopify product data
        
    Returns:
        Dict containing transformed data matching our schema
    """
    try:
        # Get the first variant for price and inventory information
        variant = product.get('variants', [{}])[0] if product.get('variants') else {}
        
        transformed_data = {
            'id': product.get('id'),
            'title': product.get('title'),
            'description': product.get('body_html'),
            'vendor': product.get('vendor'),
            'product_type': product.get('product_type'),
            'created_at': product.get('created_at'),
            'updated_at': product.get('updated_at'),
            'published_at': product.get('published_at'),
            'status': product.get('status'),
            'price': float(variant.get('price', 0)) if variant.get('price') else None,
            'compare_at_price': float(variant.get('compare_at_price', 0)) if variant.get('compare_at_price') else None,
            'sku': variant.get('sku'),
            'inventory_quantity': variant.get('inventory_quantity'),
            'last_synced_at': datetime.utcnow()
        }
        
        logger.debug(f"Successfully transformed product data for ID: {transformed_data['id']}")
        return transformed_data
    except Exception as e:
        logger.error(f"Error transforming product data: {str(e)}")
        raise

def sync_products_to_db(products: List[Dict[Any, Any]], conn: sqlite3.Connection) -> None:
    """Sync products to the database."""
    cursor = conn.cursor()
    current_time = datetime.utcnow()
    success_count = 0
    error_count = 0
    
    try:
        for product in products:
            try:
                # Transform the product data to match our schema
                transformed_data = transform_product_data(product)
                
                cursor.execute('''
                    INSERT OR REPLACE INTO products (
                        id, title, description, vendor, product_type,
                        created_at, updated_at, published_at, status,
                        price, compare_at_price, sku, inventory_quantity,
                        last_synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    transformed_data['id'],
                    transformed_data['title'],
                    transformed_data['description'],
                    transformed_data['vendor'],
                    transformed_data['product_type'],
                    transformed_data['created_at'],
                    transformed_data['updated_at'],
                    transformed_data['published_at'],
                    transformed_data['status'],
                    transformed_data['price'],
                    transformed_data['compare_at_price'],
                    transformed_data['sku'],
                    transformed_data['inventory_quantity'],
                    transformed_data['last_synced_at']
                ))
                success_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error syncing product {product.get('id', 'unknown')}: {str(e)}")
                continue
        
        conn.commit()
        logger.info(f"Sync completed. Successfully synced {success_count} products. Failed: {error_count}")
        
    except Exception as e:
        logger.error(f"Database error during sync: {str(e)}")
        conn.rollback()
        raise

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

def trigger_sync():
    """Trigger the sync process in a background thread."""
    try:
        # These should be set as environment variables
        shop_url = os.getenv('SHOPIFY_SHOP_URL')
        access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        
        if not all([shop_url, access_token]):
            error_msg = "Missing required environment variables: SHOPIFY_SHOP_URL, SHOPIFY_ACCESS_TOKEN"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("Starting sync process")
        
        # Initialize Shopify API
        init_shopify(shop_url, access_token)
        
        # Initialize database
        conn = init_db()
        
        try:
            # Fetch products from Shopify
            products = get_all_products()
            
            # Sync products to database
            sync_products_to_db(products, conn)
            
            result = {"status": "success", "message": f"Successfully synced {len(products)} products to database"}
            logger.info(result["message"])
            return result
            
        finally:
            conn.close()
            logger.info("Database connection closed")
            
    except Exception as e:
        error_msg = f"Sync process failed: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

@app.route('/sync/trigger', methods=['POST'])
def sync_trigger():
    """Endpoint to trigger the sync process."""
    try:
        # Start sync in a background thread
        thread = threading.Thread(target=trigger_sync)
        thread.start()
        
        logger.info("Sync process initiated via API endpoint")
        return jsonify({
            "status": "started",
            "message": "Sync process has been initiated"
        })
    except Exception as e:
        error_msg = f"Failed to start sync process: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            "status": "error",
            "message": error_msg
        }), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
