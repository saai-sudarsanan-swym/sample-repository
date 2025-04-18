import os
import unittest
import sqlite3
from unittest.mock import patch, MagicMock
from datetime import datetime
from sync import (
    init_shopify,
    get_all_products,
    init_db,
    transform_product_data,
    sync_products_to_db,
    trigger_sync
)

class TestShopifySync(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test."""
        # Set test environment variables
        os.environ['SHOPIFY_SHOP_URL'] = 'test-shop.myshopify.com'
        os.environ['SHOPIFY_ACCESS_TOKEN'] = 'test-token'
        os.environ['SHOPIFY_API_KEY'] = 'test-api-key'
        os.environ['SHOPIFY_API_SECRET'] = 'test-api-secret'
        
        # Use in-memory SQLite database for tests
        self.db_path = ':memory:'
        
        # Sample Shopify product data
        self.sample_product = {
            'id': 123456789,
            'title': 'Test Product',
            'body_html': '<p>Test Description</p>',
            'vendor': 'Test Vendor',
            'product_type': 'Test Type',
            'created_at': '2024-03-14T10:00:00Z',
            'updated_at': '2024-03-14T11:00:00Z',
            'published_at': '2024-03-14T12:00:00Z',
            'status': 'active',
            'variants': [{
                'id': 987654321,
                'title': 'Default Title',
                'price': '19.99',
                'compare_at_price': '24.99',
                'sku': 'TEST-SKU-123',
                'inventory_quantity': 100,
                'weight': 1.0,
                'weight_unit': 'kg'
            }]
        }

    def tearDown(self):
        """Clean up after each test."""
        # Clear environment variables
        for key in ['SHOPIFY_SHOP_URL', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_API_KEY', 'SHOPIFY_API_SECRET']:
            if key in os.environ:
                del os.environ[key]

    @patch('shopify.Session.setup')
    @patch('shopify.Session')
    @patch('shopify.ShopifyResource.activate_session')
    def test_init_shopify(self, mock_activate_session, mock_session, mock_setup):
        """Test Shopify API initialization."""
        # Arrange
        shop_url = 'test-shop.myshopify.com'
        access_token = 'test-token'
        
        # Act
        init_shopify(shop_url, access_token)
        
        # Assert
        mock_setup.assert_called_once_with(api_key='test-api-key', secret='test-api-secret')
        mock_session.assert_called_once_with(shop_url, '2024-01', access_token)
        mock_activate_session.assert_called_once()

    @patch('shopify.Product.find')
    def test_get_all_products(self, mock_product_find):
        """Test fetching all products from Shopify."""
        # Arrange
        mock_product_find.side_effect = [
            [self.sample_product],  # First page
            []  # Second page (empty)
        ]
        
        # Act
        products = get_all_products()
        
        # Assert
        self.assertEqual(len(products), 1)
        self.assertEqual(products[0]['id'], self.sample_product['id'])
        mock_product_find.assert_called()

    def test_init_db(self):
        """Test database initialization."""
        # Act
        conn = init_db(self.db_path)
        
        # Assert
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check table schema
        cursor.execute("PRAGMA table_info(products)")
        columns = {row[1] for row in cursor.fetchall()}
        expected_columns = {
            'id', 'title', 'description', 'vendor', 'product_type',
            'created_at', 'updated_at', 'published_at', 'status',
            'price', 'compare_at_price', 'sku', 'inventory_quantity',
            'last_synced_at'
        }
        self.assertEqual(columns, expected_columns)
        
        conn.close()

    def test_transform_product_data(self):
        """Test product data transformation."""
        # Act
        transformed_data = transform_product_data(self.sample_product)
        
        # Assert
        self.assertEqual(transformed_data['id'], self.sample_product['id'])
        self.assertEqual(transformed_data['title'], self.sample_product['title'])
        self.assertEqual(transformed_data['description'], self.sample_product['body_html'])
        self.assertEqual(transformed_data['vendor'], self.sample_product['vendor'])
        self.assertEqual(transformed_data['product_type'], self.sample_product['product_type'])
        self.assertEqual(transformed_data['status'], self.sample_product['status'])
        self.assertEqual(float(transformed_data['price']), float(self.sample_product['variants'][0]['price']))
        self.assertEqual(float(transformed_data['compare_at_price']), float(self.sample_product['variants'][0]['compare_at_price']))
        self.assertEqual(transformed_data['sku'], self.sample_product['variants'][0]['sku'])
        self.assertEqual(transformed_data['inventory_quantity'], self.sample_product['variants'][0]['inventory_quantity'])

    def test_sync_products_to_db(self):
        """Test syncing products to database."""
        # Arrange
        conn = init_db(self.db_path)
        products = [self.sample_product]
        
        # Act
        sync_products_to_db(products, conn)
        
        # Assert
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM products WHERE id = ?", (self.sample_product['id'],))
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        self.assertEqual(row[0], self.sample_product['id'])  # id
        self.assertEqual(row[1], self.sample_product['title'])  # title
        self.assertEqual(row[2], self.sample_product['body_html'])  # description
        self.assertEqual(row[3], self.sample_product['vendor'])  # vendor
        self.assertEqual(row[4], self.sample_product['product_type'])  # product_type
        self.assertEqual(float(row[9]), float(self.sample_product['variants'][0]['price']))  # price
        self.assertEqual(float(row[10]), float(self.sample_product['variants'][0]['compare_at_price']))  # compare_at_price
        self.assertEqual(row[11], self.sample_product['variants'][0]['sku'])  # sku
        self.assertEqual(row[12], self.sample_product['variants'][0]['inventory_quantity'])  # inventory_quantity
        
        conn.close()

    @patch('sync.init_shopify')
    @patch('sync.get_all_products')
    @patch('sync.init_db')
    @patch('sync.sync_products_to_db')
    def test_trigger_sync(self, mock_sync_products, mock_init_db, mock_get_products, mock_init_shopify):
        """Test the complete sync trigger process."""
        # Arrange
        mock_get_products.return_value = [self.sample_product]
        mock_conn = MagicMock()
        mock_init_db.return_value = mock_conn
        
        # Act
        result = trigger_sync()
        
        # Assert
        self.assertEqual(result['status'], 'success')
        mock_init_shopify.assert_called_once()
        mock_get_products.assert_called_once()
        mock_init_db.assert_called_once()
        mock_sync_products.assert_called_once_with([self.sample_product], mock_conn)
        mock_conn.close.assert_called_once()

    def test_transform_product_data_with_missing_fields(self):
        """Test product data transformation with missing fields."""
        # Arrange
        incomplete_product = {
            'id': 123456789,
            'title': 'Test Product',
            # Missing other fields
        }
        
        # Act
        transformed_data = transform_product_data(incomplete_product)
        
        # Assert
        self.assertEqual(transformed_data['id'], incomplete_product['id'])
        self.assertEqual(transformed_data['title'], incomplete_product['title'])
        self.assertIsNone(transformed_data['description'])
        self.assertIsNone(transformed_data['vendor'])
        self.assertIsNone(transformed_data['product_type'])
        self.assertIsNone(transformed_data['price'])
        self.assertIsNone(transformed_data['compare_at_price'])
        self.assertIsNone(transformed_data['sku'])
        self.assertIsNone(transformed_data['inventory_quantity'])

if __name__ == '__main__':
    unittest.main() 