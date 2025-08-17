#!/usr/bin/env python3
"""
Simplified DLT pipeline for testing - extracts just a sample of data.
"""

import dlt
from dlt.sources.rest_api import rest_api_source


def create_sample_pipeline():
    """Create a sample pipeline with limited data."""
    
    source = rest_api_source({
        "client": {
            "base_url": "https://jaffle-shop.dlthub.com/"
        },
        "resources": [
            {
                "name": "customers",
                "endpoint": {
                    "path": "api/v1/customers",
                    "paginator": {
                        "type": "header_link",
                        "links_next_key": "next",
                        "maximum_page": 2  # Limit to first 2 pages
                    }
                },
            },
            {
                "name": "products", 
                "endpoint": {
                    "path": "api/v1/products",
                },
            },
            {
                "name": "stores",
                "endpoint": {
                    "path": "api/v1/stores", 
                },
            },
            {
                "name": "supplies",
                "endpoint": {
                    "path": "api/v1/supplies",
                },
            },
            {
                "name": "orders",
                "endpoint": {
                    "path": "api/v1/orders",
                    "paginator": {
                        "type": "header_link", 
                        "links_next_key": "next",
                        "maximum_page": 5  # Limit to first 5 pages for testing
                    }
                },
            },
        ],
    })
    
    return source


def run_sample_pipeline():
    """Run the sample data extraction pipeline."""
    print("🚀 Starting sample jaffle shop data extraction...")
    
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_sample",
        destination="duckdb",
        dataset_name="raw_jaffle_shop",
        dev_mode=False,
    )
    
    # Get the source
    source = create_sample_pipeline()
    
    # Run the pipeline
    load_info = pipeline.run(source)
    
    print(f"✅ Sample pipeline completed successfully!")
    print(f"📊 Load info: {load_info}")
    
    return load_info


if __name__ == "__main__":
    run_sample_pipeline()