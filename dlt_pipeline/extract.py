"""DLT pipeline to extract data from fast-api-jaffle-shop."""

import dlt
from dlt.sources.rest_api import rest_api_source


def create_jaffle_shop_pipeline():
    """Create and configure the jaffle shop data pipeline."""
    
    # Configure the REST API source
    source = rest_api_source({
        "client": {
            "base_url": "https://jaffle-shop.dlthub.com/"
        },
        "resources": [
            {
                "name": "customers",
                "endpoint": {
                    "path": "api/v1/customers",
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
                },
            },
        ],
    })
    
    return source


def run_pipeline():
    """Run the data extraction pipeline."""
    print("🚀 Starting jaffle shop data extraction...")
    
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="raw_jaffle_shop",
        dev_mode=False,
    )
    
    # Get the source
    source = create_jaffle_shop_pipeline()
    
    # Run the pipeline with replace mode to avoid duplicates
    load_info = pipeline.run(source, write_disposition="replace")
    
    print("✅ Pipeline completed successfully!")
    print(f"📊 Load info: {load_info}")
    
    return load_info


if __name__ == "__main__":
    run_pipeline()