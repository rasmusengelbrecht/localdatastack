# Local Data Stack

An end-to-end local data stack featuring dlt, DuckDB, dbt, and Evidence.dev for extracting, transforming, and visualizing data from the Jaffle Shop API.

## 🏗️ Architecture

- **dlt**: Extracts data from fast-api-jaffle-shop API
- **DuckDB**: Local analytical database for data storage  
- **dbt**: Transforms raw data into staging, intermediate, and mart layers
- **Evidence.dev**: Interactive dashboards and visualizations
- **UV**: Python package management and virtual environments

## 📁 Project Structure

```
localdatastack/
├── pyproject.toml              # UV project configuration
├── run.sh                      # Simple execution script
├── localdatastack/
│   └── run.py                  # Main orchestration script
├── dlt_pipeline/
│   ├── extract.py              # dlt extraction pipeline
│   └── .dlt/                   # dlt configuration
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/            # 1:1 with source tables
│       ├── intermediate/       # Business logic layer
│       └── marts/              # Final analytical models
├── evidence_dashboard/
│   ├── package.json
│   ├── pages/                  # Dashboard pages
│   └── sources/                # SQL queries for Evidence
└── data/
    └── jaffle_shop.duckdb      # DuckDB database file
```

## 🚀 Quick Start

### Prerequisites

- [UV](https://docs.astral.sh/uv/) installed
- [Node.js](https://nodejs.org/) (for Evidence.dev)

### Run Everything

```bash
# Clone and navigate to project
cd localdatastack

# Run the complete pipeline (extract → transform → dashboard)
./run.sh
```

This single command will:
1. Install all dependencies
2. Extract data from the Jaffle Shop API
3. Transform data using dbt
4. Start the Evidence dashboard at http://localhost:3000

### Individual Steps

You can also run individual parts of the pipeline:

```bash
# Setup only
./run.sh --setup

# Extract data only
./run.sh --extract

# Transform data only  
./run.sh --transform

# Start dashboard only
./run.sh --dashboard

# Skip setup (if already done)
./run.sh --skip-setup
```

## 📊 Data Models

### Staging Layer
- `stg_customers`: Clean customer data
- `stg_orders`: Clean order data
- `stg_products`: Clean product catalog
- `stg_stores`: Clean store information
- `stg_supplies`: Clean supply data

### Intermediate Layer
- `int_customer_orders`: Joins customers with their orders

### Marts Layer
- `dim_customers`: Customer dimension with segments and metrics
- `dim_products`: Product dimension with price tiers
- `fct_orders`: Order facts with customer context

## 🎯 Dashboard Features

The Evidence dashboard includes:

- **Overview**: Key metrics and trends
- **Customer Analysis**: Segmentation, cohorts, and behavior
- **Order Analysis**: Status distribution, patterns by day/time
- **Product Analysis**: Catalog overview, price distribution

## 🔧 Development

### Manual dlt Pipeline
```bash
cd dlt_pipeline
uv run python extract.py
```

### Manual dbt Runs
```bash
cd dbt_project
uv run dbt run
uv run dbt test
```

### Evidence Development
```bash
cd evidence_dashboard
npm run dev
```

## 🧪 Testing

```bash
# Test dbt models
cd dbt_project
uv run dbt test

# Check data quality
uv run dbt run --full-refresh
```

## 📝 Configuration

### dlt Configuration
- API endpoint: `https://jaffle-shop.dlthub.com/`
- Database: `data/jaffle_shop.duckdb`
- Schemas: Raw data loaded to `raw_jaffle_shop` schema

### dbt Configuration  
- Database: DuckDB at `../data/jaffle_shop.duckdb`
- Target: `dev` environment
- Materializations:
  - Staging: `view`
  - Intermediate: `view`  
  - Marts: `table`

### Evidence Configuration
- Database: DuckDB connection to `../data/jaffle_shop.duckdb`
- Layout: Sidebar navigation

## 🛠️ Troubleshooting

### Common Issues

1. **UV not found**: Install UV using the instructions at https://docs.astral.sh/uv/
2. **Node.js missing**: Install Node.js from https://nodejs.org/
3. **Database locked**: Stop any running processes and retry
4. **API connection issues**: Check internet connectivity

### Clean Restart

```bash
# Remove database and start fresh
rm -f data/jaffle_shop.duckdb
./run.sh
```

## 🎨 Customization

- **Add new data sources**: Modify `dlt_pipeline/extract.py`
- **Create new models**: Add SQL files to `dbt_project/models/`
- **Add dashboard pages**: Create new `.md` files in `evidence_dashboard/pages/`
- **Modify visualizations**: Edit existing pages or create new components

## 📚 Learn More

- [dlt Documentation](https://dlthub.com/docs)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Evidence Documentation](https://evidence.dev/docs)
- [UV Documentation](https://docs.astral.sh/uv/)