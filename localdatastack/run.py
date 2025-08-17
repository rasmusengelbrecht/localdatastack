#!/usr/bin/env python3
"""
Main orchestration script for the local data stack.
Runs the complete pipeline: dlt extract -> dbt transform -> Evidence dashboard.
"""

import os
import sys
import subprocess
import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from pathlib import Path

console = Console()

def run_command(command, cwd=None, description="Running command"):
    """Run a shell command and handle errors."""
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(description, total=None)
            
            result = subprocess.run(
                command,
                shell=True,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=True
            )
            
            progress.update(task, completed=True)
            
        if result.stdout:
            console.print(f"✅ {description}")
            console.print(result.stdout)
        return result
        
    except subprocess.CalledProcessError as e:
        console.print(f"❌ Error in {description}")
        console.print(f"Command: {command}")
        console.print(f"Exit code: {e.returncode}")
        console.print(f"Error output: {e.stderr}")
        sys.exit(1)


def setup_environment():
    """Set up the project environment."""
    console.print(Panel("🔧 Setting up environment", style="blue"))
    
    # Install UV dependencies
    run_command("uv sync", description="Installing Python dependencies")
    
    # Install Evidence dependencies
    evidence_dir = Path("evidence_dashboard")
    if evidence_dir.exists():
        run_command("npm install", cwd=evidence_dir, description="Installing Evidence dependencies")


def extract_data():
    """Run the dlt extraction pipeline."""
    console.print(Panel("📥 Extracting data from Jaffle Shop API", style="green"))
    
    # Change to dlt_pipeline directory and run extraction
    run_command(
        "uv run python extract.py", 
        cwd="dlt_pipeline",
        description="Extracting data with dlt"
    )


def transform_data():
    """Run dbt transformations."""
    console.print(Panel("🔄 Transforming data with dbt", style="yellow"))
    
    dbt_dir = Path("dbt_project")
    
    # Run dbt deps (in case we add packages later)
    run_command(
        "uv run dbt deps", 
        cwd=dbt_dir,
        description="Installing dbt dependencies"
    )
    
    # Run dbt models
    run_command(
        "uv run dbt run", 
        cwd=dbt_dir,
        description="Running dbt transformations"
    )
    
    # Test dbt models
    run_command(
        "uv run dbt test", 
        cwd=dbt_dir,
        description="Testing dbt models"
    )


def start_dashboard():
    """Start the Evidence dashboard."""
    console.print(Panel("📊 Starting Evidence dashboard", style="purple"))
    
    evidence_dir = Path("evidence_dashboard")
    
    console.print("🚀 Starting Evidence dashboard at http://localhost:3000")
    console.print("Press Ctrl+C to stop the dashboard")
    
    try:
        # Start Evidence in development mode
        subprocess.run(
            "npm run dev",
            cwd=evidence_dir,
            shell=True,
            check=True
        )
    except KeyboardInterrupt:
        console.print("\n👋 Dashboard stopped")
    except subprocess.CalledProcessError as e:
        console.print(f"❌ Error starting dashboard: {e}")


@click.command()
@click.option('--extract', is_flag=True, help='Only run data extraction')
@click.option('--transform', is_flag=True, help='Only run data transformation') 
@click.option('--dashboard', is_flag=True, help='Only start dashboard')
@click.option('--setup', is_flag=True, help='Only setup environment')
@click.option('--skip-setup', is_flag=True, help='Skip environment setup')
def main(extract, transform, dashboard, setup, skip_setup):
    """
    🏗️ Local Data Stack Orchestrator
    
    Runs the complete data pipeline: Extract -> Transform -> Dashboard
    """
    
    console.print(Panel.fit(
        "🏗️ Local Data Stack\n"
        "dlt → DuckDB → dbt → Evidence.dev",
        style="bold blue"
    ))
    
    try:
        # Handle individual steps
        if setup:
            setup_environment()
            return
            
        if extract:
            extract_data()
            return
            
        if transform:
            transform_data() 
            return
            
        if dashboard:
            start_dashboard()
            return
        
        # Run full pipeline
        if not skip_setup:
            setup_environment()
            
        extract_data()
        transform_data()
        start_dashboard()
        
    except KeyboardInterrupt:
        console.print("\n👋 Pipeline interrupted by user")
    except Exception as e:
        console.print(f"❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()