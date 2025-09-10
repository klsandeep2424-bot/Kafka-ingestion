"""
Command Line Interface for Group Load Kafka Tool
"""
import logging
import sys
from typing import Optional
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from config import config
from kafka_producer import GroupLoadStreamer
from sample_data import GroupDataGenerator, SAMPLE_GROUPS
from models import GroupDetails


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

console = Console()


@click.group()
@click.option('--environment', '-e', 
              type=click.Choice(['dev', 'qa']), 
              default='dev',
              help='Target environment (dev or qa)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, environment, verbose):
    """Group Load Kafka Tool - Stream group data to Kafka topics"""
    ctx.ensure_object(dict)
    ctx.obj['environment'] = environment
    ctx.obj['verbose'] = verbose
    
    # Update config with environment
    config.environment = environment
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    console.print(f"[bold blue]Group Load Kafka Tool[/bold blue]")
    console.print(f"Environment: [green]{environment}[/green]")
    console.print(f"Target Topic: [yellow]{config.current_topic}[/yellow]")


@cli.command()
@click.option('--count', '-c', default=1, help='Number of groups to generate and send')
@click.option('--corporate', is_flag=True, help='Generate corporate group data')
@click.option('--company-name', help='Company name for corporate group')
@click.option('--employees', default=10, help='Number of employees for corporate group')
@click.pass_context
def send_sample(ctx, count, corporate, company_name, employees):
    """Send sample group data to Kafka"""
    console.print(f"[bold]Sending {count} sample group(s) to Kafka...[/bold]")
    
    streamer = GroupLoadStreamer()
    generator = GroupDataGenerator()
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            if corporate:
                if not company_name:
                    company_name = "Sample Corporation"
                
                task = progress.add_task("Generating corporate group...", total=None)
                group = generator.generate_corporate_group(company_name, employees)
                progress.update(task, description="Sending corporate group to Kafka...")
                
                success = streamer.stream_group_data(group.model_dump())
                if success:
                    console.print(f"[green]✓[/green] Corporate group sent successfully")
                    console.print(f"Group ID: [blue]{group.group_id}[/blue]")
                    console.print(f"Members: [blue]{len(group.members)}[/blue]")
                else:
                    console.print(f"[red]✗[/red] Failed to send corporate group")
                    sys.exit(1)
            else:
                task = progress.add_task("Generating sample groups...", total=count)
                groups = generator.generate_batch_groups(count)
                
                progress.update(task, description="Sending groups to Kafka...")
                results = streamer.stream_batch_data([group.model_dump() for group in groups])
                
                # Display results
                success_count = sum(1 for success in results.values() if success)
                console.print(f"[green]✓[/green] Successfully sent {success_count}/{count} groups")
                
                if success_count < count:
                    failed_groups = [group_id for group_id, success in results.items() if not success]
                    console.print(f"[red]✗[/red] Failed groups: {', '.join(failed_groups)}")
    
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)
    finally:
        streamer.close()


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.pass_context
def send_file(ctx, file_path):
    """Send group data from JSON file to Kafka"""
    import json
    
    console.print(f"[bold]Sending group data from {file_path}...[/bold]")
    
    streamer = GroupLoadStreamer()
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Handle single group or list of groups
        if isinstance(data, dict):
            groups = [data]
        else:
            groups = data
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Sending groups to Kafka...", total=len(groups))
            
            results = streamer.stream_batch_data(groups)
            
            # Display results
            success_count = sum(1 for success in results.values() if success)
            console.print(f"[green]✓[/green] Successfully sent {success_count}/{len(groups)} groups")
            
            if success_count < len(groups):
                failed_groups = [group_id for group_id, success in results.items() if not success]
                console.print(f"[red]✗[/red] Failed groups: {', '.join(failed_groups)}")
    
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)
    finally:
        streamer.close()


@cli.command()
@click.pass_context
def config_info(ctx):
    """Display current configuration"""
    table = Table(title="Kafka Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="magenta")
    
    table.add_row("Environment", config.environment)
    table.add_row("Bootstrap Servers", config.bootstrap_servers)
    table.add_row("API Key", config.api_key[:8] + "..." if config.api_key else "Not set")
    table.add_row("Current Topic", config.current_topic)
    table.add_row("Dev Topic", config.dev_topic)
    table.add_row("QA Topic", config.qa_topic)
    
    console.print(table)


@cli.command()
@click.option('--output', '-o', help='Output file path for generated data')
@click.option('--count', '-c', default=5, help='Number of groups to generate')
@click.option('--corporate', is_flag=True, help='Generate corporate group data')
@click.option('--company-name', help='Company name for corporate group')
@click.option('--employees', default=25, help='Number of employees for corporate group')
def generate_data(output, count, corporate, company_name, employees):
    """Generate sample group data (without sending to Kafka)"""
    generator = GroupDataGenerator()
    
    if corporate:
        if not company_name:
            company_name = "Sample Corporation"
        
        group = generator.generate_corporate_group(company_name, employees)
        data = [group.model_dump()]
    else:
        groups = generator.generate_batch_groups(count)
        data = [group.model_dump() for group in groups]
    
    if output:
        import json
        with open(output, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        console.print(f"[green]✓[/green] Generated data saved to {output}")
    else:
        import json
        console.print(json.dumps(data, indent=2, default=str))


if __name__ == '__main__':
    cli()
