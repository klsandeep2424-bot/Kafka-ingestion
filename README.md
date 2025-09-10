# Group Load Kafka Tool

A Python tool for streaming group load data to Kafka topics, designed for CVS Health's group load streaming requirements.

## Overview

This tool provides a comprehensive solution for:
- Producing group load messages to Kafka topics
- Supporting both Development and QA environments
- Generating sample group data for testing
- Streaming corporate and individual group data
- Managing Kafka configuration and authentication

## Features

- **Kafka Integration**: Built with Confluent Kafka Python client
- **Environment Support**: Dev and QA environment configurations
- **Data Models**: Pydantic-based models for type safety and validation
- **Sample Data Generation**: Faker-powered data generation for testing
- **CLI Interface**: Rich command-line interface with progress indicators
- **Batch Processing**: Support for single and batch message processing
- **Configuration Management**: Environment-based configuration with .env support

## Installation

1. **Clone or download the project**
   ```bash
   cd python_kafka
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables** (optional)
   ```bash
   cp .env.example .env
   # Edit .env with your specific configuration
   ```

## Configuration

The tool uses the following Kafka configuration from the email:

- **Bootstrap Server**: `lkc-g1r211.dom8wd1r3wx.us-east4.gcp.confluent.cloud:9092`
- **API Key**: `SY367CYSVPCVDNXJ`
- **API Secret**: `Zyqm1BIID+G8/426QP6FNIYw/bl6kgolKbpnlh6nyZ9c+JcC/Qnt9bGXQDsb/wvQ`
- **Dev Topic**: `gcp.pss.groupfl.mypbmcaa.dev.groupdetails`
- **QA Topic**: `gcp.pss.groupfl.mypbmcaa.qa.groupdetails`

## Usage

### Command Line Interface

The tool provides a rich CLI for various operations:

#### 1. Send Sample Data
```bash
# Send a single sample group
python cli.py send-sample

# Send multiple groups
python cli.py send-sample --count 5

# Send corporate group data
python cli.py send-sample --corporate --company-name "Acme Corp" --employees 25

# Send to QA environment
python cli.py --environment qa send-sample --count 3
```

#### 2. Send Data from File
```bash
# Send data from JSON file
python cli.py send-file data/groups.json
```

#### 3. Generate Sample Data
```bash
# Generate and save sample data to file
python cli.py generate-data --count 10 --output sample_groups.json

# Generate corporate group data
python cli.py generate-data --corporate --company-name "TechCorp" --employees 50 --output corporate_group.json
```

#### 4. View Configuration
```bash
# Display current configuration
python cli.py config-info
```

### Programmatic Usage

```python
from kafka_producer import GroupLoadStreamer
from sample_data import GroupDataGenerator
from models import GroupDetails

# Initialize streamer
streamer = GroupLoadStreamer()

# Generate sample data
generator = GroupDataGenerator()
group = generator.generate_group()

# Stream to Kafka
success = streamer.stream_group_data(group.model_dump())

# Clean up
streamer.close()
```

### Direct Execution

```bash
# Run the main example script
python main.py
```

## Data Models

### GroupDetails
Main data model containing:
- `group_id`: Unique group identifier
- `group_name`: Group name
- `group_type`: Type of group (corporate, individual, family, small_business)
- `effective_date`: Group effective date
- `termination_date`: Group termination date (optional)
- `status`: Group status (active, terminated)
- `members`: List of group members
- `metadata`: Additional group metadata

### GroupMember
Individual member information:
- `member_id`: Unique member identifier
- `first_name`, `last_name`: Member name
- `email`, `phone`: Contact information
- `date_of_birth`: Member DOB
- `address`: Address information
- `enrollment_date`: Member enrollment date
- `status`: Member status

## Project Structure

```
python_kafka/
├── cli.py              # Command-line interface
├── config.py           # Configuration management
├── kafka_producer.py   # Kafka producer implementation
├── main.py             # Main entry point with examples
├── models.py           # Pydantic data models
├── sample_data.py      # Sample data generation
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Environment Variables

You can override default configuration using environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="your-bootstrap-server"
export KAFKA_API_KEY="your-api-key"
export KAFKA_API_SECRET="your-api-secret"
export ENVIRONMENT="qa"  # or "dev"
```

## Error Handling

The tool includes comprehensive error handling:
- Connection failures are logged and reported
- Message delivery confirmations via callbacks
- Graceful cleanup of resources
- Detailed error messages for troubleshooting

## Logging

The tool uses Python's logging module with configurable levels:
- INFO: General operation information
- DEBUG: Detailed debugging information
- ERROR: Error conditions

## Security Notes

- API keys and secrets are included in the code for demonstration purposes
- In production, use environment variables or secure configuration management
- Consider using Confluent Cloud's IAM for enhanced security

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check bootstrap server and credentials
2. **Topic Not Found**: Verify topic names and permissions
3. **Authentication Failed**: Verify API key and secret
4. **Message Delivery Failed**: Check network connectivity and topic configuration

### Debug Mode

Enable verbose logging for troubleshooting:
```bash
python cli.py --verbose send-sample
```

## Contributing

This tool is designed for CVS Health's specific requirements. For modifications:
1. Update configuration in `config.py`
2. Modify data models in `models.py` as needed
3. Add new sample data generators in `sample_data.py`
4. Extend CLI commands in `cli.py`

## License

This tool is created for CVS Health internal use based on the provided requirements.
