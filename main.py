"""
Main entry point for Group Load Kafka Tool
"""
import logging
import sys
from kafka_producer import GroupLoadStreamer
from sample_data import SAMPLE_GROUPS, GroupDataGenerator
from models import GroupDetails

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """Main function to demonstrate the Group Load Kafka Tool"""
    print("Group Load Kafka Tool - Sample Usage")
    print("=" * 50)
    
    # Initialize the streamer
    streamer = GroupLoadStreamer()
    
    try:
        # Example 1: Send predefined sample data
        print("\n1. Sending predefined sample group...")
        sample_group = GroupDetails(**SAMPLE_GROUPS[0])
        success = streamer.stream_group_data(sample_group.model_dump())
        
        if success:
            print(f"✓ Successfully sent group: {sample_group.group_id}")
        else:
            print("✗ Failed to send sample group")
            return 1
        
        # Example 2: Generate and send random group data
        print("\n2. Generating and sending random group data...")
        generator = GroupDataGenerator()
        random_group = generator.generate_group()
        
        success = streamer.stream_group_data(random_group.model_dump())
        if success:
            print(f"✓ Successfully sent random group: {random_group.group_id}")
            print(f"  - Group Name: {random_group.group_name}")
            print(f"  - Members: {len(random_group.members)}")
            print(f"  - Group Type: {random_group.group_type}")
        else:
            print("✗ Failed to send random group")
            return 1
        
        # Example 3: Generate and send corporate group
        print("\n3. Generating and sending corporate group...")
        corporate_group = generator.generate_corporate_group("TechCorp Inc", 15)
        
        success = streamer.stream_group_data(corporate_group.model_dump())
        if success:
            print(f"✓ Successfully sent corporate group: {corporate_group.group_id}")
            print(f"  - Company: {corporate_group.group_name}")
            print(f"  - Employees: {len(corporate_group.members)}")
            print(f"  - Industry: {corporate_group.metadata.get('industry', 'N/A')}")
        else:
            print("✗ Failed to send corporate group")
            return 1
        
        print("\n✓ All examples completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        print(f"✗ Error: {str(e)}")
        return 1
    
    finally:
        # Clean up
        streamer.close()


if __name__ == "__main__":
    sys.exit(main())
