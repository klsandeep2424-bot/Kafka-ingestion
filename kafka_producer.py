"""
Kafka Producer for Group Load Tool
"""
import logging
import uuid
from typing import Optional, Dict, Any
from confluent_kafka import Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from config import config
from models import GroupLoadMessage, GroupDetails


class GroupLoadKafkaProducer:
    """Kafka producer for group load messages"""
    
    def __init__(self):
        """Initialize the Kafka producer"""
        self.producer = Producer(config.kafka_config)
        self.topic = config.current_topic
        self.logger = logging.getLogger(__name__)
        
    def produce_group_message(self, group_details: GroupDetails, 
                            message_id: Optional[str] = None) -> bool:
        """
        Produce a group load message to Kafka
        
        Args:
            group_details: GroupDetails object to send
            message_id: Optional message ID, will generate if not provided
            
        Returns:
            bool: True if message was queued successfully, False otherwise
        """
        try:
            if not message_id:
                message_id = str(uuid.uuid4())
            
            # Create the message wrapper
            message = GroupLoadMessage(
                environment=config.environment,
                group_details=group_details,
                message_id=message_id
            )
            
            # Convert to JSON
            message_json = message.to_kafka_message()
            
            # Produce to Kafka
            self.producer.produce(
                topic=self.topic,
                value=message_json.encode('utf-8'),
                key=message_id.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Flush to ensure message is sent
            self.producer.flush(timeout=10)
            
            self.logger.info(f"Successfully produced message {message_id} to topic {self.topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to produce message: {str(e)}")
            return False
    
    def produce_batch_messages(self, group_details_list: list[GroupDetails]) -> Dict[str, bool]:
        """
        Produce multiple group load messages to Kafka
        
        Args:
            group_details_list: List of GroupDetails objects to send
            
        Returns:
            Dict mapping message_id to success status
        """
        results = {}
        
        for group_details in group_details_list:
            message_id = str(uuid.uuid4())
            success = self.produce_group_message(group_details, message_id)
            results[message_id] = success
            
        return results
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.logger.info("Kafka producer closed")


class GroupLoadStreamer:
    """High-level interface for streaming group load data"""
    
    def __init__(self):
        """Initialize the streamer"""
        self.producer = GroupLoadKafkaProducer()
        self.logger = logging.getLogger(__name__)
    
    def stream_group_data(self, group_data: Dict[str, Any]) -> bool:
        """
        Stream group data to Kafka
        
        Args:
            group_data: Dictionary containing group data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert dict to GroupDetails model
            group_details = GroupDetails(**group_data)
            
            # Produce to Kafka
            return self.producer.produce_group_message(group_details)
            
        except Exception as e:
            self.logger.error(f"Failed to stream group data: {str(e)}")
            return False
    
    def stream_batch_data(self, batch_data: list[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Stream batch group data to Kafka
        
        Args:
            batch_data: List of dictionaries containing group data
            
        Returns:
            Dict mapping group_id to success status
        """
        results = {}
        
        for group_data in batch_data:
            try:
                group_details = GroupDetails(**group_data)
                success = self.producer.produce_group_message(group_details)
                results[group_details.group_id] = success
            except Exception as e:
                self.logger.error(f"Failed to process group data: {str(e)}")
                results[group_data.get('group_id', 'unknown')] = False
                
        return results
    
    def close(self):
        """Close the streamer"""
        self.producer.close()
