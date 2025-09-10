"""
Configuration management for Kafka Group Load Tool
"""
import os
from typing import Literal
from pydantic import BaseSettings, Field


class KafkaConfig(BaseSettings):
    """Kafka configuration settings"""
    
    # Kafka connection settings
    bootstrap_servers: str = Field(
        default="lkc-g1r211.dom8wd1r3wx.us-east4.gcp.confluent.cloud:9092",
        description="Kafka bootstrap servers"
    )
    api_key: str = Field(
        default="SY367CYSVPCVDNXJ",
        description="Kafka API key"
    )
    api_secret: str = Field(
        default="Zyqm1BIID+G8/426QP6FNIYw/bl6kgolKbpnlh6nyZ9c+JcC/Qnt9bGXQDsb/wvQ",
        description="Kafka API secret"
    )
    
    # Environment settings
    environment: Literal["dev", "qa"] = Field(
        default="dev",
        description="Target environment (dev or qa)"
    )
    
    # Topic configurations
    dev_topic: str = Field(
        default="gcp.pss.groupfl.mypbmcaa.dev.groupdetails",
        description="Development Kafka topic"
    )
    qa_topic: str = Field(
        default="gcp.pss.groupfl.mypbmcaa.qa.groupdetails", 
        description="QA Kafka topic"
    )
    
    @property
    def current_topic(self) -> str:
        """Get the current topic based on environment"""
        return self.dev_topic if self.environment == "dev" else self.qa_topic
    
    @property
    def kafka_config(self) -> dict:
        """Get Kafka producer configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self.api_key,
            'sasl.password': self.api_secret,
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 10,
            'buffer.memory': 33554432,
        }
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global config instance
config = KafkaConfig()
