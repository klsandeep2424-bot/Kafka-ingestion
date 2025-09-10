"""
Data models for Group Load Kafka messages
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import json


class GroupMember(BaseModel):
    """Individual group member information"""
    member_id: str = Field(..., description="Unique member identifier")
    first_name: str = Field(..., description="Member first name")
    last_name: str = Field(..., description="Member last name")
    email: str = Field(..., description="Member email address")
    phone: Optional[str] = Field(None, description="Member phone number")
    date_of_birth: Optional[str] = Field(None, description="Member date of birth")
    address: Optional[Dict[str, str]] = Field(None, description="Member address information")
    enrollment_date: Optional[str] = Field(None, description="Member enrollment date")
    status: str = Field(default="active", description="Member status")


class GroupDetails(BaseModel):
    """Group details for Kafka message"""
    group_id: str = Field(..., description="Unique group identifier")
    group_name: str = Field(..., description="Group name")
    group_type: str = Field(..., description="Type of group (e.g., corporate, individual)")
    effective_date: str = Field(..., description="Group effective date")
    termination_date: Optional[str] = Field(None, description="Group termination date")
    status: str = Field(default="active", description="Group status")
    members: List[GroupMember] = Field(..., description="List of group members")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Record creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Record update timestamp")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    def to_kafka_message(self) -> str:
        """Convert to JSON string for Kafka message"""
        return self.model_dump_json()
    
    @classmethod
    def from_kafka_message(cls, message: str) -> "GroupDetails":
        """Create GroupDetails from Kafka message JSON"""
        data = json.loads(message)
        return cls(**data)


class GroupLoadMessage(BaseModel):
    """Wrapper for group load messages sent to Kafka"""
    message_type: str = Field(default="group_load", description="Type of message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Message timestamp")
    environment: str = Field(..., description="Target environment (dev/qa)")
    group_details: GroupDetails = Field(..., description="Group details payload")
    message_id: str = Field(..., description="Unique message identifier")
    
    def to_kafka_message(self) -> str:
        """Convert to JSON string for Kafka message"""
        return self.model_dump_json()
    
    @classmethod
    def from_kafka_message(cls, message: str) -> "GroupLoadMessage":
        """Create GroupLoadMessage from Kafka message JSON"""
        data = json.loads(message)
        return cls(**data)
