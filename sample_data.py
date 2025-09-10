"""
Sample data generators for Group Load Tool
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from faker import Faker

from models import GroupDetails, GroupMember


class GroupDataGenerator:
    """Generate sample group data for testing"""
    
    def __init__(self):
        """Initialize the data generator"""
        self.fake = Faker()
        self.group_types = ["corporate", "individual", "family", "small_business"]
        self.member_statuses = ["active", "inactive", "pending", "terminated"]
    
    def generate_member(self, group_id: str) -> GroupMember:
        """Generate a sample group member"""
        return GroupMember(
            member_id=f"{group_id}_M{random.randint(1000, 9999)}",
            first_name=self.fake.first_name(),
            last_name=self.fake.last_name(),
            email=self.fake.email(),
            phone=self.fake.phone_number(),
            date_of_birth=self.fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
            address={
                "street": self.fake.street_address(),
                "city": self.fake.city(),
                "state": self.fake.state_abbr(),
                "zip_code": self.fake.zipcode(),
                "country": "USA"
            },
            enrollment_date=self.fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d"),
            status=random.choice(self.member_statuses)
        )
    
    def generate_group(self, group_id: Optional[str] = None) -> GroupDetails:
        """Generate a sample group"""
        if not group_id:
            group_id = f"GRP_{random.randint(10000, 99999)}"
        
        # Generate random number of members (1-50)
        num_members = random.randint(1, 50)
        members = [self.generate_member(group_id) for _ in range(num_members)]
        
        # Generate group dates
        effective_date = self.fake.date_between(start_date="-1y", end_date="today")
        termination_date = None
        if random.random() < 0.1:  # 10% chance of terminated group
            termination_date = self.fake.date_between(start_date=effective_date, end_date="today")
        
        return GroupDetails(
            group_id=group_id,
            group_name=f"{self.fake.company()} Group Plan",
            group_type=random.choice(self.group_types),
            effective_date=effective_date.strftime("%Y-%m-%d"),
            termination_date=termination_date.strftime("%Y-%m-%d") if termination_date else None,
            status="active" if not termination_date else "terminated",
            members=members,
            metadata={
                "plan_type": random.choice(["HMO", "PPO", "EPO", "POS"]),
                "coverage_level": random.choice(["individual", "family", "employee_plus_spouse"]),
                "premium_amount": round(random.uniform(200, 2000), 2),
                "deductible": random.randint(500, 5000),
                "max_out_of_pocket": random.randint(1000, 10000)
            }
        )
    
    def generate_batch_groups(self, count: int) -> List[GroupDetails]:
        """Generate a batch of sample groups"""
        return [self.generate_group() for _ in range(count)]
    
    def generate_corporate_group(self, company_name: str, num_employees: int) -> GroupDetails:
        """Generate a specific corporate group"""
        group_id = f"CORP_{company_name.upper().replace(' ', '_')}_{random.randint(1000, 9999)}"
        
        members = []
        for i in range(num_employees):
            member = GroupMember(
                member_id=f"{group_id}_EMP{i+1:04d}",
                first_name=self.fake.first_name(),
                last_name=self.fake.last_name(),
                email=f"{self.fake.first_name().lower()}.{self.fake.last_name().lower()}@{company_name.lower().replace(' ', '')}.com",
                phone=self.fake.phone_number(),
                date_of_birth=self.fake.date_of_birth(minimum_age=22, maximum_age=65).strftime("%Y-%m-%d"),
                address={
                    "street": self.fake.street_address(),
                    "city": self.fake.city(),
                    "state": self.fake.state_abbr(),
                    "zip_code": self.fake.zipcode(),
                    "country": "USA"
                },
                enrollment_date=self.fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d"),
                status="active"
            )
            members.append(member)
        
        return GroupDetails(
            group_id=group_id,
            group_name=f"{company_name} Employee Health Plan",
            group_type="corporate",
            effective_date=self.fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d"),
            termination_date=None,
            status="active",
            members=members,
            metadata={
                "plan_type": "PPO",
                "coverage_level": "employee_plus_family",
                "premium_amount": round(random.uniform(800, 1500), 2),
                "deductible": random.randint(1000, 3000),
                "max_out_of_pocket": random.randint(2000, 8000),
                "company_size": num_employees,
                "industry": random.choice(["Technology", "Healthcare", "Finance", "Manufacturing", "Retail"])
            }
        )


# Sample data for immediate use
SAMPLE_GROUPS = [
    {
        "group_id": "GRP_12345",
        "group_name": "Acme Corporation Health Plan",
        "group_type": "corporate",
        "effective_date": "2024-01-01",
        "termination_date": None,
        "status": "active",
        "members": [
            {
                "member_id": "GRP_12345_M1001",
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@acme.com",
                "phone": "+1-555-0123",
                "date_of_birth": "1985-03-15",
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zip_code": "10001",
                    "country": "USA"
                },
                "enrollment_date": "2024-01-01",
                "status": "active"
            },
            {
                "member_id": "GRP_12345_M1002",
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane.smith@acme.com",
                "phone": "+1-555-0124",
                "date_of_birth": "1990-07-22",
                "address": {
                    "street": "456 Oak Ave",
                    "city": "New York",
                    "state": "NY",
                    "zip_code": "10002",
                    "country": "USA"
                },
                "enrollment_date": "2024-01-01",
                "status": "active"
            }
        ],
        "metadata": {
            "plan_type": "PPO",
            "coverage_level": "employee_plus_family",
            "premium_amount": 1200.00,
            "deductible": 2000,
            "max_out_of_pocket": 5000
        }
    }
]
