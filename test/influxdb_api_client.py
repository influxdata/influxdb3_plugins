"""
InfluxDB 3 API client for plugin testing

Provides centralized API request handling for database operations,
package installation, and trigger management.
"""

import requests
import time
from typing import Dict, List, Optional, Tuple


def print_status(message: str) -> None:
    """Print status message in green"""
    print(f"\033[0;32m[INFO]\033[0m {message}")

def print_warning(message: str) -> None:
    """Print warning message in yellow"""
    print(f"\033[1;33m[WARN]\033[0m {message}")

def print_error(message: str) -> None:
    """Print error message in red"""
    print(f"\033[0;31m[ERROR]\033[0m {message}")


class InfluxDBApiClient:
    """Centralized API client for InfluxDB 3 operations"""
    
    def __init__(self, host_url: str, timeout: int = 30):
        self.host_url = host_url
        self.timeout = timeout
    
    def make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                    params: Optional[Dict] = None) -> Tuple[bool, Dict]:
        """Make an API request to InfluxDB 3"""
        url = f"{self.host_url}/api/v3{endpoint}"
        
        try:
            if method.upper() == "POST":
                response = requests.post(url, json=data, timeout=self.timeout)
            elif method.upper() == "DELETE":
                response = requests.delete(url, params=params, timeout=self.timeout)
            elif method.upper() == "GET":
                response = requests.get(url, params=params, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            try:
                response_data = response.json()
            except:
                response_data = {"message": response.text}
            
            return response.status_code < 400, response_data
            
        except requests.exceptions.RequestException as e:
            return False, {"error": str(e)}
    
    def check_health(self) -> bool:
        """Check if InfluxDB is healthy"""
        try:
            response = requests.get(f"{self.host_url}/health", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def create_database(self, db_name: str) -> bool:
        """Create a database"""
        print_status(f"Creating database: {db_name}")
        
        success, response = self.make_request(
            "POST", 
            "/configure/database",
            {"db": db_name}
        )
        
        if success:
            print_status(f"Database {db_name} created successfully")
            return True
        else:
            print_warning(f"Database creation might have failed: {response}")
            return True  # Continue testing even if database creation fails
    
    def delete_database(self, db_name: str) -> bool:
        """Delete a database"""
        print_status(f"Deleting database: {db_name}")
        
        success, response = self.make_request(
            "DELETE",
            "/configure/database",
            params={"db": db_name}
        )
        
        if success:
            print_status(f"Database {db_name} deleted successfully")
            return True
        else:
            error_msg = response.get('message', '')
            if 'not found' not in error_msg.lower() and 'does not exist' not in error_msg.lower():
                print_warning(f"Database deletion might have failed: {response}")
            else:
                print_status(f"Database {db_name} not found (already deleted or never existed)")
            return False
    
    def install_packages(self, packages: List[str]) -> bool:
        """Install Python packages"""
        if not packages:
            print_status("No packages to install")
            return True
        
        print_status(f"Installing packages: {', '.join(packages)}")
        
        success, response = self.make_request(
            "POST",
            "/configure/plugin_environment/install_packages",
            {"packages": packages}
        )
        
        if success:
            print_status("Packages installed successfully")
            return True
        else:
            print_warning(f"Package installation might have failed: {response}")
            return True  # Continue testing even if package installation fails
    
    def write_test_data(self, database: str, test_data: str) -> bool:
        """Write test data using line protocol"""
        print_status("Writing test data...")
        
        try:
            response = requests.post(
                f"{self.host_url}/api/v3/write_lp",
                params={"db": database},
                data=test_data,
                headers={"Content-Type": "text/plain"},
                timeout=self.timeout
            )
            
            if response.status_code < 400:
                print_status("Test data written successfully")
                return True
            else:
                print_warning(f"Test data write might have failed: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print_error(f"Failed to write test data: {e}")
            return False
    
    def query_internal_db(self, query: str) -> List[Dict]:
        """Query the _internal database using SQL"""
        try:
            response = requests.post(
                f"{self.host_url}/api/v3/query_sql",
                json={
                    "db": "_internal",
                    "q": query,
                    "format": "json"
                },
                timeout=self.timeout
            )
            
            if response.status_code < 400:
                return response.json()
            else:
                print_warning(f"Internal query failed: {response.text}")
                return []
                
        except requests.exceptions.RequestException as e:
            print_warning(f"Failed to query _internal database: {e}")
            return []