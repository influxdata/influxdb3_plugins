"""
Docker container management for InfluxDB 3 plugin testing

Handles container lifecycle, health checking, and environment setup.
"""

import os
import subprocess
import time
from typing import List

try:
    from .influxdb_api_client import InfluxDBApiClient, print_status, print_warning, print_error
except ImportError:
    # When running as a script, use absolute imports
    from influxdb_api_client import InfluxDBApiClient, print_status, print_warning, print_error


class ContainerManager:
    """Manages Docker containers for InfluxDB 3 testing"""
    
    def __init__(self, api_client: InfluxDBApiClient, service_type: str = "core", skip_container: bool = False):
        self.api_client = api_client
        self.service_type = service_type
        self.container_name = f"influxdb3-{service_type}"
        self.skip_container = skip_container or os.getenv('SKIP_CONTAINER_MANAGEMENT', '').lower() in ('true', '1')
        
        if self.skip_container:
            print_status("Container management disabled (running inside container)")
    
    def run_command(self, command: List[str], capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a command and return the result"""
        try:
            if capture_output:
                result = subprocess.run(command, capture_output=True, text=True, check=True)
            else:
                result = subprocess.run(command, check=True)
            return result
        except subprocess.CalledProcessError as e:
            if capture_output:
                print_error(f"Command failed: {' '.join(command)}")
                if e.stderr:
                    print_error(f"Error output: {e.stderr}")
            raise
    
    def start_influxdb(self) -> None:
        """Start InfluxDB container (skipped when running inside container)"""
        if self.skip_container:
            print_status("Skipping container startup (already running inside container)")
            # Just wait for InfluxDB to be ready
            self.wait_for_influxdb()
            return
            
        print_status(f"Starting InfluxDB 3 {self.service_type} container...")
        
        # Stop existing container
        try:
            self.run_command(["docker", "compose", "-f", "compose.yml", "down", self.container_name])
        except subprocess.CalledProcessError:
            pass  # Container might not be running
        
        # Pull latest image
        self.run_command(["docker", "compose", "-f", "compose.yml", "pull", self.container_name])
        
        # Start service
        self.run_command(["docker", "compose", "-f", "compose.yml", "up", "-d", self.container_name])
        
        # Wait for readiness
        self.wait_for_influxdb()
    
    def wait_for_influxdb(self) -> None:
        """Wait for InfluxDB to be ready"""
        print_status(f"Waiting for InfluxDB 3 {self.service_type} to be ready...")
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            if self.api_client.check_health():
                print_status(f"InfluxDB 3 {self.service_type} is ready!")
                return
            
            attempt += 1
            print_status(f"Attempt {attempt}/{max_attempts} - waiting 2 seconds...")
            time.sleep(2)
        
        print_error(f"InfluxDB 3 {self.service_type} failed to start within {max_attempts * 2} seconds")
        
        # Show container logs for debugging (only if not skipping container management)
        if not self.skip_container:
            try:
                result = self.run_command(["docker", "compose", "-f", "compose.yml", "logs", self.container_name], 
                                        capture_output=True)
                print_error("Container logs:")
                print(result.stdout)
            except subprocess.CalledProcessError:
                pass
        
        raise RuntimeError(f"InfluxDB 3 {self.service_type} failed to start")
    
    def test_plugin_dir_env(self) -> bool:
        """Test: Check PLUGIN_DIR environment variable via Docker"""
        if self.skip_container:
            print_status("Test: Skipping PLUGIN_DIR check (running inside container)")
            return True
            
        print_status("Test: Checking PLUGIN_DIR environment variable")
        
        try:
            result = self.run_command([
                "docker", "compose", "-f", "compose.yml", "exec", "-T", "influxdb3-core",
                "env"
            ], capture_output=True)
            
            if "PLUGIN_DIR" in result.stdout:
                print_status("PLUGIN_DIR environment variable is set")
                plugin_dir_lines = [line for line in result.stdout.split('\n') if 'PLUGIN_DIR' in line]
                for line in plugin_dir_lines:
                    print(line)
                return True
            else:
                print_warning("PLUGIN_DIR environment variable not found")
                return False
        except subprocess.CalledProcessError as e:
            print_error("Failed to check environment variables")
            if e.stderr:
                print(e.stderr)
            return False