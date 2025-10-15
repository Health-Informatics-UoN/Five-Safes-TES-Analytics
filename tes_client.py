import json
import os
import requests
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import urljoin, urlparse
from enum import IntEnum

# Load environment variables from .env file
load_dotenv()


class TaskStatus(IntEnum):
    """
    Enum for task status codes with their corresponding display names.
    source: https://github.com/SwanseaUniversityMedical/DARE-Control/blob/main/src/BL/Models/Enums/Enums.cs
    """
    # Parent only
    WAITING_FOR_CHILD_SUBS_TO_COMPLETE = 0
    # Stage 1
    WAITING_FOR_AGENT_TO_TRANSFER = 1
    # Stage 2
    TRANSFERRED_TO_POD = 2
    # Stage 3
    POD_PROCESSING = 3
    # Stage 3 - Green
    POD_PROCESSING_COMPLETE = 4
    # Stage 4
    DATA_OUT_APPROVAL_BEGUN = 5
    # Stage 4 - Red
    DATA_OUT_APPROVAL_REJECTED = 6
    # Stage 4 - Green
    DATA_OUT_APPROVED = 7
    # Stage 1 - Red
    USER_NOT_ON_PROJECT = 8
    # Stage 2 - Red
    INVALID_USER = 9
    # Stage 2 - Red
    TRE_NOT_AUTHORISED_FOR_PROJECT = 10
    # Stage 5 - Green (completed enum)
    COMPLETED = 11
    # Stage 1 - Red
    INVALID_SUBMISSION = 12
    # Stage 1 - Red
    CANCELLING_CHILDREN = 13
    # Stage 1 - Red
    REQUEST_CANCELLATION = 14
    # Stage 1 - Red
    CANCELLATION_REQUEST_SENT = 15
    # Stage 5 - Red
    CANCELLED = 16
    # Stage 1
    SUBMISSION_WAITING_FOR_CRATE_FORMAT_CHECK = 17
    # Unused
    VALIDATING_USER = 18
    # Unused
    VALIDATING_SUBMISSION = 19
    # Unused - Green
    VALIDATION_SUCCESSFUL = 20
    # Stage 2
    AGENT_TRANSFERRING_TO_POD = 21
    # Stage 2 - Red
    TRANSFER_TO_POD_FAILED = 22
    # Unused
    TRE_REJECTED_PROJECT = 23
    # Unused
    TRE_APPROVED_PROJECT = 24
    # Stage 3 - Red
    POD_PROCESSING_FAILED = 25
    # Stage 1 - Parent only
    RUNNING = 26
    # Stage 5 - Red
    FAILED = 27
    # Stage 2
    SENDING_SUBMISSION_TO_HUTCH = 28
    # Stage 4
    REQUESTING_HUTCH_DOES_FINAL_PACKAGING = 29
    # Stage 3
    WAITING_FOR_CRATE = 30
    # Stage 3
    FETCHING_CRATE = 31
    # Stage 3
    QUEUED = 32
    # Stage 3
    VALIDATING_CRATE = 33
    # Stage 3
    FETCHING_WORKFLOW = 34
    # Stage 3
    STAGING_WORKFLOW = 35
    # Stage 3
    EXECUTING_WORKFLOW = 36
    # Stage 3
    PREPARING_OUTPUTS = 37
    # Stage 3
    DATA_OUT_REQUESTED = 38
    # Stage 3
    TRANSFERRED_FOR_DATA_OUT = 39
    # Stage 3
    PACKAGING_APPROVED_RESULTS = 40
    # Stage 3 - Green
    COMPLETE = 41
    # Stage 3 - Red
    FAILURE = 42
    # Stage 1
    SUBMISSION_RECEIVED = 43
    # Stage 1 - Green
    SUBMISSION_CRATE_VALIDATED = 44
    # Stage 1 - Red
    SUBMISSION_CRATE_VALIDATION_FAILED = 45
    # Stage 2 - Green
    TRE_CRATE_VALIDATED = 46
    # Stage 2 - Red
    TRE_CRATE_VALIDATION_FAILED = 47
    # Stage 2
    TRE_WAITING_FOR_CRATE_FORMAT_CHECK = 48
    # Stage 5 - Green - Parent Only
    PARTIAL_RESULT = 49


# Status lookup dictionary for easy access to display names
TASK_STATUS_DESCRIPTIONS = {
    TaskStatus.WAITING_FOR_CHILD_SUBS_TO_COMPLETE: "Waiting for Child Submissions To Complete",
    TaskStatus.WAITING_FOR_AGENT_TO_TRANSFER: "Waiting for Agent To Transfer",
    TaskStatus.TRANSFERRED_TO_POD: "Transferred To Pod",
    TaskStatus.POD_PROCESSING: "Pod Processing",
    TaskStatus.POD_PROCESSING_COMPLETE: "Pod Processing Complete",
    TaskStatus.DATA_OUT_APPROVAL_BEGUN: "Data Out Approval Begun",
    TaskStatus.DATA_OUT_APPROVAL_REJECTED: "Data Out Rejected",
    TaskStatus.DATA_OUT_APPROVED: "Data Out Approved",
    TaskStatus.USER_NOT_ON_PROJECT: "User Not On Project",
    TaskStatus.INVALID_USER: "User not authorised for project on TRE",
    TaskStatus.TRE_NOT_AUTHORISED_FOR_PROJECT: "TRE Not Authorised For Project",
    TaskStatus.COMPLETED: "Completed",
    TaskStatus.INVALID_SUBMISSION: "Invalid Submission",
    TaskStatus.CANCELLING_CHILDREN: "Cancelling Children",
    TaskStatus.REQUEST_CANCELLATION: "Request Cancellation",
    TaskStatus.CANCELLATION_REQUEST_SENT: "Cancellation Request Sent",
    TaskStatus.CANCELLED: "Cancelled",
    TaskStatus.SUBMISSION_WAITING_FOR_CRATE_FORMAT_CHECK: "Waiting For Crate Format Check",
    TaskStatus.VALIDATING_USER: "Validating User",
    TaskStatus.VALIDATING_SUBMISSION: "Validating Submission",
    TaskStatus.VALIDATION_SUCCESSFUL: "Validation Successful",
    TaskStatus.AGENT_TRANSFERRING_TO_POD: "Agent Transferring To Pod",
    TaskStatus.TRANSFER_TO_POD_FAILED: "Transfer To Pod Failed",
    TaskStatus.TRE_REJECTED_PROJECT: "Tre Rejected Project",
    TaskStatus.TRE_APPROVED_PROJECT: "Tre Approved Project",
    TaskStatus.POD_PROCESSING_FAILED: "Pod Processing Failed",
    TaskStatus.RUNNING: "Running",
    TaskStatus.FAILED: "Failed",
    TaskStatus.SENDING_SUBMISSION_TO_HUTCH: "Sending submission to Hutch",
    TaskStatus.REQUESTING_HUTCH_DOES_FINAL_PACKAGING: "Requesting Hutch packages up final output",
    TaskStatus.WAITING_FOR_CRATE: "Waiting for a Crate",
    TaskStatus.FETCHING_CRATE: "Fetching Crate",
    TaskStatus.QUEUED: "Crate queued",
    TaskStatus.VALIDATING_CRATE: "Validating Crate",
    TaskStatus.FETCHING_WORKFLOW: "Fetching workflow",
    TaskStatus.STAGING_WORKFLOW: "Preparing workflow",
    TaskStatus.EXECUTING_WORKFLOW: "Executing workflow",
    TaskStatus.PREPARING_OUTPUTS: "Preparing outputs",
    TaskStatus.DATA_OUT_REQUESTED: "Requested Egress",
    TaskStatus.TRANSFERRED_FOR_DATA_OUT: "Waiting for Egress results",
    TaskStatus.PACKAGING_APPROVED_RESULTS: "Finalising approved results",
    TaskStatus.COMPLETE: "Completed",
    TaskStatus.FAILURE: "Failed",
    TaskStatus.SUBMISSION_RECEIVED: "Submission has been received",
    TaskStatus.SUBMISSION_CRATE_VALIDATED: "Crate Validated",
    TaskStatus.SUBMISSION_CRATE_VALIDATION_FAILED: "Crate Failed Validation",
    TaskStatus.TRE_CRATE_VALIDATED: "Crate Validated",
    TaskStatus.TRE_CRATE_VALIDATION_FAILED: "Crate Failed Validation",
    TaskStatus.TRE_WAITING_FOR_CRATE_FORMAT_CHECK: "Waiting For Crate Format Check",
    TaskStatus.PARTIAL_RESULT: "Complete but not all TREs returned a result",
}


def get_status_description(status_code: int) -> str:
    """
    Get the display description for a given status code.
    
    Args:
        status_code (int): The numeric status code
        
    Returns:
        str: The display description for the status code, or "Unknown Status" if not found
    """
    try:
        return TASK_STATUS_DESCRIPTIONS[TaskStatus(status_code)]
    except (ValueError, KeyError):
        return f"Unknown Status ({status_code})"


def get_status_code(description: str) -> int:
    """
    Get the status code for a given display description.
    
    Args:
        description (str): The display description
        
    Returns:
        int: The status code, or -1 if not found
    """
    for code, desc in TASK_STATUS_DESCRIPTIONS.items():
        if desc.lower() == description.lower():
            return code.value
    return -1


class TESClient:
    """
    Handles TES (Task Execution Service) operations including task generation and submission.
    """
    
    def __init__(self, 
                 base_url: str = None,
                 TES_url: str = None,
                 submission_url: str = None,
                 default_image: str = None,
                 default_db_config: Dict[str, str] = None,
                 default_db_port: str = None):
        """
        Initialize the TES client.
        
        Args:
            base_url (str): Base URL for the TES API
            default_image (str): Default Docker image to use
            default_db_config (Dict[str, str]): Default database configuration
            default_db_port (str): Default database port
        """
        # Use environment variables - required
        self.base_url = base_url or os.getenv('TES_BASE_URL')
        if not self.base_url:
            raise ValueError("TES_BASE_URL environment variable is required")
        
        # Use pathlib to properly construct URLs
        if TES_url is None:
            parsed = urlparse(self.base_url)
            path = Path(parsed.path) / "v1"
            self.TES_url = f"{parsed.scheme}://{parsed.netloc}{path}"
        else:
            self.TES_url = TES_url
            
        if submission_url is None:
            parsed = urlparse(self.base_url)
            path = Path(parsed.path) / "api" / "Submission"
            self.submission_url = f"{parsed.scheme}://{parsed.netloc}{path}"
        else:
            self.submission_url = submission_url
        
        self.default_image = default_image or os.getenv('TES_DOCKER_IMAGE')
        if not self.default_image:
            raise ValueError("TES_DOCKER_IMAGE environment variable is required")
    def _build_api_url(self, base_url: str, endpoint: str, query_params: Dict[str, str] = None) -> str:
        """
        Build a complete API URL with proper path joining and query parameters.
        
        Args:
            base_url (str): Base URL
            endpoint (str): API endpoint
            query_params (Dict[str, str], optional): Query parameters to add
            
        Returns:
            str: Complete API URL
        """
        # Use pathlib to join URL paths
        parsed = urlparse(base_url)
        path = Path(parsed.path) / endpoint
        url = f"{parsed.scheme}://{parsed.netloc}{path}"
        
        if query_params:
            from urllib.parse import urlencode
            query_string = urlencode(query_params)
            url = f"{url}?{query_string}"
        
        return url
    
    def generate_tes_task(self,
                         query: str,
                         name: str = "analysis test",
                         image: str = None,
                         db_config: Dict[str, str] = None,
                         output_path: str = "/outputs") -> Dict[str, Any]:
        """
        Generate a TES task JSON configuration.
        
        Args:
            query (str): SQL query to execute
            name (str): Name of the analysis task
            image (str): Docker image to use (uses default if None)
            db_config (Dict[str, str]): Database configuration (uses default if None)
            output_path (str): Path for output files
            
        Returns:
            Dict[str, Any]: TES task configuration
        """
        if image is None:
            image = self.default_image
        
        
        task = {
            "name": name,
            "inputs": [],
            "outputs": [
                {
                    "url": "s3://beacon7283outputtre",
                    "path": output_path,
                    "type": "DIRECTORY",
                    "name": "workdir"
                }
            ],
            "executors": [
                {
                    "image": image,
                    "command": [
                        f"--Output={output_path}/output.csv",
                        f"--Query={query}"
                    ],
                    "env": {    
                    },
                    "workdir": "/app"
                }
            ]
        }
        return task
    
    def save_tes_task(self, task: Dict[str, Any], output_file: str):
        """
        Save the TES task configuration to a JSON file.
        
        Args:
            task (Dict[str, Any]): TES task configuration
            output_file (str): Path to save the JSON file
        """
        with open(output_file, 'w') as f:
            json.dump(task, f, indent=4)
    
    def generate_submission_template(
        self,
        name: str = "Analysis Submission Test",
        description: str = "Federated analysis task",
        tres: list = ["Nottingham"],
        project: str = None,
        output_bucket: str = None,
        output_path: str = "/outputs",
        image: str = None,
        db_config: dict = None,
        query: str = None
    ) -> tuple[dict, int]:
        """
        Generate a submission template JSON configuration.
        
        Args:
            name (str): Name of the analysis submission
            description (str): Description of the analysis task
            tres (list): List of TREs to run the analysis on
            project (str): Project name (defaults to TRE_FX_PROJECT env var)
            output_bucket (str): S3 bucket name for outputs (defaults to MINIO_OUTPUT_BUCKET env var)
            output_path (str): Path for output files
            image (str): Docker image to use
            db_config (dict): Database configuration
            query (str): SQL query to execute
        
        Returns:
            tuple[dict, int]: Submission template configuration and number of TREs
        """
        # Use environment variables for project and output bucket if not provided
        project = project or os.getenv('TRE_FX_PROJECT')
        if not project:
            raise ValueError("TRE_FX_PROJECT environment variable is required when project parameter is not provided")
        
        output_bucket = output_bucket or os.getenv('MINIO_OUTPUT_BUCKET')
        if not output_bucket:
            raise ValueError("MINIO_OUTPUT_BUCKET environment variable is required when output_bucket parameter is not provided")
        
        if image is None:
            image = self.default_image
        
        
        # If a TES task is provided, use its executor configuration
        if query is None:
            query = f"SELECT COUNT(*) FROM measurement WHERE measurement_concept_id = 3037532"
        
        executors = [{
            "image": image,
            "command": [
                f"--Output={output_path}/output.csv",
                f"--Query={query}"
            ],
            "env": {      
            },
            "workdir": "/app"
        }]
        
        template = {
            "id": None,
            "name": name,
            "description": description,
            "inputs": None,
            "outputs": [
                {
                    "name": "workdir",
                    "description": "analysis test output",
                    "url": f"s3://{output_bucket}",
                    "path": output_path,
                    "type": "DIRECTORY"
                }
            ],
            "resources": None,
            "executors": executors,
            "volumes": None,
            "tags": {
                "Project": project,
                "tres": "|".join(tres)
            },
            "logs": None,
            "creation_time": None
        }
        return template, len(tres)
    
    def save_submission_template(self, template: Dict[str, Any], output_file: str):
        """
        Save the submission template configuration to a JSON file.
        
        Args:
            template (Dict[str, Any]): Submission template configuration
            output_file (str): Path to save the JSON file
        """
        with open(output_file, 'w') as f:
            json.dump(template, f, indent=4)
    
    def generate_curl_command(self, template: Dict[str, Any]) -> str:
        """
        Generate a curl command for submitting the template.
        
        Args:
            template (Dict[str, Any]): Submission template configuration
            
        Returns:
            str: Formatted curl command
        """
        # Convert template to JSON string with proper escaping
        template_json = json.dumps(template).replace('"', '\\"')
        
        tasks_url = self._build_api_url(self.TES_url, "tasks")
        curl_command = f"""curl -X 'POST' \\
  '{tasks_url}' \\
  -H 'accept: text/plain' \\
  -H 'Authorization: Bearer **TOKEN-HERE**' \\
  -H 'Content-Type: application/json' \\
  -d '{template_json}'"""
        
        return curl_command
    
    def submit_task(self, template: Dict[str, Any], token: str) -> Dict[str, Any]:
        """
        Submit a TES task using the requests library.
        
        Args:
            template (Dict[str, Any]): The TES task template
            token (str): Authentication token
            
        Returns:
            Dict[str, Any]: Response from the server
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        headers = {
            'accept': 'text/plain',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            tasks_url = self._build_api_url(self.TES_url, "tasks")
            response = requests.post(tasks_url, headers=headers, json=template)
            
            # Debug: Print response details for 400 errors
            if response.status_code == 400:
                print(f"400 Bad Request Response:")
                print(f"Status Code: {response.status_code}")
                print(f"Response Headers: {dict(response.headers)}")
                print(f"Response Content: {response.text}")
            
            response.raise_for_status()  # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error submitting task: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Response content: {e.response.text}")
            raise
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get the status of a submitted task.
        
        Args:
            task_id (str): Task ID
            
        Returns:
            Dict[str, Any]: Task status information
        """

        headers = {
            'accept': 'text/plain'#,
            #'Authorization': f'Bearer {token}'
        }

        try:    
            url = self._build_api_url(self.submission_url, f"GetASubmission/{task_id}")
            response = requests.get(url, headers=headers)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting task status: {str(e)}")
            raise




if __name__ == "__main__":
    # Test URL construction
    print("Testing URL construction...")
    
    # Create a test client with a sample base URL
    test_base_url = "https://api.example.com/"
    client = TESClient(base_url=test_base_url)
    
    # Test URL joining
    print(f"Base URL: {client.base_url}")
    print(f"TES URL: {client.TES_url}")
    print(f"Submission URL: {client.submission_url}")
    
    # Test API URL building
    tasks_url = client._build_api_url(client.TES_url, "tasks")
    print(f"Tasks URL: {tasks_url}")
    
    # Test with query parameters
    status_url = client._build_api_url(client.TES_url, "123", {"view": "FULL"})
    print(f"Status URL: {status_url}")
    
    # Test with trailing slashes using pathlib directly
    parsed = urlparse("https://api.example.com/")
    path = Path(parsed.path) / "v1" / "tasks"
    test_url = f"{parsed.scheme}://{parsed.netloc}{path}"
    print(f"Test URL with trailing slash: {test_url}")
    
    # Example usage
    print("\n" + "="*50)
    print("Example usage:")
    
    # Generate a simple task
    task = client.generate_tes_task(
        query="SELECT COUNT(*) FROM measurement WHERE measurement_concept_id = 3037532",
        name="Test Analysis"
    )
    
    # Save to file
    client.save_tes_task(task, "tes-task.json")
    
    # Generate submission template
    template, n_tres = client.generate_submission_template(
        name="Test Submission",
        tres=["Nottingham", "Nottingham 2"],
        project="TestProject"
    )
    
    # Save template
    client.save_submission_template(template, "submission_template.json")
    
    # Generate curl command
    curl_cmd = client.generate_curl_command(template)
    print("\nCurl command for submission:")
    print(curl_cmd) 