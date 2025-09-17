import json
from pydantic import AnyUrl
import requests
from typing import Dict, Any
from pathlib import Path
from urllib.parse import urlparse
from enum import Enum
from config import DbConfig


class TaskStatus(int, Enum):
    """
    Enum for task status codes with their corresponding display names.
    source: https://github.com/SwanseaUniversityMedical/DARE-Control/blob/main/src/BL/Models/Enums/Enums.cs
    """
    def __new__(cls, value, phrase):
        obj = int.__new__(cls, value)
        obj._value_ = value

        obj.phrase = phrase
        return obj

    # Parent only
    WAITING_FOR_CHILD_SUBS_TO_COMPLETE = 0, "Waiting for Child Submissions To Complete"
    # Stage 1
    WAITING_FOR_AGENT_TO_TRANSFER = 1, "Waiting for Agent To Transfer"
    # Stage 2
    TRANSFERRED_TO_POD = 2, "Transferred To Pod"
    # Stage 3
    POD_PROCESSING = 3, "Pod Processing"
    # Stage 3 - Green
    POD_PROCESSING_COMPLETE = 4, "Pod Processing Complete"
    # Stage 4
    DATA_OUT_APPROVAL_BEGUN = 5, "Data Out Approval Begun"
    # Stage 4 - Red
    DATA_OUT_APPROVAL_REJECTED = 6, "Data Out Rejected"
    # Stage 4 - Green
    DATA_OUT_APPROVED = 7, "Data Out Approved"
    # Stage 1 - Red
    USER_NOT_ON_PROJECT = 8, "User Not On Project"
    # Stage 2 - Red
    INVALID_USER = 9, "User not authorised for project on TRE"
    # Stage 2 - Red
    TRE_NOT_AUTHORISED_FOR_PROJECT = 10, "TRE Not Authorised For Project"
    # Stage 5 - Green (completed enum)
    COMPLETED = 11, "Completed"
    # Stage 1 - Red
    INVALID_SUBMISSION = 12, "Invalid Submission"
    # Stage 1 - Red
    CANCELLING_CHILDREN = 13, "Cancelling Children"
    # Stage 1 - Red
    REQUEST_CANCELLATION = 14, "Request Cancellation"
    # Stage 1 - Red
    CANCELLATION_REQUEST_SENT = 15, "Cancellation Request Sent"
    # Stage 5 - Red
    CANCELLED = 16, "Cancelled"
    # Stage 1
    SUBMISSION_WAITING_FOR_CRATE_FORMAT_CHECK = 17, "Waiting for Crate Format Check"
    # Unused
    VALIDATING_USER = 18, "Validating User"
    # Unused
    VALIDATING_SUBMISSION = 19, "Validating Submission"
    # Unused - Green
    VALIDATION_SUCCESSFUL = 20, "Validation Successful"
    # Stage 2
    AGENT_TRANSFERRING_TO_POD = 21, "Agent Transferring To Pod"
    # Stage 2 - Red
    TRANSFER_TO_POD_FAILED = 22, "Transfer To Pod Failed"
    # Unused
    TRE_REJECTED_PROJECT = 23, "TRE Rejected Project"
    # Unused
    TRE_APPROVED_PROJECT = 24, "TRE Approved Project"
    # Stage 3 - Red
    POD_PROCESSING_FAILED = 25, "Pod Processing Failed"
    # Stage 1 - Parent only
    RUNNING = 26, "Running"
    # Stage 5 - Red
    FAILED = 27, "Failed"
    # Stage 2
    SENDING_SUBMISSION_TO_HUTCH = 28, "Sending submission to Hutch"
    # Stage 4
    REQUESTING_HUTCH_DOES_FINAL_PACKAGING = 29, "Requesting Hutch packages up final output"
    # Stage 3
    WAITING_FOR_CRATE = 30, "Waiting for a Crate"
    # Stage 3
    FETCHING_CRATE = 31, "Fetching Crate"
    # Stage 3
    QUEUED = 32, "Crate queued"
    # Stage 3
    VALIDATING_CRATE = 33, "Validating Crate"
    # Stage 3
    FETCHING_WORKFLOW = 34, "Fetching workflow"
    # Stage 3
    STAGING_WORKFLOW = 35, "Preparing workflow"
    # Stage 3
    EXECUTING_WORKFLOW = 36, "Executing workflow"
    # Stage 3
    PREPARING_OUTPUTS = 37, "Preparing outputs"
    # Stage 3
    DATA_OUT_REQUESTED = 38, "Requested Egress"
    # Stage 3
    TRANSFERRED_FOR_DATA_OUT = 39, "Waiting for Egress results"
    # Stage 3
    PACKAGING_APPROVED_RESULTS = 40, "Finalising approved results"
    # Stage 3 - Green
    COMPLETE = 41, "Completed"
    # Stage 3 - Red
    FAILURE = 42, "Failed"
    # Stage 1
    SUBMISSION_RECEIVED = 43, "Submission has been received"
    # Stage 1 - Green
    SUBMISSION_CRATE_VALIDATED = 44, "Crate Validated"
    # Stage 1 - Red
    SUBMISSION_CRATE_VALIDATION_FAILED = 45, "Crate Failed Validation"
    # Stage 2 - Green
    TRE_CRATE_VALIDATED = 46, "Crate Validated"
    # Stage 2 - Red
    TRE_CRATE_VALIDATION_FAILED = 47, "Crate Failed Validation"
    # Stage 2
    TRE_WAITING_FOR_CRATE_FORMAT_CHECK = 48, "Waiting for Crate Format Check"
    # Stage 5 - Green - Parent Only
    PARTIAL_RESULT = 49, "Complete but not all TREs returned a result"

    @classmethod
    def get_status_description(cls, status_code: int):
        for status in cls:
            if status._value_ == status_code:
                return status.phrase
        raise ValueError(f"Unknown Status ({status_code})")

    @classmethod
    def get_status_code(cls, description: str):
        description = description.lower()
        for status in cls:
            if status.phrase.lower() == description:
                return status._value_
        raise ValueError(f"No status matching {description}")

class TESClient:
    """
    Handles TES (Task Execution Service) operations including task generation and submission.
    """

    def __init__(
        self,
        base_url: str,
        default_image: AnyUrl,
        default_db_config: DbConfig,
        TES_url: str | None = None,
        submission_url: str | None = None,
    ) -> None:
        """
        Initialize the TES client.

        Args:
            base_url (str): Base URL for the TES API
            default_image (str): Default Docker image to use
            default_db_config (Dict[str, str]): Default database configuration
            default_db_port (str): Default database port
        """
        self.base_url = str(base_url)

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

        self.default_image = default_image

        self.default_db_config = default_db_config

    def _build_api_url(
        self, base_url: str, endpoint: str, query_params: Dict[str, str] | None = None
    ) -> str:
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

    def generate_tes_task(
        self,
        query: str,
        name: str = "analysis test",
        image: str | None = None,
        # if you're happy with a db_config model here, we could add a __repr__ for the "command". This would also mean nobody could pass a bad config
        db_config: Dict[str, str] | None = None,
        output_path: str = "/outputs",
    ) -> Dict[str, Any]:
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
            image = str(self.default_image)

        if db_config is None:
            db_config = self.default_db_config.model_dump()

        task = {
            "name": name,
            "inputs": [],
            "outputs": [
                {
                    # should this be hard-coded?
                    "url": "s3://beacon7283outputtre",
                    "path": output_path,
                    "type": "DIRECTORY",
                    "name": "workdir",
                }
            ],
            "executors": [
                {
                    "image": image,
                    "command": [
                        f"--Connection=Host={db_config['host']}:{db_config['port']};Username={db_config['username']};Password={db_config['password']};Database={db_config['name']}",
                        f"--Output={output_path}/output.csv",
                        f"--Query={query}",
                    ],
                    "env": {
                        "DATASOURCE_DB_DATABASE": db_config["name"],
                        "DATASOURCE_DB_HOST": db_config["host"],
                        "DATASOURCE_DB_PASSWORD": db_config["password"],
                        "DATASOURCE_DB_USERNAME": db_config["username"],
                    },
                    "workdir": "/app",
                }
            ],
        }
        return task

    def save_tes_task(self, task: Dict[str, Any], output_file: str):
        """
        Save the TES task configuration to a JSON file.

        Args:
            task (Dict[str, Any]): TES task configuration
            output_file (str): Path to save the JSON file
        """
        with open(output_file, "w") as f:
            json.dump(task, f, indent=4)

    def generate_submission_template(
        self,
        project: str,
        output_bucket: str,
        name: str = "Analysis Submission Test",
        description: str = "Federated analysis task",
        tres: list = ["Nottingham"],
        output_path: str = "/outputs",
        image: str | None = None,
        db_config: dict | None = None,
        query: str | None = None,
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
        if image is None:
            image = str(self.default_image)

        if db_config is None:
            db_config = self.default_db_config.model_dump()

        # If a TES task is provided, use its executor configuration
        if query is None:
            query = "SELECT COUNT(*) FROM measurement WHERE measurement_concept_id = 3037532"

        executors = [
            {
                "image": image,
                "command": [
                    f"--Connection=Host={db_config['host']}:{db_config['port']};Username={db_config['username']};Password={db_config['password']};Database={db_config['name']}",
                    f"--Output={output_path}/output.csv",
                    f"--Query={query}",
                ],
                "env": {
                    "DATASOURCE_DB_DATABASE": db_config["name"],
                    "DATASOURCE_DB_HOST": db_config["host"],
                    "DATASOURCE_DB_PASSWORD": db_config["password"],
                    "DATASOURCE_DB_USERNAME": db_config["username"],
                },
                "workdir": "/app",
            }
        ]

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
                    "type": "DIRECTORY",
                }
            ],
            "resources": None,
            "executors": executors,
            "volumes": None,
            "tags": {"Project": project, "tres": "|".join(tres)},
            "logs": None,
            "creation_time": None,
        }
        return template, len(tres)

    def save_submission_template(self, template: Dict[str, Any], output_file: str):
        """
        Save the submission template configuration to a JSON file.

        Args:
            template (Dict[str, Any]): Submission template configuration
            output_file (str): Path to save the JSON file
        """
        with open(output_file, "w") as f:
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
            "accept": "text/plain",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
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
            if hasattr(e.response, "text"):
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
            "accept": "text/plain"  # ,
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
    from config import Settings, DbConfig

    settings = Settings()
    # Test URL construction
    print("Testing URL construction...")

    # Create a test client with a sample base URL
    test_base_url = "https://api.example.com/"
    client = TESClient(
        base_url=str(test_base_url),
        default_image=settings.tes_docker_image,
        default_db_config=DbConfig(
            db_host=settings.db_host,
            db_port=settings.db_port,
            db_username=settings.db_username,
            db_password=settings.db_password,
            db_name=settings.db_name,
        )
    )

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
    print("\n" + "=" * 50)
    print("Example usage:")

    # Generate a simple task
    task = client.generate_tes_task(
        query="SELECT COUNT(*) FROM measurement WHERE measurement_concept_id = 3037532",
        name="Test Analysis",
    )

    # Save to file
    client.save_tes_task(task, "tes-task.json")

    # Generate submission template
    template, n_tres = client.generate_submission_template(
        name="Test Submission",
        tres=["Nottingham", "Nottingham 2"],
        project="TestProject",
        output_bucket=settings.minio_output_bucket,
    )

    # Save template
    client.save_submission_template(template, "submission_template.json")

    # Generate curl command
    curl_cmd = client.generate_curl_command(template)
    print("\nCurl command for submission:")
    print(curl_cmd)
