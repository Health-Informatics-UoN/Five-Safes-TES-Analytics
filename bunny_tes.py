import tes_client
import tes
import os
from tes import Input, Output, Executor
from typing import List, Dict, Union

class BunnyTES(tes_client.BaseTESClient):

    def __init__(self, *args, **kwargs):
        """
        Initialize BunnyTES client. Calls parent __init__ first, then reads bunny-specific env vars.
        """
        super().__init__(*args, **kwargs)
        
        # Read bunny-specific environment variables
        self.collection_id = os.getenv('COLLECTION_ID')
        self.bunny_logger_level = os.getenv('BUNNY_LOGGER_LEVEL', 'INFO')  # Default to INFO if not set
        self.task_api_base_url = os.getenv('TASK_API_BASE_URL')
        self.task_api_username = os.getenv('TASK_API_USERNAME')
        self.task_api_password = os.getenv('TASK_API_PASSWORD')
        
        # Add schema to default_db_config if not already present
        if 'schema' not in self.default_db_config:
            self.default_db_config['schema'] = os.getenv('SQL_SCHEMA')  # None if not set - will fail naturally if needed

   #### this section will be implemented for each type of task using the pytes classes. Note that many of these fields are set in the submission layer after submission.
    def set_inputs(self) -> None:
        """
        Set the inputs for a TES task.
        """
        ## don't use tes.Input() because it will set type = 'FILE' rather than empty and not accepted by the TES server
        self.inputs = []
        return None

### is name required? Or even overwritten?
    def set_outputs(self, name: str, output_path: str, output_type: str = "DIRECTORY", url: str = "", description: str = "") -> None:
        """
        Set the outputs for a TES task.
        """
        self.outputs = [tes.Output(path=output_path, type=output_type, url=url, name=name, description=description)]
        return None

    def _set_env(self) -> None:
        """
        Set the environment variables for a TES task.
        """
        self.env = {
            "DATASOURCE_DB_DATABASE": self.default_db_config['name'],
            "DATASOURCE_DB_HOST": self.default_db_config['host'],
            "DATASOURCE_DB_PASSWORD": self.default_db_config['password'],
            "DATASOURCE_DB_USERNAME": self.default_db_config['username'],
            "DATASOURCE_DB_PORT": self.default_db_config['port'],
            "DATASOURCE_DB_SCHEMA": self.default_db_config['schema'],
            "TASK_API_BASE_URL": self.task_api_base_url,
            "TASK_API_USERNAME": self.task_api_username,
            "TASK_API_PASSWORD": self.task_api_password,
            "COLLECTION_ID": self.collection_id,
            "BUNNY_LOGGER_LEVEL": self.bunny_logger_level
        }
        return None

    def _set_command(self, output_path: str, analysis: str = "DISTRIBUTION") -> None:
        """
        Set the command for a TES task.
        
        Args:
            output_path (str): Path for output files
            analysis (str): Analysis parameter for bunny (e.g., 'distribution', 'demographics')
        """

        code_analysis_pairs = {
        "distribution": ("GENERIC", "DISTRIBUTION"),
        "demographics": ("DEMOGRAPHICS", "DEMOGRAPHICS")
        }

        code, analysis = code_analysis_pairs[analysis.lower()]

        self.command = [
            f"--body-json",
            f"{{\"code\":\"{code}\",\"analysis\":\"{analysis}\",\"uuid\":\"123\",\"collection\":\"test\",\"owner\":\"me\"}}",
            f"--output",
            f"{output_path}/output.json",
            f"--no-encode"
        ]
        return None

    def set_executors(self, workdir = "/app", output_path="/outputs", analysis: str = "DISTRIBUTION") -> None:
        """
        Set the executors for a TES task.
        """
        self._set_command(output_path, analysis)
        self._set_env()
        self.executors = [tes.Executor(
            image=self.default_image,
            command=self.command,
            env=self.env,
            workdir=workdir,
        )]
        return None

    def set_tes_messages(
        self,
        analysis: str = "DISTRIBUTION",
        task_name: str = "test",
        task_description: str = "",
    ) -> None:
        """
        Set the TES message for a TES task.
        """
        self.set_inputs()
        self.set_outputs(name="", output_path="/outputs", output_type="DIRECTORY", url = "", description = "")
        self.set_executors(workdir="/app", output_path="/outputs", analysis=analysis)
        self.create_tes_message(task_name=task_name, task_description=task_description)
        self.create_FiveSAFES_TES_message()
        return None

