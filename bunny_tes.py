import tes_client
import tes
import os
from tes import Input, Output, Executor
from typing import List, Dict, Union

class BunnyTES(tes_client.TESClient):

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
            self.default_db_config['schema'] = os.getenv('DB_SCHEMA')  # None if not set - will fail naturally if needed

   #### this section will be implemented for each type of task using the pytes classes. Note that many of these fields are set in the submission layer after submission.
    def set_inputs(self) -> tes.Input:
        """
        Set the inputs for a TES task.
        """
        ## don't use tes.Input() because it will set type = 'FILE' rather than empty and not accepted by the TES server
        self.inputs = []
        return self.inputs

### is name required? Or even overwritten?
    def set_outputs(self, name: str, output_path: str, output_type: str = "DIRECTORY", url: str = "", description: str = "") -> tes.Output:
        """
        Set the outputs for a TES task.
        """
        self.outputs = [tes.Output(path=output_path, type=output_type, url=url, name=name, description=description)]
        return self.outputs

    def set_env(self) -> Dict[str, str]:
        """
        Set the environment variables for a TES task.
        """
        self.env = {
            "DATASOURCE_DB_DATABASE": self.default_db_config['name'],
            "DATASOURCE_DB_HOST": self.default_db_config['host'],
            "DATASOURCE_DB_PASSWORD": self.default_db_config['password'],
            "DATASOURCE_DB_USERNAME": self.default_db_config['username'],
            "DATASOURCE_DB_PORT": self.default_db_config['port'],
            "DATASOURCE_DB_SCHEMA": self.default_db_config.get('schema'),
            "TASK_API_BASE_URL": self.task_api_base_url,
            "TASK_API_USERNAME": self.task_api_username,
            "TASK_API_PASSWORD": self.task_api_password,
            "COLLECTION_ID": self.collection_id,
            "BUNNY_LOGGER_LEVEL": self.bunny_logger_level
        }
        return self.env

    def set_command(self, output_path: str) -> List[str]:
        """
        Set the command for a TES task.
        """

        self.command = [
            f"bunny",
            f"--body-json",
            f"{{\"code\":\"DEMOGRAPHICS\",\"analysis\":\"DISTRIBUTION\",\"uuid\":\"123\",\"collection\":\"test\",\"owner\":\"me\"}}",
            f"--output",
            f"{output_path}/output.json",
            
        ]
        return self.command

    def set_executors(self, workdir = "/app", output_path="/outputs") -> Union[tes.Executor, List[tes.Executor]]:
        """
        Set the executors for a TES task.
        """
        self.executors = [tes.Executor(image=self.default_image, 
        command=self.set_command(output_path), 
        env=self.set_env(), 
        workdir=workdir)]
        return self.executors

    def set_tes_messages(self, name: str = "test") -> None:
        """
        Set the TES message for a TES task.
        """
        self.set_inputs()
        self.set_outputs(name=name, output_path="/outputs", output_type="DIRECTORY", url = "", description = "")
        self.set_executors(workdir="/app", output_path="/outputs")
        self.create_tes_message(name=name)
        self.create_FiveSAFES_TES_message()
        return None

if __name__ == "__main__":
    bunny_tes = BunnyTES()
    #bunny_tes.set_inputs()
    #bunny_tes.set_outputs(name="test", output_path="/outputs", output_type="DIRECTORY")

    #bunny_tes.set_executors(query=query, analysis_type="mean", workdir="/app", output_path="/outputs", output_format="json")
    
    bunny_tes.set_tes_messages(name="test")
    pass