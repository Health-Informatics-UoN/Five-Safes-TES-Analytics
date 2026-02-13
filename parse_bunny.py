from pydantic import BaseModel, model_validator
import polars as pl
from io import StringIO
from typing import Any
import json

class BunnyFile(BaseModel):
    """
    In decoded Bunny Outputs, there are "files" in the queryResult attribute.
    This model represents the fields in a "file"
    """
    file_name: str
    file_data: str
    file_description: str
    file_reference: str
    file_sensitive: bool
    file_size: float
    file_type: str

    def parse_table(self) -> pl.DataFrame:
        """
        Tries to parse the `file_data` field of a BunnyFile as a TSV table

        Returns
        -------
        pl.DataFrame
            The data held in the file_data string as a data frame
        """
        return pl.read_csv(
            StringIO(self.file_data),
            separator="\t"
        )


class BunnyQueryResult(BaseModel):
    """
    One of the attributes of bunny outputs is a `queryResult`.
    A modification from the original is that the `files` attribute is an array in the JSON, but here I have pulled the `file_name` attribute from each file to create a dictionary so you can ergonomically get files by their name.
    """
    count: int
    datasetCount: int
    files: dict[str, BunnyFile]
    
    @model_validator(mode="before")
    @classmethod
    def hoist_filenames(cls, data=Any) -> Any:
        """
        Takes the dictionary and pulls the file name out as a key for the files
        """
        if not isinstance(data, dict):
            return data
        else:
            if "files" in data:
                files = {file["file_name"]: file for file in data["files"]}
                return {
                        "count": data["count"],
                        "datasetCount": data["datasetCount"],
                        "files": files,
                        }
            else:
                return data



class BunnyTSVOutput(BaseModel):
    """
    The overall format for a Bunny output
    """
    uuid: str
    status: str
    collection_id: str
    message: str
    protocolVersion: str
    queryResult: BunnyQueryResult

def parse_bunny(path) -> pl.DataFrame:
    """
    Given the path of a decoded JSON of Bunny output, parses the JSON as a BunnyTSVOutput, then pulls out the code.distribution table

    Parameters
    ----------
    path
        The path of the JSON file

    Returns
    -------
    pl.DataFrame
        The data held as a TSV string in the queryResult file with the file_name "code.distribution"
    """
    with open(path, "r") as f:
        bunny_json = json.load(f)
        bunny_output = BunnyTSVOutput.model_validate(bunny_json)
    return bunny_output.queryResult.files["code.distribution"].parse_table()
