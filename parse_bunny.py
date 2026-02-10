from pydantic import BaseModel, model_validator
import polars as pl
from io import StringIO
from typing import Any

class BunnyFile(BaseModel):
    file_name: str
    file_data: str
    file_description: str
    file_reference: str
    file_sensitive: bool
    file_size: float
    file_type: str

    def parse_table(self) -> pl.DataFrame:
        return pl.read_csv(
            StringIO(self.file_data),
            separator="\t"
        )


class BunnyQueryResult(BaseModel):
    count: str
    datasetCount: str
    files: dict[str, BunnyFile]
    
    @model_validator(mode="before")
    @classmethod
    def hoist_filenames(cls, data=Any) -> Any:
        if not isinstance(data, dict):
            return data
        else:
            if "files" in data:
                files = {(file["file_name"], file) for file in data["files"]}
                return {
                        "count": data["count"],
                        "datasetCount": data["datasetCount"],
                        "files": files,
                        }
            else:
                return data



class BunnyTSVOutput(BaseModel):
    uuid: str
    status: str
    collection_id: str
    message: str
    protocolVersion: str
    queryResult: BunnyQueryResult

def parse_bunny(path) -> pl.DataFrame:
    with open(path, "r") as f:
        bunny_output = BunnyTSVOutput.model_validate(f)
    return bunny_output.queryResult.files["code.distribution"].parse_table()
