from analysis_engine import AnalysisEngine
from typing import List, Dict, Any
from bunny_tes import BunnyTES
from data_processor import DataProcessor
import os

class Metadata:
    def __init__(self, analysis_engine: AnalysisEngine):
        self.analysis_engine = analysis_engine
        
        ## don't know whether to use the same processor, or create a new one.
        self.data_processor = DataProcessor()
        
        ## There will be significant differences in the metadata processing, so will need something new. But I'm leaving this here as a placeholder.
        #self.statistical_analyzer = StatisticalAnalyzer()
        
        # Storage for aggregated values
        self.aggregated_data = {}

    def get_metadata(self, 
                    tres: List[str] = None,
                    task_name: str = None,
                    bucket: str = None) -> Dict[str, Any]:

        analysis_type = "metadata"

        task_name, bucket, tres = self.analysis_engine.setup_analysis(analysis_type, task_name, bucket, tres)

        ### create the TES message for the metadata
        metadata_tes = self.analysis_engine.tes_client
        metadata_tes.set_tes_messages(name=task_name)
        metadata_tes.set_tags(tres=self.analysis_engine.tres)
        metadata_tes.create_FiveSAFES_TES_message()

        try:
            task_id, data = self.analysis_engine._submit_and_collect_results(
                metadata_tes.task,
                bucket,
                output_format="json",
                submit_message=f"Submitting {analysis_type} analysis to {len(self.analysis_engine.tres)} TREs..."
            )

            # Process and analyze data (aggregation moved to this class)
            print("Processing and analyzing data...")

            raw_aggregated_data = self.data_processor.aggregate_data(data, "metadata")
            
            ## placeholder for now, this is where the postprocessing will go.
            #analysis_result = self.statistical_analyzer.analyze_data(raw_aggregated_data, analysis_type)
            metadata_result = self.postprocess_metadata(raw_aggregated_data)

            # Store the aggregated values in the centralized dict
            # Note: Metadata storage may differ from analysis results
            self.aggregated_data.update(raw_aggregated_data)
            

            return {
                'analysis_type': "metadata",
                'result': metadata_result,
                'task_id': task_id,
                'tres_used': tres,
                'data_sources': len(data)
            }

        except Exception as e:
            print(f"Metadata analysis failed: {str(e)}")
            raise


    def postprocess_metadata(self, raw_aggregated_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Postprocess the metadata (placeholder for now).
        
        Args:
            raw_aggregated_data (Dict[str, Any]): Raw aggregated data
            
        Returns:
            Dict[str, Any]: Postprocessed metadata
        """
        return raw_aggregated_data

if __name__ == "__main__":

    engine = AnalysisEngine(tes_client=BunnyTES())
    metadata = Metadata(engine)

    sql_schema = os.getenv("SQL_SCHEMA", "public")

    print("Running metadata analysis...")
    metadata_result = metadata.get_metadata(
        task_name="DEMO: metadata test"
    )

    print(f"Metadata analysis result: {metadata_result['result']}")
    print(f"Stored aggregated data: {metadata.aggregated_data}")