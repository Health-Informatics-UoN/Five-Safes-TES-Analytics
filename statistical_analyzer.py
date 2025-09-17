import numpy as np
from scipy import stats
from typing import Dict, Any, Union, List
from abc import ABC, abstractmethod


class AnalysisBase(ABC):
    """
    Abstract base class for statistical analyses.
    All analysis classes must inherit from this and implement required methods.
    """

    @property
    @abstractmethod
    def return_format(self) -> dict:
        """Return format description for the analysis."""
        pass

    @abstractmethod
    def aggregate_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]]
    ) -> np.ndarray:
        """
        Aggregate input data for analysis.

        Args:
            input_data: Input data (numpy array or list of arrays)

        Returns:
            np.ndarray: Aggregated data ready for analysis
        """
        pass

    @abstractmethod
    def analyze(self, aggregated_data: np.ndarray) -> Union[float, Dict[str, Any]]:
        """
        Perform the statistical analysis.

        Args:
            aggregated_data: Aggregated data from aggregate_data method

        Returns:
            Union[float, Dict[str, Any]]: Analysis result
        """
        pass


class MeanAnalysis(AnalysisBase):
    """Analysis class for calculating mean values."""

    def __init__(self):
        self.aggregated_data = {}

    @property
    def return_format(self) -> dict:
        return {"n": None, "total": None}

    def aggregate_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]]
    ) -> np.ndarray:
        """Aggregate data for mean calculation."""
        if isinstance(input_data, list):
            return np.vstack(input_data)
        return input_data

    def analyze(self, aggregated_data: np.ndarray) -> float:
        """Calculate mean from aggregated values."""
        n, total = np.sum(aggregated_data, axis=0)
        # Store the aggregated values
        self.aggregated_data = {"n": n, "total": total}
        return total / n


class VarianceAnalysis(AnalysisBase):
    """Analysis class for calculating variance."""

    def __init__(self):
        self.aggregated_data = {}

    @property
    def return_format(self) -> dict:
        return {"n": None, "sum_x2": None, "total": None}

    def aggregate_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]]
    ) -> np.ndarray:
        """Aggregate data for variance calculation."""
        if isinstance(input_data, list):
            return np.vstack(input_data)
        return input_data

    def analyze(self, aggregated_data: np.ndarray) -> float:
        """Calculate variance from aggregated values."""
        n, sum_x2, total = np.sum(aggregated_data, axis=0)
        # Store the aggregated values
        self.aggregated_data = {"n": n, "sum_x2": sum_x2, "total": total}
        return (sum_x2 - (total * total) / n) / (n - 1)


class PMCCAnalysis(AnalysisBase):
    """Analysis class for calculating Pearson's correlation coefficient."""

    def __init__(self):
        self.aggregated_data = {}

    @property
    def return_format(self) -> dict:
        return {
            "n": None,
            "sum_x": None,
            "sum_y": None,
            "sum_xy": None,
            "sum_x2": None,
            "sum_y2": None,
        }

    def aggregate_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]]
    ) -> np.ndarray:
        """Aggregate data for PMCC calculation."""
        if isinstance(input_data, list):
            return np.vstack(input_data)
        return input_data

    def analyze(self, aggregated_data: np.ndarray) -> float:
        """Calculate Pearson's correlation coefficient from aggregated values."""
        n, sum_x, sum_y, sum_xy, sum_x2, sum_y2 = np.sum(aggregated_data, axis=0)

        # Store the aggregated values
        self.aggregated_data = {
            "n": n,
            "sum_x": sum_x,
            "sum_y": sum_y,
            "sum_xy": sum_xy,
            "sum_x2": sum_x2,
            "sum_y2": sum_y2,
        }

        # Calculate standard deviations
        std_x = np.sqrt(sum_x2 - (sum_x**2) / n)
        std_y = np.sqrt(sum_y2 - (sum_y**2) / n)

        # Calculate covariance
        cov = (sum_xy - (sum_x * sum_y) / n) / (n - 1)

        # Calculate correlation coefficient
        return cov / (std_x * std_y)


class ChiSquaredScipyAnalysis(AnalysisBase):
    """Analysis class for chi-squared test using scipy."""

    def __init__(self):
        self.aggregated_data = {}

    @property
    def return_format(self) -> dict:
        return {"contingency_table": None}

    def aggregate_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]]
    ) -> np.ndarray:
        """Aggregate contingency tables."""
        if isinstance(input_data, list):
            # For contingency tables, we need to combine them
            if len(input_data) == 1:
                return input_data[0]
            else:
                # Combine multiple contingency tables
                combined = np.zeros_like(input_data[0])
                for table in input_data:
                    combined += table
                return combined
        return input_data

    def analyze(self, aggregated_data: np.ndarray) -> float:
        """Calculate chi-squared statistic using scipy."""
        # Store the contingency table
        self.aggregated_data = {"contingency_table": aggregated_data}

        # Get both corrected and uncorrected results
        #chi2_corrected, p_corrected, dof, expected = stats.chi2_contingency(
        #    aggregated_data
        #)
        chi2_uncorrected = stats.chi2_contingency(
            aggregated_data, correction=False
        )

        return chi2_uncorrected.statistic


class ChiSquaredManualAnalysis(AnalysisBase):
    """Analysis class for manual chi-squared calculation."""

    def __init__(self):
        self.aggregated_data = {}

    @property
    def return_format(self) -> dict:
        return {"contingency_table": None}

    def aggregate_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]]
    ) -> np.ndarray:
        """Aggregate contingency tables."""
        if isinstance(input_data, list):
            # For contingency tables, we need to combine them
            if len(input_data) == 1:
                return input_data[0]
            else:
                # Combine multiple contingency tables
                combined = np.zeros_like(input_data[0])
                for table in input_data:
                    combined += table
                return combined
        return input_data

    def analyze(self, aggregated_data: np.ndarray) -> Dict[str, Any]:
        """Calculate chi-squared statistic manually."""
        # Store the contingency table
        self.aggregated_data = {"contingency_table": aggregated_data}

        # Manual calculation
        row_totals = np.sum(aggregated_data, axis=1)
        col_totals = np.sum(aggregated_data, axis=0)
        total = np.sum(row_totals)

        # Calculate expected frequencies
        expected = np.zeros_like(aggregated_data)
        for i in range(len(aggregated_data)):
            for j in range(len(aggregated_data[i])):
                expected[i][j] = row_totals[i] * col_totals[j] / total

        # Calculate chi-squared
        chi2 = np.sum((aggregated_data - expected) ** 2 / expected)
        dof = (len(row_totals) - 1) * (len(col_totals) - 1)
        p_value = 1 - stats.chi2.cdf(chi2, dof)

        return {
            "chi_squared": chi2,
            "p_value": p_value,
            "degrees_of_freedom": dof,
            "expected_frequencies": expected,
        }


class StatisticalAnalyzer:
    """
    Handles statistical calculations and analysis for federated data.
    Uses individual analysis classes that inherit from AnalysisBase.
    """

    def __init__(self):
        """Initialize the statistical analyzer with analysis classes."""
        self.analysis_classes = {
            "mean": MeanAnalysis(),
            "variance": VarianceAnalysis(),
            "PMCC": PMCCAnalysis(),
            "chi_squared_scipy": ChiSquaredScipyAnalysis(),
            "chi_squared_manual": ChiSquaredManualAnalysis(),
        }

    def get_analysis_config(self, analysis_type: str) -> Dict[str, Any]:
        """
        Get configuration for a specific analysis type.

        Args:
            analysis_type (str): Type of analysis

        Returns:
            Dict[str, Any]: Analysis configuration

        Raises:
            ValueError: If analysis type is not supported
        """
        if analysis_type not in self.analysis_classes:
            raise ValueError(f"Unsupported analysis type: {analysis_type}")

        analysis_class = self.analysis_classes[analysis_type]
        return {
            "return_format": analysis_class.return_format,
            "aggregation_function": analysis_class.aggregate_data,
            "analysis_function": analysis_class.analyze,
        }

    def get_supported_analysis_types(self) -> List[str]:
        """
        Get list of supported analysis types.

        Returns:
            List[str]: List of supported analysis types
        """
        return list(self.analysis_classes.keys())

    def analyze_data(
        self, input_data: Union[np.ndarray, List[np.ndarray]], analysis_type: str
    ) -> Union[float, Dict[str, Any]]:
        """
        Analyze data using the specified analysis type.

        Args:
            input_data: Input data (numpy array or list of arrays)
            analysis_type (str): Type of analysis to perform

        Returns:
            Union[float, Dict[str, Any]]: Analysis result

        Raises:
            ValueError: If analysis type is not supported
        """
        if analysis_type not in self.analysis_classes:
            raise ValueError(f"Unsupported analysis type: {analysis_type}")

        analysis_class = self.analysis_classes[analysis_type]

        # Aggregate data
        aggregated_data = analysis_class.aggregate_data(input_data)

        # Perform analysis
        result = analysis_class.analyze(aggregated_data)
        return result
