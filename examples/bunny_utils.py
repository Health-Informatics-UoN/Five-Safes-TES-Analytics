import pandas as pd
from five_safes_tes_analytics.utils.parse_bunny import parse_bunny
import altair as alt
import altair_upset as au

class DistributionCodesets:
    """
    A class for comparing multiple distribution queries from Bunny outputs

    Attributes
    ----------
    table_names: list[str]
        The names of tables loaded from Bunny JSON
    tables: dict[str, pd.DataFrame]
        Tables loaded from Bunny JSON
    """
    def __init__(self, table_paths: dict[str, str]) -> None:
        self.table_names = list(table_paths.keys())
        self.tables = build_tables(table_paths)

    @property
    def counts_by_TRE(self) -> pd.DataFrame:
        """
        Get the counts for each code as a DataFrame with a column for each TRE.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame where each row is an OMOP code, each column is a TRE, and each cell is a count of individuals
        """
        dfs = [
            df
                .loc[df["OMOP"] != 0]
        for df in self.tables.values()]
        return pd.concat(dfs).pivot(index="OMOP", columns="TRE", values="COUNT")

    @property
    def tre_memberships(self) -> pd.Series:
        """
        Get a series of string descriptions of what TRE a particular code is present in

        Returns
        -------
        pd.Series
            A pandas series of TRE memberships for each OMOP code
        """
        counts = self.counts_by_TRE
        return pd.Series(counts.apply(
            lambda row: str(list([counts.columns[i] for i, x in enumerate(row) if x > 0])),
                            axis=1
        ))

    @property
    def code_intersections(self) -> pd.DataFrame:
        """
        A count of the codes TREs have in common, as well as unique codes

        Returns
        -------
        pd.DataFrame
            A dataframe showing how many codes are in each combination of TREs
        """
        membership = self.tre_memberships
        return pd.DataFrame(membership.groupby(membership).count())

    @property
    def all_descriptions(self) -> pd.DataFrame:
        """
        Get all the OMOP_DESCR (OMOP descriptions) for the codes in all datasets

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame with the unique OMOP codes and their descriptions
        """
        description_tables = [v[["OMOP", "OMOP_DESCR"]] for v in self.tables.values()]
        return pd.DataFrame(pd.concat(description_tables).groupby("OMOP").first())

    def get_codes_by_membership(self, membership_string: str) -> pd.DataFrame:
        """
        Passing in a string describing a particular combination of TREs, get the codes with that TRE membership

        Parameters
        ----------
        membership_string: str
            A string describing some combination of TREs

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame of the code counts matching the described TRE membership
        """
        membership = self.tre_memberships.reset_index()
        membership.columns = ["OMOP", "membership"]
        filtered_membership = membership[membership["membership"] == membership_string]
        return pd.DataFrame(filtered_membership)

    def plot_top_k_by_count(self, k: int) -> alt.Chart:
        """
        Plot the k codes that have the highest counts across TREs

        Parameters
        ----------
        k: int
            How many codes you want to plot

        Returns
        -------
        alt.Chart
            A bar chart showing the counts of the top k codes, coloured by TRE
        """
        counts = self.counts_by_TRE.fillna(0)
        counts["total"] = counts.apply(lambda row: sum(row), axis=1)
        top_k_by_count = counts.sort_values(by="total", ascending=False).drop("total", axis=1)[:k]
        top_k_by_count = top_k_by_count.stack().reset_index()
        top_k_by_count.columns = ["OMOP", "TRE", "Count"]
        top_k_by_count = top_k_by_count.join(self.all_descriptions, on="OMOP")
        return count_bar(top_k_by_count)

    def plot_by_codes(self, descriptions: list[str]) -> alt.Chart:
        """
        Plot the specified codes

        Parameters
        ----------
        descriptions: list[str]
            A list of OMOP codes

        Returns
        -------
        alt.Chart
            A bar chart showing the counts of the specified codes, coloured by TRE
        """
        counts = self.counts_by_TRE.fillna(0).stack().reset_index()
        counts.columns = ["OMOP", "TRE", "Count"]
        matching_counts = counts[counts["OMOP"].isin(descriptions)]
        for_plot = matching_counts.join(self.all_descriptions, on="OMOP")
        return count_bar(for_plot)

    def plot_count_heatmap(self) -> alt.Chart:
        """
        Plot a heatmap of the codes found in each combination of two TREs

        Returns
        -------
        alt.Chart
            A heatmap with TRE names on the x and y axes, and the number of codes found in both TREs in each cell
        """
        counts = self.counts_by_TRE

        tre_mat = []

        for tre1 in self.tables.keys():
            for tre2 in self.tables.keys():
                intersection = counts[[tre1, tre2]].dropna()
                total = counts[tre1].dropna()
                tre_mat.append({"tre1": tre1, "tre2": tre2, "count": len(intersection), "fraction": len(intersection)/len(total)})
        
        return alt.Chart(pd.DataFrame(tre_mat)).mark_rect(cornerRadius=20).encode(
                alt.X('tre1'),
                alt.Y('tre2'),
                alt.Color('fraction').scale(scheme="greens"),
                alt.Tooltip('count')
            )

    def plot_upset(self) -> alt.Chart:
        """
        Render an Upset plot for the codes found in each combination of TREs

        Returns
        -------
        alt.Chart
            An Upset plot for the codes, where each bar shows the count of codes in a combination of TREs.
            Hovering over will help you read it.
        """
        # This nonsense is necessary because the upsetplot library throws a wobbly with
        # the funny indices you get from the pivot
        data = pd.DataFrame(
                self
                .counts_by_TRE
                .map(lambda x: 1 if x > 0 else 0)
                .reset_index(drop=True)
                .to_dict()
                )

        return au.UpSetAltair(
                data=data,
                sets=list(self.tables.keys()),
                title="Codes in datasets"
                )




def build_tables(table_names: dict[str, str]) -> dict[str, pd.DataFrame]:
    """
    Load Bunny tables into a dictionary of pandas DataFrames

    Parameters
    ----------
    table_names: dict[str,str]
        A dictionary where the key is the name you want to give your table in the result, and the value is the path to that JSON

    Returns
    -------
    dict[str, pd.DataFrame]
        A dictionary of DataFrames loaded from the specified paths, with the original names
    """
    tables = {k: parse_bunny(path) for k, path in table_names.items()}
    for k, table in tables.items():
        table.insert(0, "TRE", k)
        table.drop(
            [
                "DESCRIPTION", "MIN", "Q1", "MEDIAN", "MEAN", "Q3", "MAX"
            ], axis = 1, inplace=True
        )
    return tables

def count_bar(df: pd.DataFrame) -> alt.Chart:
    """
    Take a DataFrame of OMOP code counts and plot as a stacked bar chart, coloured by TRE

    Returns
    -------
    alt.Chart
        A bar chart
    """
    return alt.Chart(df).mark_bar().encode(
            alt.X("Count"),
            alt.Y("OMOP:N").sort("-x"),
            alt.Color("TRE"),
            alt.Tooltip("OMOP_DESCR")
            )




