import pandas as pd
from five_safes_tes_analytics.utils.parse_bunny import parse_bunny
import altair as alt
import altair_upset as au

class DistributionCodesets:
    def __init__(self, table_paths: dict[str, str]) -> None:
        self.table_names = list(table_paths.keys())
        self.tables = build_tables(table_paths)

    @property
    def counts_by_TRE(self) -> pd.DataFrame:
        dfs = [
            df
                .loc[df["OMOP"] != 0]
        for df in self.tables.values()]
        return pd.concat(dfs).pivot(index="OMOP", columns="TRE", values="COUNT")

    @property
    def tre_memberships(self) -> pd.Series:
        counts = self.counts_by_TRE
        return pd.Series(counts.apply(
            lambda row: str(list([counts.columns[i] for i, x in enumerate(row) if x > 0])),
                            axis=1
        ))

    @property
    def code_intersections(self) -> pd.DataFrame:
        membership = self.tre_memberships
        return pd.DataFrame(membership.groupby(membership).count())

    @property
    def all_descriptions(self) -> pd.DataFrame:
        description_tables = [v[["OMOP", "OMOP_DESCR"]] for v in self.tables.values()]
        return pd.DataFrame(pd.concat(description_tables).groupby("OMOP").first())

    def get_codes_by_membership(self, membership_string: str) -> pd.DataFrame:
        membership = self.tre_memberships.reset_index()
        membership.columns = ["OMOP", "membership"]
        filtered_membership = membership[membership["membership"] == membership_string]
        return pd.DataFrame(filtered_membership)

    def plot_top_k_by_count(self, k: int) -> alt.Chart:
        counts = self.counts_by_TRE.fillna(0)
        counts["total"] = counts.apply(lambda row: sum(row), axis=1)
        top_k_by_count = counts.sort_values(by="total", ascending=False).drop("total", axis=1)[:k]
        top_k_by_count = top_k_by_count.stack().reset_index()
        top_k_by_count.columns = ["OMOP", "TRE", "Count"]
        top_k_by_count = top_k_by_count.join(self.all_descriptions, on="OMOP")
        return count_bar(top_k_by_count)

    def plot_by_codes(self, descriptions: list[str]) -> alt.Chart:
        counts = self.counts_by_TRE.fillna(0).stack().reset_index()
        counts.columns = ["OMOP", "TRE", "Count"]
        matching_counts = counts[counts["OMOP"].isin(descriptions)]
        for_plot = matching_counts.join(self.all_descriptions, on="OMOP")
        return count_bar(for_plot)

    def plot_count_heatmap(self) -> alt.Chart:
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
                alt.Color('fraction').scale(scheme="viridis"),
                alt.Tooltip('count')
            )

    def plot_upset(self) -> alt.Chart:
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
    return alt.Chart(df).mark_bar().encode(
            alt.X("Count"),
            alt.Y("OMOP:N").sort("-x"),
            alt.Color("TRE"),
            alt.Tooltip("OMOP_DESCR")
            )




