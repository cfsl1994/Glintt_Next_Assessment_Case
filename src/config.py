from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    base_path: str

    @property
    def raw_dir(self) -> str:
        return f"{self.base_path}/raw"

    @property
    def processed_dir(self) -> str:
        return f"{self.base_path}/processed"

    # Silver
    @property
    def silver_athlete_delta(self) -> str:
        return f"{self.base_path}/silver/athlete_snapshot_delta"

    @property
    def silver_games_delta(self) -> str:
        return f"{self.base_path}/silver/games_snapshot_delta"

    @property
    def silver_results_delta(self) -> str:
        return f"{self.base_path}/silver/results_snapshot_delta"

    # Gold
    @property
    def gold_dim_athlete(self) -> str:
        return f"{self.base_path}/gold/dim_athlete_delta"

    @property
    def gold_dim_games(self) -> str:
        return f"{self.base_path}/gold/dim_games_delta"

    @property
    def gold_fact_results(self) -> str:
        return f"{self.base_path}/gold/fact_olympic_results_delta"