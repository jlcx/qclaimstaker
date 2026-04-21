from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="QCS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dsn: str = "postgresql:///algae"
    dump_version: str = "unknown"

    min_wp_count: int = 20
    subclass_max_depth: int = 10
    transitive_max_depth: int = 6
    transitive_pids: list[str] = Field(
        default_factory=lambda: ["P361", "P527", "P131", "P276", "P279", "P171"]
    )

    top_k: int = 3
    auto_score_threshold: float = 0.8
    auto_margin_threshold: float = 0.2
    smoothing_alpha: float = 1.0

    # Properties forced to review_queue regardless of score.
    always_review: list[str] = Field(
        default_factory=lambda: ["P31", "P279", "P361", "P527"]
    )

    # Weights for the linear score. Priors are in [0,1]; specificity in {0,1,2};
    # log-usage and log-wp are normalized before weighting.
    w_prior: float = 1.0
    w_specificity: float = 0.15
    w_usage_penalty: float = 0.10
    w_evidence: float = 0.25

    # Output / emitter
    qs_output_dir: str = "out"
    qs_reference_template: str = "inferred from Wikipedia link graph, dump {dump_version}"

    # Review UI
    review_bind: str = "127.0.0.1"
    review_port: int = 8765
    review_lang: str = "en"
    reviewer_name: str = "anon"


settings = Settings()
