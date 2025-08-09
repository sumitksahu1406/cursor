from src.pipelines.bronze_ingest import run as run_bronze
from src.pipelines.silver_transform import run as run_silver
from src.pipelines.gold_aggregates import run as run_gold


def main() -> None:
    run_bronze()
    run_silver()
    run_gold()


if __name__ == "__main__":
    main()