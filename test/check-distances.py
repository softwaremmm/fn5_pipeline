import argparse
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--actual", required=True, help="Path to the actual distances JSON file"
    )
    parser.add_argument(
        "--expected", required=True, help="Path to the expected distances JSON file"
    )
    parser.add_argument(
        "--run-ids", required=True, nargs="+", type=str, help="List of run IDs to check"
    )
    parser.add_argument(
        "--test-ids",
        required=True,
        nargs="+",
        type=str,
        help="List of test IDs to check",
    )
    args = parser.parse_args()

    # Load actual distances
    with open(args.actual) as f:
        actual_dists = json.load(f)

    # Load expected distances
    with open(args.expected) as f:
        expected_dists = json.load(f)

    run_id_map = {
        run_id: test_id for test_id, run_id in zip(args.test_ids, args.run_ids)
    }

    # Subset actual dists to just those which include the given run IDs
    if actual_dists["distances"] is not None:
        actual_dists = {
            "distances": {
                run_id_map[run]: dist
                for run, dist in actual_dists["distances"].items()
                if run in args.run_ids
            },
            "QC": actual_dists.get("QC", {}),
        }

    failed = False
    if actual_dists["QC"] != expected_dists["QC"]:
        failed = True
        print("QC sections do not match.")
        print("Actual QC:", actual_dists["QC"])
        print("Expected QC:", expected_dists["QC"])
        print()

    if (
        actual_dists["distances"] is not None
        and expected_dists["distances"] is not None
        and len(actual_dists["distances"]) != len(expected_dists["distances"])
    ):
        failed = True
        print("Number of runs in distances do not match.")
        print("Actual runs:", list(actual_dists["distances"].keys()))
        print("Expected runs:", list(expected_dists["distances"].keys()))
        print()

    if actual_dists["distances"] != expected_dists["distances"]:
        failed = True
        print("Distances do not match.")
        print("Actual distances:", actual_dists["distances"])
        print("Expected distances:", expected_dists["distances"])
        print()

    if failed:
        print("Test failed.")
        exit(1)
    else:
        print("Results match!")


if __name__ == "__main__":
    main()
