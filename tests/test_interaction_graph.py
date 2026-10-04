import unittest

from sentinelagent.interaction_graph import InteractionGraph


class TestInteractionGraph(unittest.TestCase):
    def test_builds_nodes_and_directed_edges(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-B": 2,
                    "runtime-C": 1,
                },
                "runtime-B": {
                    "runtime-C": 1,
                },
            }
        )

        self.assertEqual(
            graph.nodes(),
            ["runtime-A", "runtime-B", "runtime-C"],
        )

        self.assertEqual(
            graph.edges(),
            [
                {
                    "source_runtime_id": "runtime-A",
                    "target_runtime_id": "runtime-B",
                    "interaction_count": 2,
                    "repeated": True,
                },
                {
                    "source_runtime_id": "runtime-A",
                    "target_runtime_id": "runtime-C",
                    "interaction_count": 1,
                    "repeated": False,
                },
                {
                    "source_runtime_id": "runtime-B",
                    "target_runtime_id": "runtime-C",
                    "interaction_count": 1,
                    "repeated": False,
                },
            ],
        )

    def test_identifies_pairwise_coordination(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-B": 3,
                },
                "runtime-C": {
                    "runtime-D": 1,
                },
            }
        )

        self.assertEqual(
            graph.pairwise_coordination(),
            [
                {
                    "source_runtime_id": "runtime-A",
                    "target_runtime_id": "runtime-B",
                    "interaction_count": 3,
                    "signals": ["REPEATED_INTERACTION"],
                }
            ],
        )

    def test_identifies_group_coordination_through_shared_repeated_target(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-T": 2,
                },
                "runtime-B": {
                    "runtime-T": 3,
                },
                "runtime-C": {
                    "runtime-T": 1,
                },
            }
        )

        self.assertEqual(
            graph.group_coordination(),
            [
                {
                    "member_runtime_ids": [
                        "runtime-A",
                        "runtime-B",
                    ],
                    "shared_target_runtime_id": "runtime-T",
                    "interaction_counts": {
                        "runtime-A": 2,
                        "runtime-B": 3,
                    },
                    "signals": ["SHARED_REPEATED_TARGET"],
                }
            ],
        )

    def test_single_interaction_does_not_create_coordination_evidence(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-B": 1,
                },
                "runtime-C": {
                    "runtime-B": 1,
                },
            }
        )

        self.assertEqual(graph.pairwise_coordination(), [])
        self.assertEqual(graph.group_coordination(), [])

    def test_invalid_interaction_data_is_ignored(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-B": 2,
                    "runtime-C": -1,
                    "runtime-D": True,
                    "": 5,
                },
                "": {
                    "runtime-E": 2,
                },
                "runtime-F": "invalid",
            }
        )

        self.assertEqual(
            graph.edges(),
            [
                {
                    "source_runtime_id": "runtime-A",
                    "target_runtime_id": "runtime-B",
                    "interaction_count": 2,
                    "repeated": True,
                }
            ],
        )

    def test_snapshot_contains_all_graph_evidence(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-B": 2,
                },
                "runtime-C": {
                    "runtime-B": 2,
                },
            }
        )

        snapshot = graph.snapshot()

        self.assertEqual(
            snapshot["nodes"],
            ["runtime-A", "runtime-B", "runtime-C"],
        )
        self.assertEqual(len(snapshot["edges"]), 2)
        self.assertEqual(len(snapshot["pairwise_coordination"]), 2)
        self.assertEqual(len(snapshot["group_coordination"]), 1)

    def test_update_replaces_previous_graph_evidence(self):
        graph = InteractionGraph(
            {
                "runtime-A": {
                    "runtime-B": 2,
                }
            }
        )

        graph.update(
            {
                "runtime-C": {
                    "runtime-D": 3,
                }
            }
        )

        self.assertEqual(
            graph.nodes(),
            ["runtime-C", "runtime-D"],
        )


if __name__ == "__main__":
    unittest.main()
