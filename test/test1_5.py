"""Four graph shapes and one explicit hub; run in common without arguments."""

from easy_agent import Graph, HubNode, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"


def one_to_one(env_file=ENV_FILE) -> list[Info]:
    source = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                  prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    target = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                  prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    graph = Graph()
    graph.involve(nodes=[target, source], relations=[(source, target)], start=[source], end=[target])
    return graph.run([Info("1")])


def one_to_many(env_file=ENV_FILE) -> list[Info]:
    source = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                  prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    left = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    right = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                 prompt="Add 2 to the input integer. Reply with only the resulting digit.")
    graph = Graph(execution="async", max_concurrency=2)
    graph.involve(
        nodes=[right, source, left], relations=[(source, left), (source, right)],
        start=[source], end=[right, left],
    )
    return graph.run([Info("1")])


def many_to_one(env_file=ENV_FILE) -> list[Info]:
    left = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    right = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                 prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    target = Node(
        env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
        prompt="The input is a JSON object with numeric string values. Add all its values as numbers. "
               "Reply with only the sum as one digit.",
    )
    graph = Graph(execution="async", max_concurrency=2)
    graph.involve(
        nodes=[target, right, left], relations=[(right, target), (left, target)],
        start=[left, right], end=[target],
    )
    return graph.run([Info("1"), Info("3")])


def many_to_many(env_file=ENV_FILE) -> list[Info]:
    left = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    right = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                 prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    writer = Node(
        env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
        prompt="The input is a JSON object with numeric string values. Add all its values as numbers. "
               "Reply with only the sum as one digit.",
    )
    reviewer = Node(
        env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
        prompt="The input is a JSON object with numeric string values. Multiply all its values as numbers. "
               "Reply with only the product as one digit.",
    )
    graph = Graph(execution="async", max_concurrency=2)
    graph.involve(
        nodes=[reviewer, right, writer, left],
        relations=[(left, writer), (right, writer), (right, reviewer), (left, reviewer)],
        start=[left, right], end=[reviewer, writer],
    )
    return graph.run([Info("1"), Info("3")])


def hub_many_to_many(env_file=ENV_FILE) -> list[Info]:
    left = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    right = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                 prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    hub = HubNode(
        env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=128,
        input_count=2, output_count=2, input_keys=["left", "right"],
        rules=[
            "Add the numeric values left and right. This output contains their sum as one digit.",
            "Multiply the numeric values left and right. This output contains their product as one digit.",
        ],
    )
    writer = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                  prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    reviewer = Node(env_file=env_file, timeout=30, max_attempts=1, max_output_tokens=16,
                    prompt="Add 1 to the input integer. Reply with only the resulting digit.")
    graph = Graph(execution="async", max_concurrency=2)
    graph.involve(
        nodes=[reviewer, right, hub, writer, left],
        relations=[
            (right, hub.inputs["right"]), (left, hub.inputs["left"]),
            (hub.outputs[0], writer), (hub.outputs[1], reviewer),
        ],
        start=[left, right], end=[reviewer, writer],
    )
    return graph.run([Info("1"), Info("3")])


if __name__ == "__main__":
    for workflow in (one_to_one, one_to_many, many_to_one, many_to_many, hub_many_to_many):
        print(workflow.__name__, [info.text for info in workflow()])
