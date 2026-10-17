"""Three simple workflows; run this file in common without arguments."""

from easy_agent import Graph, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"


def one_to_one(env_file=ENV_FILE) -> list[Info]:
    source = Node(
        env_file=env_file, max_output_tokens=16, timeout=30, max_attempts=1,
        prompt="If the user message is INPUT_A, reply A_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    target = Node(
        env_file=env_file, max_output_tokens=24, timeout=30, max_attempts=1,
        prompt="If the user message is A_OK, reply TEST1_4_1_1_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    graph = Graph()
    graph.involve(nodes=[target, source], relations=[(source, target)], start=[source], end=[target])
    return graph.run([Info("INPUT_A")])


def many_to_one(env_file=ENV_FILE) -> list[Info]:
    left = Node(
        env_file=env_file, max_output_tokens=16, timeout=30, max_attempts=1,
        prompt="If the user message is INPUT_A, reply A_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    right = Node(
        env_file=env_file, max_output_tokens=16, timeout=30, max_attempts=1,
        prompt="If the user message is INPUT_B, reply B_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    target = Node(
        env_file=env_file, max_output_tokens=32, timeout=30, max_attempts=1,
        prompt="The user message is a JSON object. If its values contain exactly A_OK and B_OK, "
               "reply TEST1_4_2_1_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    graph = Graph(execution="async", max_concurrency=2)
    graph.involve(
        nodes=[target, right, left], relations=[(right, target), (left, target)],
        start=[left, right], end=[target],
    )
    return graph.run([Info("INPUT_A"), Info("INPUT_B")])


def one_to_many(env_file=ENV_FILE) -> list[Info]:
    source = Node(
        env_file=env_file, max_output_tokens=16, timeout=30, max_attempts=1,
        prompt="If the user message is INPUT_A, reply A_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    left = Node(
        env_file=env_file, max_output_tokens=32, timeout=30, max_attempts=1,
        prompt="If the user message is A_OK, reply TEST1_4_1_2_LEFT_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    right = Node(
        env_file=env_file, max_output_tokens=32, timeout=30, max_attempts=1,
        prompt="If the user message is A_OK, reply TEST1_4_1_2_RIGHT_OK. Otherwise reply INPUT_ERROR. No other text.",
    )
    graph = Graph(execution="async", max_concurrency=2)
    graph.involve(
        nodes=[left, source, right], relations=[(source, left), (source, right)],
        start=[source], end=[right, left],
    )
    return graph.run([Info("INPUT_A")])


if __name__ == "__main__":
    for workflow in (one_to_one, many_to_one, one_to_many):
        print(workflow.__name__, [info.text for info in workflow()])
