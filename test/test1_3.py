"""Run in common with no arguments; relations and entry determine execution order."""

from easy_agent import Graph, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"


def run(env_file=ENV_FILE) -> list[Info]:
    source = Node(
        env_file=env_file, max_output_tokens=16, timeout=30, max_attempts=1,
        prompt="Reply with exactly CHAIN_A. No other text.",
    )
    middle = Node(
        env_file=env_file, max_output_tokens=24, timeout=30, max_attempts=1,
        prompt="If the user message is exactly CHAIN_A, reply CHAIN_B. "
               "Otherwise reply INPUT_ERROR. No other text.",
    )
    target = Node(
        env_file=env_file, max_output_tokens=32, timeout=30, max_attempts=1,
        prompt="If the user message is exactly CHAIN_B, reply TEST1_3_OK. "
               "Otherwise reply INPUT_ERROR. No other text.",
    )
    graph = Graph()
    graph.involve(
        nodes=[target, source, middle],
        relations=[(middle, target), (source, middle)],
        start=[source], end=[target],
    )
    return graph.run([Info("Start test1-3")])


if __name__ == "__main__":
    print(run()[0].text)
