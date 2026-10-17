"""Run this file in the common environment; configuration is below."""

from easy_agent import Graph, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"


def run(env_file=ENV_FILE) -> list[Info]:
    source = Node(
        env_file=env_file, max_output_tokens=16, timeout=30, max_attempts=1,
        prompt="Reply with exactly CHAIN_A. No other text.",
    )
    target = Node(
        env_file=env_file, max_output_tokens=32, timeout=30, max_attempts=1,
        prompt="If the user message is exactly CHAIN_A, reply TEST1_2_OK. "
               "Otherwise reply TEST1_2_INPUT_ERROR. No other text.",
    )
    graph = Graph()
    graph.involve(nodes=[source, target], relations=[(source, target)], start=[source], end=[target])
    return graph.run([Info('Start test1-2')])


if __name__ == "__main__":
    print(run()[0].text)
