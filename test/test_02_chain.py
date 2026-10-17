"""Two-node public API example; configure below and run this file."""

from easy_agent import Graph, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"


def run(env_file=ENV_FILE) -> list[Info]:
    graph = Graph(env_file=env_file, timeout=30, max_output_tokens=32, max_attempts=1)
    graph.first = Node(prompt='Reply with exactly CHAIN_A. No other text.')
    graph.second = Node(prompt='If the user message is exactly CHAIN_A, reply CHAIN_B. '
                               'Otherwise reply CHAIN_INPUT_ERROR. No other text.')
    graph.first.send_to(graph.second)
    return graph.run([Info('Start the chain')])


if __name__ == "__main__":
    print(run()[0].text)
