"""One-node public API example; configure below and run this file."""

from easy_agent import Graph, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"


def run(env_file=ENV_FILE) -> list[Info]:
    graph = Graph(env_file=env_file, timeout=30, max_output_tokens=32, max_attempts=1)
    graph.answer = Node(prompt='Reply with exactly API_TEST_OK. No other text.')
    return graph.run([Info('First API connectivity test')])


if __name__ == "__main__":
    print(run()[0].text)
