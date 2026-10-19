"""Strict decoding shared by model-produced Hub outputs and tool arguments."""

import json
import math


def load_object(text: str) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError()
            result[key] = value
        return result

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError()
        return number

    def reject_constant(value):
        raise ValueError()

    value = json.loads(text, object_pairs_hook=unique, parse_float=finite_float,
                       parse_constant=reject_constant)
    if not isinstance(value, dict):
        raise ValueError()
    return value
