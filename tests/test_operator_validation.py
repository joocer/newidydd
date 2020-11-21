"""
Test Validator
"""

import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import newidydd


def test_operator_validator_case_one():
    # CASE ONE = ALL VALID VALUES
    CASE_ONE_DATA = { 
        "string_field": "string",
        "integer_field": 100,
        "boolean_field": True,
        "date_field": "2000-01-01T00:00:00.000",
        "other_field": ["abc"],
        "nullable_field": None
    }
    CASE_ONE_SCHEMA = {
        "fields": [
            { "name": "string_field",   "type": "string"  },
            { "name": "integer_field",  "type": "numeric" },
            { "name": "boolean_field",  "type": "boolean" },
            { "name": "date_field",     "type": "date"    },
            { "name": "other_field",    "type": "other"   },
            { "name": "nullable_field", "type": "null"    }
        ]
    }

    test = newidydd.operations.ValidationOperator(CASE_ONE_SCHEMA)
    result = test.execute(CASE_ONE_DATA)
    assert (result != None), "[CASE ONE] Validator failed testing well-formed data"
    print("[CASE ONE] Passed")


def test_operator_validator_case_two():
    # CASE TWO = INVALID STRING
    CASE_TWO_DATA = { "string_field": 100 }
    CASE_TWO_SCHEMA = { "fields": [ { "name": "string_field", "type": "string" } ] }

    test = newidydd.operations.ValidationOperator(CASE_TWO_SCHEMA)
    result = test.execute(CASE_TWO_DATA)
    assert (not result), "[CASE TWO] Validator failed testing invalid string"
    print("[CASE TWO] Passed")


def test_operator_validator_case_three():
    # CASE THREE = INVALID NUMBER
    CASE_THREE_DATA = { "number_field": "one hundred" }
    CASE_THREE_SCHEMA = { "fields": [ { "name": "number_field", "type": "numeric" } ] }

    test = newidydd.operations.ValidationOperator(CASE_THREE_SCHEMA)
    result = test.execute(CASE_THREE_DATA)
    assert (not result), "[CASE THREE] Validator failed testing invalid number"
    print("[CASE THREE] Passed")


test_operator_validator_case_one()
test_operator_validator_case_two()
test_operator_validator_case_three()
