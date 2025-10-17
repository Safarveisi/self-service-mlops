import math

import pytest
from check_k8s_resources import parse_cpu, parse_memory


@pytest.mark.parametrize(
    (
        "input_s, expected",
        [
            ("250m", 0.25),  # standard milli format
            ("100m", 0.1),
            ("2000m", 2.0),  # large milli -> cores
            ("12.5m", 0.0125),  # nonstandard but supported: float before 'm'
            ("12m", 0.012),  # integer before 'm'
            ("0m", 0.0),  # zero milli
            ("2", 2.0),  # integer cores
            ("0.5", 0.5),  # fractional cores
            ("  250m  ", 0.25),  # whitespace trimming
            ("abc123.5def", 123.5),  # fallback: extract number with decimal
            ("abc123mxyz", 0.123),  # fallback + milli suffix -> extracted number divided by 1000
            ("12.5", 12.5),  # plain float
        ],
    ),
)
def test_parse_cpu_various(input_s, expected):
    val = parse_cpu(input_s)
    assert math.isclose(val, expected, rel_tol=1e-9), f"{input_s!r} -> {val}, want {expected}"


def test_parse_cpu_none_and_empty():
    # function casts None -> "None" then falls back to no digits -> 0.0
    assert parse_cpu(None) == 0.0
    assert parse_cpu("") == 0.0


def test_parse_cpu_no_digits_returns_zero():
    assert parse_cpu("no-digits-here") == 0.0


def test_parse_cpu_lone_m_returns_zero():
    # "m" or strings that end with 'm' but contain no numeric prefix should return 0.0
    assert parse_cpu("m") == 0.0
    assert parse_cpu("  m  ") == 0.0


def test_parse_cpu_malformed_but_extracts_first_number():
    # For strings like 'prefix12.34suffixm' the parser should use the first numeric token
    assert math.isclose(parse_cpu("prefix12.34suffixm"), 0.01234, rel_tol=1e-9)


@pytest.mark.parametrize(
    (
        "input_s, expected",
        [
            ("128Mi", 128 * 1024**2),
            ("1Gi", 1 * 1024**3),
            ("123Ki", 123 * 1024),
            ("1024", 1024.0),  # plain integer bytes
            (1024, 1024.0),  # numeric input
            ("5Ti", 5 * 1024**4),
            ("1Pi", 1 * 1024**5),
            ("  64Mi  ", 64 * 1024**2),  # whitespace trimming
        ],
    ),
)
def test_parse_memory_units(input_s, expected):
    val = parse_memory(input_s)
    assert math.isclose(val, expected, rel_tol=1e-9), f"{input_s} -> {val}, want {expected}"


def test_parse_memory_fallback_extracts_decimal_number():
    # fallback should pick up decimal numbers in arbitrary strings
    assert math.isclose(parse_memory("foo123.5bar"), 123.5, rel_tol=1e-9)


def test_parse_memory_empty_or_no_digits():
    assert parse_memory("") == 0.0
    assert parse_memory("no-digits") == 0.0


def test_parse_memory_zero():
    assert parse_memory("0") == 0.0
