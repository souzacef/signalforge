from datetime import timedelta

import pytest

from signalforge.outbox.dispatching import DispatchErrorCode, retry_delay


@pytest.mark.parametrize(
    ("attempt", "ceiling"), [(1, 2), (2, 4), (8, 256), (9, 300), (10**100, 300)]
)
@pytest.mark.parametrize("jitter", [0.0, 0.5, 1.0])
def test_retry_delay_equal_jitter(attempt: int, ceiling: int, jitter: float) -> None:
    assert retry_delay(attempt, jitter=jitter) == timedelta(
        seconds=ceiling * (0.5 + jitter / 2)
    )


@pytest.mark.parametrize("attempt", [0, -1])
def test_retry_delay_rejects_nonpositive_attempt(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt_count"):
        retry_delay(attempt, jitter=0.5)


@pytest.mark.parametrize("field", ["base_delay", "max_delay"])
@pytest.mark.parametrize("seconds", [0, -1])
def test_retry_delay_rejects_nonpositive_configuration(
    field: str, seconds: int
) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        retry_delay(1, jitter=0.5, **{field: timedelta(seconds=seconds)})


@pytest.mark.parametrize("jitter", [-0.1, 1.1, float("nan"), float("inf")])
def test_retry_delay_rejects_invalid_jitter(jitter: float) -> None:
    with pytest.raises(ValueError, match="jitter"):
        retry_delay(1, jitter=jitter)


def test_retry_delay_caps_even_when_base_exceeds_maximum() -> None:
    assert retry_delay(
        1, jitter=1, base_delay=timedelta(seconds=20), max_delay=timedelta(seconds=3)
    ) == timedelta(seconds=3)


def test_retry_delay_handles_full_timedelta_range_without_overflow() -> None:
    assert (
        retry_delay(
            10**100,
            jitter=1,
            base_delay=timedelta(microseconds=1),
            max_delay=timedelta.max,
        )
        == timedelta.max
    )
    assert retry_delay(1, jitter=0, base_delay=timedelta(microseconds=1)) == timedelta(
        microseconds=1
    )


def test_dispatch_error_codes_are_bounded_safe_identifiers() -> None:
    for code in DispatchErrorCode:
        assert 0 < len(code.value) <= 64
        assert code.value.replace("_", "").isalpha()
