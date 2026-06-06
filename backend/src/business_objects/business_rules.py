"""Shared business rule helpers for business object validation."""

from decimal import Decimal

from core.app import App
from core.base_objects import Config
from core.const import DEFAULT_DECIMAL_SCALE_DIGITS, MAX_DECIMAL_TOTAL_DIGITS


def configured_money_scale_digits() -> int:
    """Configured money scale digits with strict validation."""
    try:
        configured_digits = App.get_config_item(
            Config.CONFIG_APP_MONEY_SCALE, DEFAULT_DECIMAL_SCALE_DIGITS
        )
    except ReferenceError:
        return DEFAULT_DECIMAL_SCALE_DIGITS

    if not isinstance(configured_digits, (int, str)):
        raise ValueError(
            "Invalid app.money_scale_digits type: "
            f"{type(configured_digits).__name__}. Expected int or str."
        )

    try:
        digits = int(configured_digits)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid app.money_scale_digits value: "
            f"{configured_digits!r}. Expected an integer."
        ) from exc

    if digits < 0 or digits > MAX_DECIMAL_TOTAL_DIGITS:
        raise ValueError(
            "app.money_scale_digits out of supported range [0, "
            f"{MAX_DECIMAL_TOTAL_DIGITS}]: {digits}"
        )
    return digits


def configured_money_scale_multiplier() -> int:
    """Scaling multiplier derived from configured money scale digits."""
    return 10**configured_money_scale_digits()


def configured_money_max_scaled_value() -> int:
    """Maximum absolute scaled integer for the shared fixed precision envelope."""
    return (10**MAX_DECIMAL_TOTAL_DIGITS) - 1


def validate_money_decimal(value: Decimal) -> None:
    """Validate a Decimal against the configured monetary precision rules."""
    scale_digits = configured_money_scale_digits()
    quantizer = Decimal(1).scaleb(-scale_digits)
    quantized = value.quantize(quantizer)
    if quantized != value:
        raise ValueError(
            f"Value '{value}' exceeds configured decimal scale of {scale_digits} digits"
        )

    max_integer_digits = MAX_DECIMAL_TOTAL_DIGITS - scale_digits
    max_abs_value = Decimal(10) ** max_integer_digits
    if abs(quantized) >= max_abs_value:
        raise ValueError(
            f"Value '{value}' exceeds configured decimal precision of {MAX_DECIMAL_TOTAL_DIGITS} total digits"
        )