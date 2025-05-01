from src.main.utility.logging_config import logger


def add_two_numbers(a: int, b: int) -> int:
    return a + b


c = add_two_numbers(1, 2)
logger.info(f"{c=}")
