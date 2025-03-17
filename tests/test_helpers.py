"""Testing helper functions."""

import unittest
import time

try:
    from src.nts.service.__helpers import time_ms
except ModuleNotFoundError:
    from nts.service.__helpers import time_ms


class TestHelpers(unittest.TestCase):
    """
    Test functions inside __helpers.py.
    """

    def test_time_ms(self) -> None:
        """
        time_ms must return the current time in milliseconds since the epoch.
        """
        now_ms = time_ms()
        now_s = time.time()
        # Assert two sequential time stamps are closer than 1 second.
        self.assertTrue(abs(now_ms - now_s * 1e3) < 1e3)


if __name__ == "__main__":
    unittest.main()
