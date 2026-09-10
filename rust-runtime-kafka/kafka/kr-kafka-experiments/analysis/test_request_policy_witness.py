import unittest

from request_policy_witness import decimal_fields


class RequestWitnessEncodingTest(unittest.TestCase):
    def test_wide_clocks_and_signed_fields_survive_json_consumers(self):
        self.assertEqual({'at': '18446744073709551615', 'fields': ['-1', '0', '1'], 'ok': True},
                         decimal_fields({'at': 2**64 - 1, 'fields': [-1, 0, 1], 'ok': True}))


if __name__ == '__main__':
    unittest.main()
