import json
import unittest

from render_request_policy import decimals, render
from request_policy_review import METRICS


class RequestDashboardTest(unittest.TestCase):
    def test_full_width_fields_are_canonical_strings(self):
        values = {key: 2**64 - 1 for key in METRICS}
        self.assertEqual(set(decimals(values).values()), {'18446744073709551615'})
        values['wire_bytes'] = 2**64
        with self.assertRaises(ValueError):
            decimals(values)
        values['wire_bytes'] = 0.5
        with self.assertRaises(ValueError):
            decimals(values)

    def test_embedded_metadata_cannot_terminate_the_data_script(self):
        data = {'schema': 'kr-request-policy-dashboard/v1',
                'cases': [{'description': ['</script><script>alert(1)</script>\u2028']}], 'provenance': []}
        page = render(data)
        self.assertNotIn('</script><script>alert', page)
        prefix = '<script>globalThis.REQUEST_POLICY_DATA = '
        payload = page.split(prefix)[1].split(';</script>')[0]
        self.assertEqual(json.loads(payload), data)
        self.assertNotIn('<script src=', page)
        self.assertNotIn('<link rel="stylesheet"', page)
        self.assertEqual(page, render(data))


if __name__ == '__main__':
    unittest.main()
