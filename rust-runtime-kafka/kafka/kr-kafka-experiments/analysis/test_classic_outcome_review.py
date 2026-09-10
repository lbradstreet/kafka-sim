import unittest

from classic_outcome_review import review_failures


class OutcomeReviewTest(unittest.TestCase):
    def test_timeout_is_ambiguous_but_notwritten_must_be_absent(self):
        report = {'failed': 1, 'environment': {'log': [{'id': 42}]},
                  'deliveries': [{'id': 42, 'success': False, 'error': 'TimeoutException'}]}
        self.assertTrue(review_failures(report)[0]['stored_despite_failure'])
        report['deliveries'][0] = {'id': 42, 'success': False, 'outcome': 2, 'reason': 1, 'attempts': 1}
        self.assertTrue(review_failures(report)[0]['stored_despite_failure'])
        report['deliveries'][0]['outcome'] = 1
        with self.assertRaisesRegex(ValueError, 'NotWritten record exists'):
            review_failures(report)
        report['environment']['log'] = []
        self.assertFalse(review_failures(report)[0]['stored_despite_failure'])


if __name__ == '__main__':
    unittest.main()
