import unittest

class TestUtils(unittest.TestCase):
    def test_is_prime(self):
        test = {"ok": "ok"}
        self.assertEqual(test, {})
        
    def test_is_prime2(self):
        test = {"ok": "ok"}
        self.assertEqual(test, test)
        
if __name__ == '__main__':
    unittest.main()