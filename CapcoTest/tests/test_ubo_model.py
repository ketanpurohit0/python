import datetime
import unittest

from pydantic import ValidationError

from TreeStruct import TransactionTypes, UBOTypes, Transaction


class TestUboModels(unittest.TestCase):
    def test_transaction_types(self):
        tt_div = TransactionTypes.DIV
        tt_int = TransactionTypes.INT
        self.assertEqual(tt_div.value, "DIV")
        self.assertEqual(tt_int.value, "INT")

    def test_ubo_types(self):
        ubo_w9 = UBOTypes.W9
        self.assertEqual(ubo_w9.value, "W-9")

    def test_is_w8_w9(self):
        ubo_w9 = UBOTypes.W9
        self.assertTrue(UBOTypes.is_W9(ubo_w9))
        self.assertFalse(UBOTypes.is_W8(ubo_w9))

        ubo_w8 = UBOTypes.W8_BEN
        self.assertTrue(UBOTypes.is_W8(ubo_w8))
        self.assertFalse(UBOTypes.is_W9(ubo_w8))

    def test_good_transaction_model(self):
        t0 = Transaction(amount=0, valueDate=datetime.date(2022, 12, 1), transactionType=TransactionTypes.INT)
        t1 = Transaction(amount=40, valueDate=datetime.date(2022, 12, 1), transactionType="INT")

    def test_bad_transaction_model(self):
        # amount, transaction type
        values = [
            (0, "NOTVALID"),    # Not a valid transaction type
            (-1, TransactionTypes.INT),  # Negative value not allowed
        ]

        for amount_, transactionType_ in values:
            with self.subTest(f"{amount_}, {transactionType_}"):
                with self.assertRaises(ValidationError) as context:
                    _ = Transaction(amount=amount_, valueDate=datetime.date(2022, 12, 1), transactionType=transactionType_)


