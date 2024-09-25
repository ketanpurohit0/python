import datetime
import unittest

from pydantic import ValidationError

from TreeStruct import UBO, Transaction, TransactionTypes, UBOTypes


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
        Transaction(amount=0, valueDate=datetime.date(2022, 12, 1), transactionType=TransactionTypes.INT)
        Transaction(amount=40, valueDate=datetime.date(2022, 12, 1), transactionType="INT")

    def test_bad_transaction_model(self):
        # amount, transaction type
        values = [
            (0, "NOTVALID"),  # Not a valid transaction type
            (-1, TransactionTypes.INT),  # Negative value not allowed
        ]

        for amount_, transactionType_ in values:
            with self.subTest(f"{amount_}, {transactionType_}"):
                with self.assertRaises(ValidationError):
                    _ = Transaction(amount=amount_, valueDate=datetime.date(2022, 12, 1),
                                    transactionType=transactionType_)

    def test_validity_for_dates(self):
        ubo = UBO(parent_code=None, code="C1", uboType=UBOTypes.W9,
                  fromDate=datetime.date(2022, 1, 1),
                  toDate=datetime.date(2022, 12, 31))

        self.assertTrue(all(ubo.is_valid_for_allocation(vd) for vd in [datetime.date(2022, 12, 1),
                                                                       datetime.date(2022, 1, 1),
                                                                       datetime.date(2022, 12, 31)]))

    def test_not_validity_for_dates(self):
        ubo = UBO(parent_code=None, code="C1", uboType=UBOTypes.W9,
                  fromDate=datetime.date(2022, 1, 1),
                  toDate=datetime.date(2022, 12, 31))

        self.assertTrue(all(not ubo.is_valid_for_allocation(vd) for vd in [datetime.date(2021, 12, 1),
                                                                           datetime.date(2023, 1, 1),
                                                                           datetime.date(2025, 12, 31)]))
