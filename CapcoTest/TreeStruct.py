import datetime
from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, confloat
from typing_extensions import Self


class TransactionTypes(Enum):
    INT = 'INT'
    DIV = 'DIV'


class UBOTypes(Enum):
    W9 = 'W-9'
    W8_BEN = 'W-8BEN'
    W8_BENE = 'W-8BEN-E'
    W8_ECI = 'W-8ECI'
    W8_EXP = 'W-8EXP'
    W8_IMY = 'W-8IMY'

    @classmethod
    def is_W8(cls, uboType: Self) -> bool:
        return str(uboType.value).startswith("W-8")

    @classmethod
    def is_W9(cls, uboType: Self) -> bool:
        return str(uboType.value).startswith("W-9")


class Transaction(BaseModel):
    amount: confloat(strict=False, ge=0.00)
    valueDate: datetime.date
    transactionType: TransactionTypes


class UBO(BaseModel):
    parent_code: Optional[str] = None
    code: str
    uboType: UBOTypes
    fromDate: datetime.date
    toDate: datetime.date


class AccountTree:
    parent_account: Optional[str]
    allocation_rate: float  # 2dp, must add to 100.00 for all children
    children_accounts: List[Self]

    def __init__(self, account_code: Optional[str], allocation_rate: float):
        self.parent_account: Optional[str] = account_code
        self.allocation_rate: float = allocation_rate
        self.children_accounts: List[AccountTree] = []

    def insert(self, parent_code: Optional[str], child_account: Optional[str], allocation_rate: float) -> bool:
        tree_root = self.find(parent_code)
        we_need_to_add = tree_root and not tree_root.contains_child(child_account)
        if we_need_to_add:
            tree_root.children_accounts.append(AccountTree(child_account, allocation_rate))
        return we_need_to_add

    def find(self, account_code: str) -> Optional[Self]:
        if self.parent_account == account_code:
            return self
        if not self.children_accounts:
            return None

        for ca in self.children_accounts:
            account_sub_tree = ca.find(account_code)
            if account_sub_tree:
                return account_sub_tree
        return None

    def contains_child(self, child_account: str) -> bool:
        return self.children_accounts and any(ca.parent_account == child_account for ca in self.children_accounts)

    def node_count(self):
        count = 1
        child_counts = sum(ca.node_count() for ca in self.children_accounts)
        return count + child_counts

    def sum_of_immediate_child_allocations(self):
        """Either sum of child allocations or the rate on the object"""
        if self.children_accounts:
            return sum(ca.allocation_rate for ca in self.children_accounts)
        return self.allocation_rate
