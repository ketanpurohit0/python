import datetime
from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, confloat, Field
from typing_extensions import Self


class TransactionTypes(Enum):
    """Enum to represent possible transaction types."""
    INT = 'INT'
    DIV = 'DIV'


class UBOTypes(Enum):
    """Enum to represent possible W8 or W9 form types."""
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
    """Pydantic model to represent a transaction"""
    amount: confloat(strict=False, ge=0.00)
    valueDate: datetime.date
    transactionType: TransactionTypes


class UBO(BaseModel):
    """Pydantic model to represent a UBO static"""
    parent_code: Optional[str] = None
    code: str = Field(min_length=1)
    uboType: UBOTypes
    fromDate: datetime.date
    toDate: datetime.date

    def is_valid_for_allocation(self, valueDate: datetime.date) -> bool:
        """'transaction.valueDate must be within the 'self.fromDate and 'self.toDate"""
        return self.fromDate <= valueDate <= self.toDate


class AccountTree:
    """Model for representing UBO tree structure"""
    parent_account: Optional[str]
    allocation_rate: float  # 2dp, must add to 100.00 for all children
    children_accounts: List[Self]

    def __init__(self, account_code: Optional[str], allocation_rate: float):
        self.parent_account: Optional[str] = account_code
        self.allocation_rate: float = allocation_rate  # pct
        self.allocation_amount: float = 0.0
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

    # def path_of(self, account_code:str, path: List[str] = None) -> List[str]:
    #     if path is None:
    #         path = []
    #     if self.parent_account == account_code:
    #         path.append(self.parent_account)
    #         return path
    #
    #     for ca in self.children_accounts:
    #         account_sub_tree = ca.find(account_code)
    #         if account_sub_tree:
    #             path.append(account_sub_tree.parent_code)

    # def path_of(self, account_code: str, path: List[str] = None) -> List[str]:
    #     if path is None:
    #         path = []
    #     if self.parent_account == account_code:
    #         print('found')
    #         path.append(account_code)
    #         return path
    #     if not self.children_accounts:
    #         print('no children')
    #         return path
    #
    #     path_ = path.copy()
    #     path_.append(self.parent_account)
    #     print(f'add parent {self.parent_account}')
    #     for ca in self.children_accounts:
    #         path_ = ca.path_of(account_code, path_)
    #
    #     return path_

    # def path_of(self, account_code: str, path: List[str] = None) -> List[str]:
    #     if path is None:
    #         path = []
    #     if self.contains_child(account_code):
    #         path.append(self.parent_account)
    #         path.append(account_code)
    #         return path
    #
    #     path.append(self.parent_account)
    #     path_ = path.copy()
    #     if self.children_accounts:
    #         for ca in self.children_accounts:
    #             path_ = ca.path_of(account_code, path_)
    #             if path_ == path:
    #                 #  No change
    #                 path_.pop()
    #     return path_

    def inner_path_of(self, account_code: str, path: List[str] = None) -> List[str]:
        if path is None:
            path = []
        if self.contains_child_arbitrary_depth(account_code):
            path.append(self.parent_account)
            if self.parent_account == account_code:
                return path
            else:
                for ca in self.children_accounts:
                    path = ca.inner_path_of(account_code, path)

        return path

    def path_of(self, account_code: str) -> List[str]:
        path = self.inner_path_of(account_code)
        path.append(account_code)
        return path

    def contains_child(self, child_account: str) -> bool:
        return self.children_accounts and any(ca.parent_account == child_account for ca in self.children_accounts)

    def contains_child_arbitrary_depth(self, child_account: str) -> bool:
        return self.contains_child(child_account) or any(
            ca.contains_child_arbitrary_depth(child_account) for ca in self.children_accounts)

    def node_count(self):
        count = 1
        child_counts = sum(ca.node_count() for ca in self.children_accounts)
        return count + child_counts

    def sum_of_immediate_child_allocations_rate(self):
        """Either sum of child allocations or the rate on the object"""
        if self.children_accounts:
            return sum(ca.allocation_rate for ca in self.children_accounts)
        return self.allocation_rate

    def sum_of_immediate_child_allocations_amount(self):
        """Either sum of child allocations or the rate on the object"""
        if self.children_accounts:
            return sum(ca.allocation_amount for ca in self.children_accounts)
        return self.allocation_amount

    def allocate_amount(self, amount: float) -> Self:
        """Allocate 'amount to node, and also distribute to children according to node allocation rate"""
        self.allocation_amount = amount
        if self.children_accounts:
            _ = [ca.allocate_amount(round(ca.allocation_rate * self.allocation_amount / 100, 2)) for ca in
                 self.children_accounts]
        return self

    def verify_sum_of_child_allocations(self) -> bool:
        """Verify that each nodes allocation amount is equal to sum of its child allocation amount"""
        this_node = self.sum_of_immediate_child_allocations_amount() == self.allocation_amount
        return this_node and all(ca.verify_sum_of_child_allocations() for ca in self.children_accounts)

    def root_account(self) -> str:
        return self.parent_account

    def dump(self, level: int = 0) -> None:
        print("\t" * level, f"{level}>", self.parent_account, self.allocation_rate, self.allocation_amount)
        _ = [ca.dump(level + 1) for ca in self.children_accounts]
