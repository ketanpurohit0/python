"""Model the UBO accounts hierarchy as a Tree structure"""
import datetime
from enum import Enum
from typing import Optional, List, Any

import numpy as np
from pydantic import BaseModel, confloat, Field
from typing_extensions import Self


class TransactionTypes(Enum):
    """Enum to represent possible transaction types."""

    INT = "INT"
    DIV = "DIV"


class UBOTypes(Enum):
    """Enum to represent possible W8 or W9 form types."""

    W9 = "W-9"
    W8_BEN = "W-8BEN"
    W8_BENE = "W-8BEN-E"
    W8_ECI = "W-8ECI"
    W8_EXP = "W-8EXP"
    W8_IMY = "W-8IMY"

    @classmethod
    def is_w8(cls, ubo_type: Self) -> bool:
        """Return True if the account is of the type W-8 class"""
        return str(ubo_type.value).startswith("W-8")

    @classmethod
    def is_w9(cls, ubo_type: Self) -> bool:
        """Return true if the account is of type W-9 class"""
        return str(ubo_type.value).startswith("W-9")


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

    def is_valid_for_allocation(self, value_date: datetime.date) -> bool:
        """'transaction.valueDate must be within the 'self.fromDate and 'self.toDate"""
        return self.fromDate <= value_date <= self.toDate


class AccountTree:
    """Model for representing UBO tree structure"""

    parent_account: Optional[str]
    allocation_rate: float  # 2dp, must add to 100.00 for all children
    children_accounts: List[Self]

    def __init__(self, account_code: Optional[str], allocation_rate: float):
        """Construct a node for a given account"""
        self.parent_account: Optional[str] = account_code
        self.allocation_rate: float = allocation_rate  # pct
        self.allocation_amount: float = 0.0
        self.overall_allocation_rate: float = 0.0
        self.children_accounts: List[AccountTree] = []

    def insert(
        self,
        parent_code: Optional[str],
        child_account: Optional[str],
        allocation_rate: float,
    ) -> bool:
        """Insert 'child_account:' under 'parent_code:' If the 'parent_node:' does
        not exist it will be ignored. Flag returned"""
        tree_root = self.find(parent_code)
        we_need_to_add = tree_root and not tree_root.contains_child(child_account)
        if we_need_to_add:
            tree_root.children_accounts.append(
                AccountTree(child_account, allocation_rate)
            )
        return we_need_to_add

    def find(self, account_code: str) -> Optional[Self]:
        """Find the account_code in (sub)tree"""
        if self.parent_account == account_code:
            return self
        if not self.children_accounts:
            return None

        for ca_ in self.children_accounts:
            account_sub_tree = ca_.find(account_code)
            if account_sub_tree:
                return account_sub_tree
        return None

    def path_of(self, account_code: str, path: List[str] = None) -> List[str]:
        """Given an 'account_node:' discover the path leading to that node.
        If the node does not exist, the result will be an empty list, otherwise
        the list will contain one element per intermediate node."""
        if path is None:
            path = []
        if self.contains_child_arbitrary_depth(account_code):
            path.append(self.parent_account)
            if self.parent_account == account_code:
                return path
            else:
                for ca_ in self.children_accounts:
                    path = ca_.path_of(account_code, path)

        return path

    def contains_child(self, child_account: str) -> bool:
        """Check if the current node contains the 'child_account:' as one one its immediate children"""
        return self.children_accounts and any(
            ca.parent_account == child_account for ca in self.children_accounts
        )

    def contains_child_arbitrary_depth(self, child_account: str) -> bool:
        """Check if the current node contains the 'child_account:' at an arbitrary depth"""
        return self.contains_child(child_account) or any(
            ca.contains_child_arbitrary_depth(child_account)
            for ca in self.children_accounts
        )

    def node_count(self):
        """Count the total number of nodes below the current node"""
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

    def allocate_amount(self, amount: float, ndp: int = 3) -> Self:
        """Allocate 'amount to node, and also distribute to children according to node allocation rate"""
        self.allocation_amount = amount
        if self.children_accounts:
            _ = [
                ca.allocate_amount(
                    round(ca.allocation_rate * self.allocation_amount / 100, ndp)
                )
                for ca in self.children_accounts
            ]
        return self

    def increment_amount(self, amount: float, ndp: int = 3) -> Self:
        self.allocation_amount += amount
        if self.children_accounts:
            _ = [
                ca.allocate_amount(
                    round(ca.allocation_rate * self.allocation_amount / 100, ndp)
                )
                for ca in self.children_accounts
            ]
            return self

    def allocated_rate_calculation(
        self, parents_allocation_rate: float = 100.00
    ) -> None:
        """Chain down the allocation rate"""
        self.overall_allocation_rate = (
            self.allocation_rate * parents_allocation_rate
        ) / 100

        for ca in self.children_accounts:
            ca.allocated_rate_calculation(
                parents_allocation_rate=self.overall_allocation_rate
            )

    def verify_sum_of_child_allocations(self, reveal_node=False) -> bool:
        """Verify that each nodes allocation amount is equal to sum of its child allocation amount and all
        sub_nodes also"""

        if self.parent_account == "root":
            """The overall root is a dummy placeholder, its parent_name will be 'root'"""
            this_node = True
        else:
            this_node = np.isclose(
                self.sum_of_immediate_child_allocations_amount(),
                self.allocation_amount,
                atol=0.001,
            )

        all_sub_nodes = all(
            ca.verify_sum_of_child_allocations(reveal_node)
            for ca in self.children_accounts
        )

        overall_sub_tree = this_node and all_sub_nodes

        if not overall_sub_tree and reveal_node:
            print(
                f"Failing (verify_sum_of_child_allocations) with parent_account={self.parent_account}"
            )

        return overall_sub_tree

    def verify_sum_of_immediate_child_allocations_rate(
        self,
    ) -> bool:
        """Each node if it has children should have 100% allocated to it, that is the sum
        of allocations to children should be 100%

        If there are no child elements, then a number between 0 and 100.00 is fine."""

        if self.children_accounts:
            # print("**P1", self.parent_account, np.isclose(self.sum_of_immediate_child_allocations_rate(), 100.00) )
            return np.isclose(self.sum_of_immediate_child_allocations_rate(), 100.00)
        # print("**P2")
        return 0.00 <= self.allocation_rate <= 100.00

    def verify_sum_of_all_child_allocation_rates(self, reveal_node=False) -> bool:
        """We ensure that the current node and all sub nodes satisfy condition"""
        if self.parent_account == "root":
            """The overall root is a dummy placeholder, its parent_name will be 'root'"""
            this_node = True
        else:
            this_node = self.verify_sum_of_immediate_child_allocations_rate()

        all_sub_nodes = all(
            ca.verify_sum_of_immediate_child_allocations_rate()
            for ca in self.children_accounts
        )

        overall_sub_tree = this_node and all_sub_nodes
        if not overall_sub_tree and reveal_node:
            print(
                f"Failing (verify_sum_of_all_child_allocation_rates) with parent_account={self.parent_account}"
            )

        return overall_sub_tree

    def root_account(self) -> str:
        """Name of the account at the root of the tree node"""
        return self.parent_account

    def dump(self, level: int = 0) -> None:
        """Pretty print the tree for visual verification of tree structure"""
        print(
            "\t" * level,
            f"{level}>",
            self.parent_account,
            self.allocation_rate,
            self.overall_allocation_rate,
            self.allocation_amount,
        )
        _ = [ca.dump(level + 1) for ca in self.children_accounts]

    def flatten(self, level: int = 0) -> List[Any]:
        r = [
            (
                level,
                self.parent_account,
                self.allocation_rate,
                self.overall_allocation_rate,
                self.allocation_amount,
            )
        ]
        for ca in self.children_accounts:
            r.extend(ca.flatten(level + 1))
        return r
