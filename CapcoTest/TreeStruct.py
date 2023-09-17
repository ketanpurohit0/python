from typing import Optional, List

from typing_extensions import Self


class AccountTree:
    parent_account: Optional[str]
    allocation_rate: float   # 2dp, must add to 100.00 for all children
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
