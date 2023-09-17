from typing import Optional, List

from typing_extensions import Self


class AccountTree:

    parent_account: Optional[str]
    children_accounts: List[Self]
    def __init__(self, account_code: Optional[str]):
        self.parent_account: Optional[str] = account_code
        self.children_accounts: List[AccountTree] = []

    def insert(self, parent_code: Optional[str], child_account: Optional[str]) -> bool:
        tree_root = self.find(parent_code)
        we_need_to_add = tree_root and not tree_root.contains_child(child_account)
        if we_need_to_add:
            tree_root.children_accounts.append(AccountTree(child_account))
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
