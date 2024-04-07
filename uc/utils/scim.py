from typing import Protocol


class SCIMFilter(Protocol):
    def build(self) -> str:
        ...
        
class Contains:
    def __init__(self, attribute: str, value: str):
        self.attribute = attribute
        self.value = value

    def build(self) -> str:
        return f"{self.attribute} co '{self.value}'"

class Equals:
    def __init__(self, attribute: str, value: str):
        self.attribute = attribute
        self.value = value

    def build(self) -> str:
        return f"{self.attribute} eq '{self.value}'"

class StartsWith:
    def __init__(self, attribute: str, value: str):
        self.attribute = attribute
        self.value = value

    def build(self) -> str:
        return f"{self.attribute} sw '{self.value}'"