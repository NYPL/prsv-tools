from dataclasses import dataclass


@dataclass
class Structural_Object:
    uuid: str
    title: str
    securityTag: str
    soCategory: str
    mdFragments: dict | None
    parent: str
    children: dict | None


@dataclass
class Information_Object:
    uuid: str
    title: str
    securityTag: str
    ioCategory: str
    mdFragments: dict | None
    parent: str
