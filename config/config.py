import os
from dotenv import load_dotenv, find_dotenv
from dataclasses import dataclass

load_dotenv(find_dotenv())


@dataclass(frozen=True)
class APIkeys:
    KeepNatureSafeAPIKey: str = os.getenv('KeepNatureSafeApiKey')
    KeepNatureSafeApiSecret: str = os.getenv('KeepNatureSafeApiSecret')
    KeepNatureSafeAccessToken: str = os.getenv('KeepNatureSafeAccessToken')
