"""Module to make a standard to connect with different storages."""
from .aws import PumpWoodAwsS3
from .azure import PumpWoodAzureStorage
from .google import PumpWoodGoogleBucket

__all__ = [
    PumpWoodAwsS3, PumpWoodAzureStorage, PumpWoodGoogleBucket,
]
