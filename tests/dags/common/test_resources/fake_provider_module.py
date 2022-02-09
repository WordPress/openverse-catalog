"""
This is a fake provider module used in test_dag_factory.
It is used to check that the output path acquisition logic is correct.
"""
from common.storage.image import ImageStore


image_store = ImageStore()


def main(mock, value):
    mock(value)
