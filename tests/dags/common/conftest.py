import pytest


@pytest.fixture
def identifier(request):
    return f"{hash(request.node.name)}".replace("-", "_")


@pytest.fixture
def image_table(identifier):
    # Parallelized tests need to use distinct database tables
    return f"image_{identifier}"
