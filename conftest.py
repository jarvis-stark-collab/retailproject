import pytest  # <--- THIS IS THE MISSING PIECE!
from lib.Utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    "create spark session"
    # This creates the session once for the whole test run
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    # After all tests are done, we shut it down
    spark_session.stop()
@pytest.fixture(scope="session")
def expected_results(spark):
    "gives the expected results"
    results_schema = "state string,count int"
    return spark.read \
        .format("csv")\
        .schema(results_schema)\
        .load("data/test_results/state_aggregate.csv")