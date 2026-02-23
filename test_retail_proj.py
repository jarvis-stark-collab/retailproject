import pytest


#@pytest.fixture
#def spark():
#	return get_spark_session("LOCAL")

from lib.Utils import get_spark_session
from lib.DataReader import  read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state,filter_orders_generic
from lib.ConfigReader import get_app_config

#@pytest.mark.skip()
def test_read_customers(spark):
    #spark = get_spark_session("LOCAL")
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435
#@pytest.mark.skip()
def test_read_orders(spark):
    #spark = get_spark_session("LOCAL")
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884
#@pytest.mark.skip()
def test_closed_orders(spark):
    #spark = get_spark_session("LOCAL")
    orders_df= read_orders(spark,"LOCAL")
    filter_count= filter_closed_orders(orders_df).count()
    assert filter_count == 7556
#@pytest.mark.skip()
def test_read_app_config():
	config = get_app_config("LOCAL")
	assert config["orders.file.path"] == "data/orders.csv"
@pytest.mark.skip("work in progress")
def test_count_orders_status(spark, expected_results):
	customer_df = read_customers(spark,"LOCAL")
	actual_results = count_orders_state(customer_df)
	assert actual_results.collect() == expected_results.collect()
      
#@pytest.mark.skip()
def test_check_closed_count(spark):
    #spark = get_spark_session("LOCAL")
    orders_df= read_orders(spark,"LOCAL")
    filtered_count= filter_orders_generic(orders_df,"CLOSED").count()
    assert filtered_count == 7556

          
#@pytest.mark.skip()
def test_check_pendingpayment_count(spark):
    #spark = get_spark_session("LOCAL")
    orders_df= read_orders(spark,"LOCAL")
    filtered_count= filter_orders_generic(orders_df,"PENDING_PAYMENT").count()
    assert filtered_count == 15030

#@pytest.mark.skip()
def test_check_complete_count(spark):
    #spark = get_spark_session("LOCAL")
    orders_df= read_orders(spark,"LOCAL")
    filtered_count= filter_orders_generic(orders_df,"COMPLETE").count()
    assert filtered_count == 22900

@pytest.mark.parametrize("status,count", [("CLOSED",7556),("PENDING_PAYMENT",15030),("COMPLETE",22900)])
def test_check_count(spark,status,count):
    #spark = get_spark_session("LOCAL")
    orders_df= read_orders(spark,"LOCAL")
    filtered_count= filter_orders_generic(orders_df,status).count()
    assert filtered_count == count


