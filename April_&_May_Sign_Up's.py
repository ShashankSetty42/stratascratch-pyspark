#April & May Sign Up's
#
#"""
#You have been asked to get a list of all the sign up IDs with transaction start dates in either April or May.
#
#Since a sign up ID can be used for multiple transactions only output the unique ID.
#
#Your output should contain a list of non duplicated sign-up IDs.
#"""


#Solution
# Import your libraries
import pyspark
from pyspark.sql.functions import year, month, dayofmonth
# Start writing code
transactions = transactions.filter(month("transaction_start_date").isin([4,5])).distinct().select("transaction_id","transaction_start_date")

# To validate your solution, convert your final pySpark df to a pandas df
transactions.toPandas()


