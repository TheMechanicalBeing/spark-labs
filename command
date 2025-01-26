docker compose up -d --scale spark-worker=3

---------- DF API ----------
1.
docker exec -it spark-labs-spark-1 spark-submit --master spark://spark:7077 --deploy-mode client src/ex1_df_api_method.py

2.
docker exec -it spark-labs-spark-1 spark-submit --master spark://spark:7077 --deploy-mode client src/ex2_df_api_method.py

docker compose