# yfinance_example

## Demonstrates yfinance with several examples

**Run Airflow on server:**

  1. airflow webserver --port 8080 -D
  
  2. airflow scheduler -D
  
**Port forward to access Airflow UI locally:**

   3. ssh -N -f -L localhost:8080:localhost:8080 -i "[pem key]" [server public dns]
