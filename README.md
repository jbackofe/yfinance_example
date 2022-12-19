# yfinance_example

Demonstrates yfinance with several examples

Run Airflow on server:

  airflow webserver --port 8080 -D
  
  airflow scheduler -D
  
Port forward to access Airflow UI locally:
   ssh -N -f -L localhost:8080:localhost:8080 -i "[pem key]" [server public dns]
