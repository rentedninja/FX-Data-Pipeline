import mysql.connector
import requests
from datetime import datetime
import json
from decimal import Decimal

# MySQL connection parameters
db_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'pass',
    'database': 'prac'
}

# API key and currency pairs
api_key = 'OFTY5ICRY07EGVVG'
currency_pairs = [('USD', 'EUR'), ('USD', 'GBP'), ('USD', 'JPY'), ('USD', 'AUD'), ('USD', 'CAD')]

# File path for the results file
#results_file_path = r'C:\Users\allan\Videos\New folder\update_exchange_rates_results.json'



def create_exchange_rates_table():
    """
    Creates the `exchange_rates1` table if it does not exist.
    """
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS `exchange_rates` (
            `event_id` INT NOT NULL AUTO_INCREMENT,
            `event_time` BIGINT NOT NULL,
            `ccy_couple` VARCHAR(10) NOT NULL,
            `rate` DECIMAL(10,6) NOT NULL,
            `date` DATETIME NOT NULL,
            `timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`event_id`),
            UNIQUE KEY `unique_ccy_couple_date` (`ccy_couple`, `date`)
        ) ENGINE=InnoDB AUTO_INCREMENT=6061 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        print("Table `exchange_rates` checked/created successfully.")
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()

def fetch_exchange_rates(base_currency, target_currency):
    api_url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={base_currency}&to_symbol={target_currency}&apikey={api_key}'
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def process_data():
    processed_data = []
    for base_currency, target_currency in currency_pairs:
        rates_data = fetch_exchange_rates(base_currency, target_currency)
        time_series = rates_data.get('Time Series FX (Daily)', {})
        for date, rates in time_series.items():
            event_time = datetime.strptime(date, '%Y-%m-%d')
            # Ensure `event_time` is a bigint timestamp in milliseconds
            event_time_ms = int(event_time.timestamp() * 1000)
            processed_data.append((
                f"{base_currency}{target_currency}",  # ccy_couple
                float(rates['4. close']),  # rate
                event_time_ms,  # event_time (bigint)
                event_time  # date (datetime)
            ))
    return processed_data

def insert_data(processed_data):
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        
        # Check if connection is successful
        print("Database connection successful.")
        
        insert_query = """
        INSERT INTO exchange_rates (ccy_couple, rate, event_time, date)
        VALUES (%s, %s, %s, %s)
        """

        # Print processed data to debug
        #print("Processed data:", processed_data)
        
        cursor.executemany(insert_query, processed_data)
        conn.commit()
        
        print(f"{cursor.rowcount} rows inserted.")
    
    except mysql.connector.Error as err:
        if err.errno == 1062:
            print()
        else:
            print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()

def analyze_data():
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        
        # Define the SQL query for analysis
        sql_query = """
        -- Define the current timestamp at 17:00:00 for New York time
        WITH CurrentTimestamp AS (
            SELECT 
                -- Convert current UTC date and time to New York time (UTC-4)
                DATE_SUB(CONCAT(CURDATE(), ' 17:00:00'), INTERVAL 4 HOUR) AS ny_end_of_day
        ),

        -- Get the recent exchange rates from the last 30 seconds
        ActiveRates AS (
            SELECT 
                ccy_couple,
                rate,
                event_time AS event_date_time,
                timestamp AS ingestion_time
            FROM exchange_rates
            WHERE timestamp >= (SELECT MAX(timestamp) FROM exchange_rates) - INTERVAL 30 SECOND
        ),

        -- Identify the latest rate for each currency pair
        LatestRates AS (
            SELECT
                ccy_couple,
                rate AS current_rate,
                event_date_time AS current_event_date_time
            FROM (
                SELECT
                    ccy_couple,
                    rate,
                    event_date_time,
                    ROW_NUMBER() OVER (PARTITION BY ccy_couple ORDER BY event_date_time DESC) AS rn
                FROM ActiveRates
            ) AS ranked
            WHERE rn = 1
        ),

        -- Get the previous rate before the current end of day timestamp
        PreviousRates AS (
            SELECT
                e1.ccy_couple,
                e1.rate AS previous_rate,
                e1.event_time AS previous_event_date_time
            FROM exchange_rates e1
            JOIN (
                SELECT
                    ccy_couple,
                    MAX(event_time) AS max_event_date_time
                FROM exchange_rates
                WHERE date <= (SELECT ny_end_of_day FROM CurrentTimestamp)
                GROUP BY ccy_couple
            ) AS lr
            ON e1.ccy_couple = lr.ccy_couple
            WHERE e1.event_time < lr.max_event_date_time
            AND e1.event_time = (
                SELECT MAX(e2.event_time)
                FROM exchange_rates e2
                WHERE e2.ccy_couple = e1.ccy_couple
                AND e2.event_time < lr.max_event_date_time
            )
        ),

        -- Deduplicate the latest rates
        DeduplicatedLatestRates AS (
            SELECT
                ccy_couple,
                MAX(current_rate) AS current_rate,
                MAX(current_event_date_time) AS current_event_date_time
            FROM LatestRates
            GROUP BY ccy_couple
        ),

        -- Deduplicate the previous rates
        DeduplicatedPreviousRates AS (
            SELECT
                ccy_couple,
                MAX(previous_rate) AS previous_rate,
                MAX(previous_event_date_time) AS previous_event_date_time
            FROM PreviousRates
            GROUP BY ccy_couple
        )

        -- Calculate percentage change and combine latest and previous rates
        SELECT
            lr.ccy_couple,
            lr.current_rate,
            pr.previous_rate,
            CASE
                WHEN pr.previous_rate IS NULL THEN 'No Previous Rate'
                ELSE CONCAT(
                    ROUND(
                        ((lr.current_rate - pr.previous_rate) / pr.previous_rate) * 100,
                        2
                    ),
                    '%'
                )
            END AS percentage_change
        FROM DeduplicatedLatestRates lr
        LEFT JOIN DeduplicatedPreviousRates pr
        ON lr.ccy_couple = pr.ccy_couple;
        """

##execute the query
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # Print the results in a readable format
        print(f"{'Currency Pair':<15} {'Current Rate':<15} {'% Change':<15}")
        print("="*60)
        for row in results:
            ccy_couple, current_rate, previous_rate, percentage_change = row
            print(f"{ccy_couple:<15} {current_rate:<15.6f}  {percentage_change:<15}")

        print("Results displayed successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    # Fetch, process, and insert data
    create_exchange_rates_table()
    data = process_data()
    insert_data(data)
    
    # Analyze data and save results
    analyze_data()

if __name__ == "__main__":
    main()
