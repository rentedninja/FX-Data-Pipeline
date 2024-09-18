import mysql.connector
import requests
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time

# MySQL connection parameters
db_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'pass',
    'database': 'prac'
}

# API key and currency pairs file path
api_key = 'OFTY5ICRY07EGVVG'
currency_pairs_file = r'C:\Users\allan\Videos\360T\currency_pairs.csv'

def create_exchange_rates_table(table_name):
    """
    Creates the `exchange_rates1` table if it does not exist.
    """
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `event_id` INT NOT NULL AUTO_INCREMENT,
            `event_time` BIGINT NOT NULL,
            `ccy_couple` VARCHAR(10) NOT NULL,
            `rate` DECIMAL(10,6) NOT NULL,
            `date` DATETIME NOT NULL,
            `timestamp` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`event_id`),
            UNIQUE KEY `unique_ccy_couple_date` (`ccy_couple`, `date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table `{table_name}` checked/created successfully.")
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()


def fetch_exchange_rates(base_currency, target_currency):
    """
    Fetch exchange rates for the given currency pair from the Alpha Vantage API.
    """
    api_url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={base_currency}&to_symbol={target_currency}&apikey={api_key}'
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()


def process_currency_pair(base_currency, target_currency):
    """
    Process a currency pair to fetch exchange rates and format data.
    """
    rates_data = fetch_exchange_rates(base_currency, target_currency)
    time_series = rates_data.get('Time Series FX (Daily)', {})
    return [{
        "ccy_couple": f"{base_currency}/{target_currency}",
        "rate": float(rates['4. close']),
        "event_time": int(datetime.strptime(date, '%Y-%m-%d').timestamp() * 1000),
        "date": datetime.strptime(date, '%Y-%m-%d'),
        "timestamp": datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
    } for date, rates in time_series.items()]


def fetch_all_data(currency_pairs, max_workers=10):
    """
    Fetch and process exchange rates for all currency pairs using parallel execution.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pair = {executor.submit(process_currency_pair, base_currency, target_currency): (base_currency, target_currency) for base_currency, target_currency in currency_pairs}
        data = []
        for future in as_completed(future_to_pair):
            try:
                data.extend(future.result())
            except Exception as exc:
                print(f'Error fetching data for {future_to_pair[future]}: {exc}')
    return data

def insert_data(processed_data, table_name):
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        
        # Check if connection is successful
        print("Database connection successful.")
        
        # Use a buffer to batch insert
        batch_size = 1000
        insert_query = f"""
        INSERT INTO {table_name} (ccy_couple, rate, event_time, date, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            rate = VALUES(rate),
            event_time = VALUES(event_time),
            timestamp = CURRENT_TIMESTAMP
        """

        # Split data into chunks for batch insertion
        for i in range(0, len(processed_data), batch_size):
            chunk = [(d['ccy_couple'], d['rate'], d['event_time'], d['date'], d['timestamp']) for d in processed_data[i:i + batch_size]]
            cursor.executemany(insert_query, chunk)
            conn.commit()
        
        print(f"{cursor.rowcount} rows inserted/updated.")
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()


def analyze_data(table_name):
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()

        # Calculate yesterday's date and time in New York Time
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)
        yesterday = now - timedelta(days=1)
        ny_5pm_yesterday = ny_tz.localize(datetime(yesterday.year, yesterday.month, yesterday.day, 17, 0, 0))
        utc_5pm_yesterday = ny_5pm_yesterday.astimezone(pytz.utc)

        # Convert to the format needed for querying
        utc_5pm_yesterday_str = utc_5pm_yesterday.strftime('%Y-%m-%d %H:%M:%S')

        # Define the SQL query for analysis
        set_query = f"SET @utc_5pm_yesterday = '{utc_5pm_yesterday_str}';"
        cursor.execute(set_query)
        
        analysis_query = f"""
        -- Get the recent exchange rates from the last 30 seconds
 WITH ActiveRates AS (
    SELECT 
        ccy_couple,
        rate,
        event_time AS event_date_time,
        timestamp AS ingestion_time
    FROM {table_name}
    WHERE timestamp >= (SELECT MAX(timestamp) FROM {table_name}) - INTERVAL 30 SECOND
),

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

PreviousRates AS (
    SELECT
        e1.ccy_couple,
        e1.rate AS previous_rate,
        e1.event_time AS previous_event_date_time
    FROM {table_name} e1
    JOIN (
        SELECT
            ccy_couple,
            MAX(event_time) AS max_event_date_time
        FROM {table_name}
        WHERE date <= @utc_5pm_yesterday
        GROUP BY ccy_couple
    ) AS lr
    ON e1.ccy_couple = lr.ccy_couple
    WHERE e1.event_time < lr.max_event_date_time
    AND e1.event_time = (
        SELECT MAX(e2.event_time)
        FROM {table_name} e2
        WHERE e2.ccy_couple = e1.ccy_couple
        AND e2.event_time < lr.max_event_date_time
    )
)

-- Combine latest and previous rates
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
FROM LatestRates lr
inner JOIN PreviousRates pr
ON lr.ccy_couple = pr.ccy_couple;
        """
        
        cursor.execute(analysis_query)
        results = cursor.fetchall()

        # Print the results in a readable format
        print(f"{'Currency Pair':<15} {'Current Rate':<15} {'% Change':<15}")
        print("="*60)
        for row in results:
            # Unpack the row but ignore the previous_rate
            ccy_couple, current_rate, previous_rate, percentage_change = row
            print(f"{ccy_couple:<15} {current_rate:<15.6f}  {percentage_change:<15}")

        print("Results displayed successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        cursor.close()
        conn.close()


def main():
    """
    Main function to execute the entire flow.
    """
    table_name = 'exchange_rates1'  # Define the table name here

    # Start time
    start_time = time.time()  # Record start time
    
    create_exchange_rates_table(table_name)

    currency_pairs = pd.read_csv(currency_pairs_file).values.tolist()
    
    processed_data = fetch_all_data(currency_pairs)
    insert_data(processed_data, table_name)
    analyze_data(table_name)
    
    # End time
    end_time = time.time()  # Record end time
    
    # Calculate the duration and print
    execution_time = end_time - start_time
    print(f"Script executed in {execution_time:.2f} seconds.")


if __name__ == "__main__":
    main()
