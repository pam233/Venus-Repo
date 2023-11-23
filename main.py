import schedule
import time
import prehook
import hook
import posthook


def etl():
    print("Running ETL process...")
    
    prehook.execute()
    hook.execute()
    posthook.execute()
    
    print("ETL process completed.")

# Set up the schedule (e.g., run the ETL process every day at midnight)
schedule.every().day.at("00:00").do(etl)

# Run the scheduler loop
while True:
    schedule.run_pending()
    time.sleep(1)
