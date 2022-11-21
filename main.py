from connection import wrapper
from apscheduler.schedulers.background import BlockingScheduler
scheduler = BlockingScheduler()


def main():
    scheduler.add_job(id='Scheduled task', func=wrapper(), day_of_week='mon-sun', hour=1)
    scheduler.start()
    # wrapper()


if __name__ == '__main__':
    main()
