from datetime import datetime, timedelta
import logging
import pickle
import requests

from tinydb import TinyDB

logging.basicConfig(level=logging.INFO)
logging.getLogger().addHandler(logging.FileHandler("crawler.log"))

timedelta = timedelta(days=5)
curr_datetime = datetime(year=2017, month=10, day=7)
session = requests.Session()
URL = "https://hacker-news.firebaseio.com/v0/"


def main():
  start_time = None
  session = requests.Session()
  with TinyDB("data/db.json") as db:
    items_total = len(db)
    if items_total > 0:
      item = min(db, key=lambda doc: doc["id"])
      logging.info("Continuing from item {} with date {}"
                    .format(item["id"], datetime.fromtimestamp(item["time"])))
      item_id = item["id"] - 1
    else:
      item_id = session.get("{}{}".format(URL, "maxitem.json")).json()
    while True:
      url = "{}{}/{}.json".format(URL, "item", item_id)
      response = session.get(url)
      if response.status_code != requests.codes.ok:
        logging.error("Requesting {} resulted in failure: {}"
                        .format(url, response.status_code))
      item = response.json()
      if item is None:
        logging.info("Skipping item #{}, because it is invalid"
                       .format(item_id))
        item_id -= 1
        continue
      submission_datetime = datetime.fromtimestamp(item["time"])
      if curr_datetime - submission_datetime > timedelta:
        break
      if curr_datetime > submission_datetime:
        if start_time is None:
          start_time = datetime.now()
        db.insert(item)
        items_total += 1
        if items_total % 100 == 0:
          now = datetime.now()
          elapsed = (now - start_time).seconds
          logging.info(
              "Added {} items, last item date: {} elapsed time: {} sec"
                .format(items_total, submission_datetime, elapsed))
          start_time = now
      else:
        logging.info("Skipping item #{} from the future {}"
                       .format(item_id, submission_datetime))
      item_id -= 1


if __name__ == "__main__":
  main()
