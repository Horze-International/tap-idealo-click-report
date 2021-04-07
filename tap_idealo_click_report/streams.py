from datetime import datetime, timedelta
import inspect
import csv
from datetime import datetime
import hashlib
import singer
import json
import re
from singer import metrics, Transformer, metadata, utils

LOGGER = singer.get_logger()

def break_into_intervals(days, start_time: str, now: datetime):
    delta = timedelta(days=days)
    # conver to datetime + add 1 millisecond so that we only get new records
    start_dt = utils.strptime_to_utc(start_time) \
               + timedelta(milliseconds=1)
    while start_dt < now:
        end_dt = min(start_dt + delta, now)
        yield start_dt, end_dt
        start_dt = end_dt


class Stream:
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields"""
    tap_stream_id = None
    pk_fields = None
    replication_method = None
    replication_keys = None

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def format_response(self, response):
        return [response] if not isinstance(response, list) else response

    def write_page(self, ctx, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        stream = ctx.catalog.get_stream(self.tap_stream_id)
        with Transformer() as transformer:
            for rec in page:
                singer.write_record(
                    self.tap_stream_id,
                    transformer.transform(
                        rec, stream.schema.to_dict(), metadata.to_map(stream.metadata),
                    )
                )
        self.metrics(page)

class ClickReports(Stream):
    tap_stream_id = 'click_report'
    replication_method = 'INCREMENTAL'
    pk_fields = ["SK"]

    def sync(self, ctx):
        #FIXME: Fix UTC handling
        last_timestamp_bookmark_key = [self.tap_stream_id, ctx.config['shop_id']]
        if ctx.bookmark(last_timestamp_bookmark_key):
            last_timestamp = utils.strptime_to_utc(ctx.bookmark(last_timestamp_bookmark_key))
        else:
            last_timestamp = utils.strptime_to_utc(ctx.config['start_date'])
        exclustion_timestamp = last_timestamp
        new_bookmark_value = last_timestamp
        start_date = last_timestamp.strftime("%Y-%m-%d")
        i = 1
        while i < 5:
            end_date = (datetime.now() - timedelta(i)).strftime("%Y-%m-%d") #yesterday
            return_value = ctx.client.download_request(start_date=start_date, end_date=end_date)
            i += 1
            if return_value != -1:
                break
        with return_value as csv_file:
            rows = []
            for row in csv.DictReader(csv_file):
                for property_name, value in row.items():
                    if value == "":
                        row[property_name] = None
                if row["Date"] == None or not re.match("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", row["Date"]):
                    continue
                timestamp = utils.strptime_to_utc(row["Date"])
                if exclustion_timestamp >= timestamp:
                    continue
                text = row["Date"] + row["Customer-IP"]  
                row["SK"] = hashlib.sha1(str.encode(text)).hexdigest()
                if timestamp > new_bookmark_value:
                    new_bookmark_value = timestamp
                rows.append(row)

        self.write_page(ctx, rows)

        ctx.set_bookmark(last_timestamp_bookmark_key, new_bookmark_value)
        ctx.write_state()

STREAM_OBJECTS = {
    cls.tap_stream_id: cls
    for cls in globals().values()
    if inspect.isclass(cls) and issubclass(cls, Stream) and cls.tap_stream_id
}
