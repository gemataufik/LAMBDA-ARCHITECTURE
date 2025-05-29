import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import logging


INPUT_SUBSCRIPTION = "projects/purwadika/subscriptions/capstone3_subscriptions_gema"
OUTPUT_TABLE = "purwadika:jdeol003_capstone3_gema.final_streaming_data"

beam_options_dict = {
    'project': 'purwadika',
    'runner': 'DataflowRunner',  
    'region': 'us-central1',
    'temp_location': 'gs://gema-dataflow-bucket/temp',
    'job_name': 'streaming-taxi-data-gema',
    'streaming': True,
    'service_account_email': 'jdeol-03@purwadika.iam.gserviceaccount.com'
}

beam_options = PipelineOptions.from_dictionary(beam_options_dict)
beam_options.view_as(StandardOptions).streaming = True


class ParseAndTransform(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        logging.info(f"Processing record: {record}")

        trip_start = record.get('trip_start_timestamp')
        trip_day = trip_start.split("T")[0] if trip_start else None

        driver_name = record.get('driver_name', '').strip().title()
        payment_type = record.get('payment_type')

        yield {
            'driver_id': record.get('driver_id'),
            'driver_name': driver_name,
            'trip_day': trip_day,
            'fare_amount': record.get('fare_amount'),
            'payment_type': payment_type,
            'trip_event_timestamp': trip_start,
        }


def run():
    with beam.Pipeline(options=beam_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
            | "ParseAndTransform" >> beam.ParDo(ParseAndTransform())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema={
                    'fields': [
                        {'name': 'driver_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'driver_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'trip_day', 'type': 'DATE', 'mode': 'NULLABLE'},
                        {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'payment_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'trip_event_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
