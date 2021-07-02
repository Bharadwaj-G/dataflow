import apache_beam as beam
import argparse
import logging
import csv, os
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "sakey.json"

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/trainingproject-317506/subscriptions/gcp-training-topic-sub'
                        )
    parser.add_argument('--inputBucket',
                        dest='inputBucket',
                        help='File to read',
                        default='gs://gcp-training-gcs-bucket/demo_data.txt'
                        )
    parser.add_argument('--output_table',
                        dest='output_table',
                        help='--output_table_name',
                        default='gcp_dataflow_test.pubsub_bq_gcs_test'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        p1 = p | "Read from Bucket">>beam.io.ReadFromText(known_args.inputBucket)
        p2 = p | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription)
        merged = (p1, p2) | "Merge" >> beam.Flatten() 
        
        pubsub_data = (
                    merged
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    | 'Split Row' >> beam.Map(lambda row : row.split(','))
                    | 'Filter By department' >> beam.Filter(lambda elements : (elements[3]=="Finance"))
                    |beam.Map(lambda elements: {"id": elements[0], "name": elements[1],"department" :elements[3]})  
                    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(known_args.output_table,
                    schema=' id:STRING, name:STRING,department:STRING', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                    )
        
        p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()