import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from beam_nuggets.io import relational_db

class filterData(beam.DoFn):
    def process(self, data):
        # if any key is null remove filter data out 
        if None in data.values():
            return
        # Convert pressure from kPa to psi
        data['pressure'] = data['pressure'] / 6.895

        # Convert temperature from Celsius to Fahrenheit
        data['temperature'] = data['temperature'] * 1.8 + 32
        
        return [data]


# Database configuration for MySQL 
src_config = relational_db.SourceConfiguration(
    drivername='mysql+pymysql',
    host='35.234.254.66',
    port=3306,
    username='usr',
    password='sofe4630u',
    database='Readings'
)

# Table Configuration for Preprocessed records
table_config = relational_db.TableConfiguration(
    name='preprocessed_records',
    create_if_missing=True,
    primary_key_columns=['time']
)



def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True, help='Input topic smartMeter".')
    parser.add_argument('--output_topic', required=True, help='Output PubSub topic smartMeter-filtered.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:

        # Read from Smart Meter
        smartmeter_unfiltered = (
            p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
              | "UTF-8 Decode" >> beam.Map(lambda x: x.decode('utf-8'))
              | "toDict" >> beam.Map(lambda x: json.loads(x))
        )

        # Write to MySQL unfiltered logs
        smartmeter_unfiltered | "Write to MySQL" >> relational_db.Write(
            source_config=src_config,
            table_config=table_config
        )

        # Filter Logic write to Smart meter Filtered
        smartmeter_filtered = smartmeter_unfiltered | "Filter Data" >> beam.ParDo(filterData())
        smartmeter_filtered | "To JSON String" >> beam.Map(lambda x: json.dumps(x).encode('utf-8')) | "Write to smartmeter-filtered" >> beam.io.WriteToPubSub(topic=known_args.output_topic)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
