import datetime
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


class FormatName(beam.DoFn):
    def process(self, element):
        state = element['state']
        latitude = element['latitude']
        longitude = element['longitude']

        states = []
        if state not in states:
            states.append(state)
            if latitude != None:
                record = {'state': state, 'latitude': latitude,
                          'longitude': longitude}
                return [record]


def run():
    PROJECT_ID = 'still-primer-302701'
    BUCKET = 'gs://biocode-mile3/covid_data'
    DIR_PATH = BUCKET + '/output/' + \
        datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # executed with the Data flow runner and not direct runner
    options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name='location',
        temp_location=BUCKET + '/temp',
        region='us-central1')

    p = beam.pipeline.Pipeline(options=options)

    # in location, choose distinct states, latitude, and longitude
    sql = 'SELECT DISTINCT(state), latitude, longitude FROM datamart.Location'
    bq_source = ReadFromBigQuery(
        query=sql, use_standard_sql=True, gcs_location=BUCKET)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

    # run a pardo call on the results from the query
    out_pcoll = query_results | 'Format Name' >> beam.ParDo(FormatName())

    out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output.txt')

    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Location_Dataflow'
    schema_id = 'state:STRING,latitude:FLOAT,longitude:FLOAT'
    # write out the results to the bucket
    out_pcoll | 'Write to BQ' >> WriteToBigQuery(
        table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
