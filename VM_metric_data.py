import asyncio
import json
import os
import pprint
import random
import signal
import sys
import time
import uuid
from datetime import datetime

import google.cloud.monitoring_v3 as monitoring_v3
from arq import create_pool
from arq.connections import RedisSettings
from google.api import label_pb2 as ga_label
from google.api import metric_pb2 as ga_metric

alignment_type_dict = {
    'COUNT': {'per_series_align': monitoring_v3.Aggregation.Aligner.ALIGN_COUNT,
              'per_series_reducer': monitoring_v3.Aggregation.Reducer.REDUCE_COUNT
              },
    'MEAN': {'per_series_align': monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
             'per_series_reducer': monitoring_v3.Aggregation.Reducer.REDUCE_MEAN
             },
    'SUM': {'per_series_align': monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            'per_series_reducer': monitoring_v3.Aggregation.Reducer.REDUCE_SUM
            },
    'MIN': {'per_series_align': monitoring_v3.Aggregation.Aligner.ALIGN_MIN,
            'per_series_reducer': monitoring_v3.Aggregation.Reducer.REDUCE_MIN
            },
    'MAX': {'per_series_align': monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            'per_series_reducer': monitoring_v3.Aggregation.Reducer.REDUCE_MAX
            },
}


class MyGCPCustomMetricGenerator(object):
    def __init__(self, project_id, async_concurrency_limit=100):
        self.project_id = project_id
        self.client = monitoring_v3.MetricServiceAsyncClient()
        self.unique_string = str(uuid.uuid4())
        self.async_concurrency_limit = async_concurrency_limit

    async def create_metric_descriptor(self, descriptor_prefix='my_metric', label_key='my_metric_test_label',
                                       unique_string=None):
        unique_string = unique_string if unique_string else self.unique_string
        descriptor = ga_metric.MetricDescriptor()
        descriptor.type = f"custom.googleapis.com/{descriptor_prefix}_{unique_string}"
        descriptor.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
        descriptor.value_type = ga_metric.MetricDescriptor.ValueType.DOUBLE
        descriptor.description = f"{unique_string}"

        labels = ga_label.LabelDescriptor()
        labels.key = label_key
        labels.value_type = ga_label.LabelDescriptor.ValueType.STRING
        labels.description = f"{unique_string}"
        descriptor.labels.append(labels)

        descriptor = await self.client.create_metric_descriptor(
            name=f"projects/{self.project_id}", metric_descriptor=descriptor
        )
        print("Created {}.".format(descriptor.name))

    async def delete_metric_descriptor(self, metric_descriptor):
        await self.client.delete_metric_descriptor(name=metric_descriptor.name)

    async def get_metric_descriptor(self, metric_name):
        return await self.client.get_metric_descriptor(name=metric_name)

    async def list_metric_descriptors(self, descriptor_prefix=None):
        pager_list = await self.client.list_metric_descriptors(name=f"projects/{self.project_id}")
        descriptor_list = await self.process_results(pager_list)
        if descriptor_prefix is None:
            descriptor_prefix = "custom.googleapis.com/"
        else:
            descriptor_prefix = f"custom.googleapis.com/{descriptor_prefix}"
        ret_descriptor_list = list(filter(lambda k: k.type.startswith(descriptor_prefix), descriptor_list))
        return ret_descriptor_list

    async def add_n_metric_descriptors(self, descriptor_prefix='my_metric', label_key='my_metric', count=1):
        descriptor_list = await self.list_metric_descriptors(descriptor_prefix=descriptor_prefix)
        if count <= len(descriptor_list):
            print(f"{count} descriptors are already added")
        else:
            descriptor_list = await self.list_metric_descriptors()
            new_count = min((2000 - len(descriptor_list)), (count - len(descriptor_list)))
            if new_count > 0:
                print(f"{new_count} descriptors can be added")
                coros = [self.create_metric_descriptor(descriptor_prefix=descriptor_prefix, label_key=label_key,
                                                       unique_string=str(index)) for index
                         in range(new_count)]
                await gather_with_concurrency(self.async_concurrency_limit, *coros)
            else:
                print("Metric descriptor limit were exausted for the project")

    async def delete_n_metric_descriptors(self, descriptor_prefix='my_metric', count=1):
        descriptor_list = await self.list_metric_descriptors(descriptor_prefix=descriptor_prefix)
        if count < 0:
            new_count = len(descriptor_list)  # delete all
        else:
            new_count = min(len(descriptor_list), count)
        if new_count > 0:
            print(f"{new_count} descriptors can be deleted")
            coros = [self.delete_metric_descriptor(metric_descriptor=desc) for desc in descriptor_list[:new_count]]
            await gather_with_concurrency(self.async_concurrency_limit, *coros)
        else:
            print("No descriptor to be deleted for the project")

    async def list_monitored_resources(self):
        pager_list = await self.client.list_monitored_resource_descriptors(name=f"projects/{self.project_id}")
        return await self.process_results(pager_list)

    async def get_monitored_resource_descriptor(self, resource_type_name):
        resource_path = (
            f"projects/{self.project_id}/monitoredResourceDescriptors/{resource_type_name}"
        )
        resource_descriptor = await self.client.get_monitored_resource_descriptor(name=resource_path)
        pprint.pprint(resource_descriptor)
        return resource_descriptor

    async def write_time_series(self, metric_type, instance_id, zone, label, data_point_count=1, timestamp=None,
                                unique_string=None):
        unique_string = unique_string if unique_string else self.unique_string
        now = timestamp if timestamp else time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {"end_time"
             : {"seconds": seconds, "nanos": nanos}}
        )
        series_list = list()
        for i in range(data_point_count):
            point = monitoring_v3.Point(
                {"interval": interval, "value": {"double_value": random.uniform(i, 99.99 + 1)}})
            series = monitoring_v3.TimeSeries()
            series.metric.type = metric_type
            series.resource.type = "gce_instance"
            series.resource.labels["instance_id"] = instance_id
            series.resource.labels["zone"] = zone
            series.metric.labels[label] = f"label_{unique_string}_{i}"
            series.points = [point]
            series_list.append(series)

        await self.client.create_time_series(
            request={"name": f"projects/{self.project_id}", "time_series": series_list})
        print("Successfully wrote time series..")

    async def list_time_series(self, metric_type, interval_seconds=1200):
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": seconds, "nanos": nanos},
                "start_time": {"seconds": (seconds - interval_seconds), "nanos": nanos},
            }
        )
        pager_list = await self.client.list_time_series(
            request={
                "name": f"projects/{self.project_id}",
                "filter": f"metric.type = \"{metric_type}\"",
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )
        return await self.process_results(pager_list)

    async def list_time_series_header(self, metric_type, interval_seconds=1200):
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": seconds, "nanos": nanos},
                "start_time": {"seconds": (seconds - interval_seconds), "nanos": nanos},
            }
        )
        pager_list = await self.client.list_time_series(
            request={
                "name": f"projects/{self.project_id}",
                "filter": f"metric.type = \"{metric_type}\"",
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.HEADERS,
            }
        )
        return await self.process_results(pager_list)

    async def list_time_series_aggregate(self, metric_type, interval_seconds=3600, alignment_period=1200,
                                         alignment_type='COUNT'):
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": seconds, "nanos": nanos},
                "start_time": {"seconds": (seconds - interval_seconds), "nanos": nanos},
            }
        )
        aggregation = monitoring_v3.Aggregation(
            {
                "alignment_period": {"seconds": alignment_period},  # 20 minutes
                "per_series_aligner": alignment_type_dict[alignment_type]['per_series_align'],
            }
        )

        pager_list = await self.client.list_time_series(
            request={
                "name": f"projects/{self.project_id}",
                "filter": f'metric.type = \"{metric_type}\"',
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
            }
        )
        return await self.process_results(pager_list)

    async def list_time_series_reduce(self, metric_type, interval_seconds=3600, alignment_period=1200,
                                      alignment_type='COUNT'):
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": seconds, "nanos": nanos},
                "start_time": {"seconds": (seconds - interval_seconds), "nanos": nanos},
            }
        )
        aggregation = monitoring_v3.Aggregation(
            {
                "alignment_period": {"seconds": alignment_period},  # 20 minutes
                "per_series_aligner": alignment_type_dict[alignment_type]['per_series_align'],
                "cross_series_reducer": alignment_type_dict[alignment_type]['per_series_reducer'],
                "group_by_fields": ["resource.zone"],
            }
        )

        pager_list = self.client.list_time_series(
            request={
                "name": f"projects/{self.project_id}",
                "filter": f"metric.type = \"{metric_type}\"",
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
            }
        )
        return await self.process_results(pager_list)

    async def process_results(self, pager_list):
        result_list = list()
        async for result in pager_list:
            result_list.append(result)
        # for result in result_list:
        #     pprint.pprint(result)
        return result_list


class AsyncCleanup(object):
    def __init__(self, project_id_list, async_concurrency_limit):
        self.project_id_list = project_id_list
        self.async_concurrency_limit = async_concurrency_limit

    async def delete_existing_custom_descriptors(self, sleep_time_in_seconds=5):
        print("Deletng existing custom descriptors")
        for project_id in self.project_id_list:
            md = MyGCPCustomMetricGenerator(project_id, async_concurrency_limit=self.async_concurrency_limit)
            await md.delete_n_metric_descriptors(descriptor_prefix=None, count=-1)
            print(f"Sleeping for {sleep_time_in_seconds} seconds...")
            await asyncio.sleep(sleep_time_in_seconds)


class AsyncWorker(object):
    def __init__(self, project_id, instance_id, zone,
                 descriptor_prefix="my_metric",
                 label_key="my_metric",
                 descriptor_count=1,
                 data_point_count_per_descriptor=1,
                 async_concurrency_limit=100
                 ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.zone = zone
        self.descriptor_prefix = descriptor_prefix
        self.label_key = label_key
        self.descriptor_count = descriptor_count
        self.data_point_count_per_descriptor = data_point_count_per_descriptor

        self.custom_metric_genrator = MyGCPCustomMetricGenerator(project_id=self.project_id,
                                                                 async_concurrency_limit=async_concurrency_limit)
        self.metric_desciptor_list = None
        self.async_concurrency_limit = async_concurrency_limit

    async def async_add_descriptors(self, delete_pre_existing_descriptors=True):
        if delete_pre_existing_descriptors:
            await self.custom_metric_genrator.delete_n_metric_descriptors(descriptor_prefix=None, count=-1)
            await asyncio.sleep(5)
        await self.custom_metric_genrator.add_n_metric_descriptors(descriptor_prefix=self.descriptor_prefix,
                                                                   label_key=self.label_key,
                                                                   count=self.descriptor_count)
        await asyncio.sleep(5)
        self.metric_desciptor_list = await self.custom_metric_genrator.list_metric_descriptors(
            descriptor_prefix=self.descriptor_prefix)

    async def add_data_points(self, timestamp):
        metric_desciptor_list = await self.custom_metric_genrator.list_metric_descriptors(
            descriptor_prefix=self.descriptor_prefix)
        coros = [self.custom_metric_genrator.write_time_series(metric_type=desc.type, instance_id=self.instance_id,
                                                               zone=self.zone, label=self.label_key,
                                                               data_point_count=self.data_point_count_per_descriptor,
                                                               unique_string=desc.description, timestamp=timestamp) for
                 desc in metric_desciptor_list]
        await gather_with_concurrency(self.async_concurrency_limit, *coros)

    def get_added_descriptors_count(self):
        return len(self.metric_desciptor_list) if self.metric_desciptor_list else 0

    def get_added_descriptors_count(self):
        return self.metric_desciptor_list


async def gather_with_concurrency(n, *tasks):  # to avoid rate limit exception
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def cleanup(json_input, sleep_time_in_seconds=5, async_concurrency_limit=100):
    project_ids = list()

    for instance in json_input['instances']:
        project_ids.append(instance['project_id'])
    project_ids = list(set(project_ids))

    cleanup = AsyncCleanup(project_ids, async_concurrency_limit=async_concurrency_limit)
    await cleanup.delete_existing_custom_descriptors(sleep_time_in_seconds=sleep_time_in_seconds)


async def add_descriptors(json_input, sleep_time_in_seconds=5, async_concurrency_limit=100):
    for instance in json_input['instances']:
        worker = AsyncWorker(project_id=instance['project_id'],
                             instance_id=instance['instance_id'],
                             zone=instance['zone'],
                             descriptor_count=instance['custom-descriptor_count'],
                             data_point_count_per_descriptor=instance['data_point_count_per_custom_descriptor'],
                             async_concurrency_limit=async_concurrency_limit
                             )
        await worker.async_add_descriptors(delete_pre_existing_descriptors=False)
        print(f"Sleeping for {sleep_time_in_seconds} seconds...")
        await asyncio.sleep(sleep_time_in_seconds)


# function to add points to arq
async def add_data_points(ctx, project_id, instance_id, zone, data_point_count_per_descriptor, timestamp,
                          async_concurrency_limit=100):
    worker = AsyncWorker(project_id=project_id,
                         instance_id=instance_id,
                         zone=zone,
                         data_point_count_per_descriptor=data_point_count_per_descriptor,
                         async_concurrency_limit=async_concurrency_limit
                         )
    await worker.add_data_points(timestamp=timestamp)


# enque tasks in arq
async def add_data_points_for_all(json_input):
    redis = await create_pool(RedisSettings(host=os.environ.get("REDIS_HOST", "localhost"),
                                            port=int(os.environ.get("REDIS_PORT", "6379"))))
    sequence_number = json_input['total_iteration_count']
    while sequence_number > 0:
        time_stamp = time.time()
        coros = []
        for instance in json_input['instances']:
            coros.append(
                redis.enqueue_job('add_data_points', instance['project_id'], instance['instance_id'], instance['zone'],
                                  instance['data_point_count_per_custom_descriptor'], time_stamp))
        print(f"Sleeping for {json_input['delay_between_iteration_in_seconds']} seconds...")
        coros.append(asyncio.sleep(json_input['delay_between_iteration_in_seconds']))
        await asyncio.gather(*coros)
        sequence_number -= 1

    redis.close()
    await redis.wait_closed()


# WorkerSettings defines the settings to use when creating the work,
# it's used by the arq cli
class WorkerSettings:
    functions = [add_data_points]


# signal handler for asyncio loop
def handler(loop):
    print('Stopping..')
    for task in asyncio.all_tasks(loop):
        task.cancel()


if __name__ == "__main__":
    with open(sys.argv[1], 'r') if len(sys.argv) > 1 else sys.stdin as f:
        try:
            json_input = json.load(f)
        except Exception as e:
            print("Invalid json input provided.")
            raise

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.add_signal_handler(signal.SIGINT, handler, loop)

    try:
        loop.run_until_complete(
            cleanup(json_input, sleep_time_in_seconds=json_input['wait_time_after_custom_descriptors_creations']))
        loop.run_until_complete(add_descriptors(json_input, sleep_time_in_seconds=json_input[
            'wait_time_after_custom_descriptors_creations']))
        loop.run_until_complete(asyncio.sleep(60 - datetime.utcnow().second))
        loop.run_until_complete(add_data_points_for_all(json_input))
    except Exception as e:
        print("Cancelling the pending jobs.")
        print(e)
        handler(loop)
    finally:
        loop.close()
