# GCPMetrics
## Script for creating TimeSeries data GCP custom metrics  

- Uses configurable JSON input to control the rate and duration for timeseries creation
- Simple seup and can run on laptop/linux system in Docker containers
- Written in Python with Redis and arq backend
- Can create 1 million (or more) metric time series points per minute


### Basic requirements
You need to have Monitoring API access enabled in your GCP project and have a VM created under 'Compute Engine'

### JSON input

**To use the utility you need to create ans save a json file similar to one below**

```json
{
   "delay_between_iteration_in_seconds":15,
   "instances":[
      {
         "custom-descriptor_count":2,
         "data_point_count_per_custom_descriptor":4,
         "instance_id":"288815808002646xxxx",
         "project_id":"testxxxx",
         "zone":"us-central1-f"
      }
   ],
   "total_iteration_count":8,
   "wait_time_after_custom_descriptors_creations":5
}
```

A sample  json for creating 1 milition metric data points per minute as below
```json
{
    "delay_between_iteration_in_seconds": 15,
    "instances": [
        {
            "custom-descriptor_count": 1250,
            "data_point_count_per_custom_descriptor": 200,
            "instance_id": "288815808002646xxxx",
            "project_id": "xxxxxx",
            "zone": "us-central1-f"
        }
    ],
    "total_iteration_count": 8,
    "wait_time_after_custom_descriptors_creations": 60
}
```
1250 X 200 =  2,50,000 data points  every 15 seconds ( 1 milion per minute) with 8 such iterations for 15 X 8 =120 seconds (2 minutes)

### Setup

#### Input files
1. The JSON file created as above
2. GCP service account file in JSON format

#### Setup the ARQ worker with Redis backend (in one terminal)
```shell
export GOOGLE_APPLICATION_CREDENTIALS=< GCP service account file path >

docker run -d --network=host --hostname redis --name redis redis

docker build -t arq_worker  -f ArqDockerfile .

docker run -it --rm --name arq_worker  --network=host -e "REDIS_HOST=redis"  -e "GOOGLE_APPLICATION_CREDENTIALS=service_account.json" -v "${GOOGLE_APPLICATION_CREDENTIALS}":/service_account.json  arq_worker 
```

#### Run the python script (in another terminal)
```shell
export GOOGLE_APPLICATION_CREDENTIALS=< GCP service account file path >

export INPUT=< JSON input file path >

docker build -t vm_metric  -f ScriptDockerfile .

docker run  -it --rm --name vm_metric --network=host -e "GOOGLE_APPLICATION_CREDENTIALS=service_account.json" -v  "${GOOGLE_APPLICATION_CREDENTIALS}":/service_account.json -v "${INPUT}":/input.json vm_metric input.json
```
Open [GCP Cloud Console ](https://console.cloud.google.com/ "GCP Cloud Console ") and check for your project under Monitoring ->Metric Explorer if time series data have been generated for given duration and the VM.
