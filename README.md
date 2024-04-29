# EAAO
Open-source release of "Everywhere All at Once: Co-Location Attacks on Public Cloud FaaS" (ASPLOS'24).
This repo contains the source code of a Google Cloud Run
service that we used for fingerprinting and
a library for interacting with the service.

You can clone the repo by running:
```bash
git clone https://github.com/zzrcxb/EAAO.git
```

## Dependences
### Python
Our scripts require `Python >= 3.9` and Python libraries that are listed in `requirements.txt`.
You can install those libraries by executing
```bash
pip3 install -r requirements.txt --user
```

### Google Cloud CLI
Please refer to this Google Cloud [documentation](https://cloud.google.com/sdk/docs/install)
on installing Google Cloud CLI. I recommend the [Docker](https://cloud.google.com/sdk/docs/downloads-docker) option.

## Code Structure
### Google Cloud Run (GCR) Service
`finp_service/` directory contains the source code of a Google Cloud Run service
that helps fingerprinting a Google Cloud physical host.

#### Deploy the Service
To deploy the service using Google Cloud CLI, you can execute a command similar to:
```bash
gcloud run deploy <Service name> --region=<GCR region> --source=<Path to service directory> --memory=<Memory requirement in MiB>Mi --cpu=<CPU requirement> --max-instances=1000 --concurrency=1 --timeout=3600 --session-affinity --allow-unauthenticated
```
Please refer to Google Cloud's [documentation](https://cloud.google.com/sdk/gcloud/reference/run/deploy) on the command and
[documentation](https://cloud.google.com/run/docs/deploying) about deploying GCR services in general.

Please note that our experiment script requires setting `concurrency=1` so that
each WebSocket connection corresponds to exactly one container.
It is also required to enable unauthenticated accesses `--allow-unauthenticated`.

The default service configuration that we used is:
```
--memory=512Mi --cpu=1 --max-instances=1000 --concurrency=1 --timeout=3600 --session-affinity --allow-unauthenticated
```

To learn more about deploying a service to Google Cloud Run via CLI,
you can execute:
```bash
gcloud run deploy --help
```

After the deployment, you will get an URL to invoke the service. Please save the URL.

### Toolkit for Invoking the Service
`toolkit/` directory contains Python helper code that helps interact with the
fingerprinting service.
These interactions include launching and connecting to individual container instances,
fingerprinting the physical host, running the `rdseed` covert channel to verify co-location,
and executing arbitrary command in the container instance.

For example, `validate.ipynb` provides an example that uses the toolkit
to launch containers and verify co-location
(please follow the comment in the first block of the Jupyter notebook and put your service URL).

## Minor Correction
Figure 6 in the paper requires a minor correction.
Please refer to [here](CORRECTION.md) for more details.
This correction does not change fingerprint results nor co-location results.
