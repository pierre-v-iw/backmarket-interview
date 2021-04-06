# Back Market Data Engineering Serve team Interview

## Data Pipeline Assessment

## Requirements

These scripts require bigquery and jinja2 python libraries. Execute the following command in your virtualenv :

```
pip install -r requirements.txt
```

## Usage

Credentials have to be setup in the environment (for example through the GOOGLE_APPLICATION_CREDENTIALS global variable)

Both scripts take optional arguments for project and schema ids.

```
python bm.py --project <project_id> --dataset <dataset_id>
```

```
python bm_incremental.py --project <project_id> --dataset <dataset_id>
```

## Interview answers & explanations

Please find a PDF document in the resources directory for answers on the test questions & detailed explanations on the code