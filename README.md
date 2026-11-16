# freshcart-pipelines

Data Engineering assignment "From Git to Gold": version control, ELT,
an ML pipeline and an event stream, end to end.

| Folder | Contents |
|---|---|
| `elt/` | FreshCart ELT notebook: API data → Snowflake bronze → silver → gold (Part B) |
| `iris_project/` | Mage project with the `iris_classifier` pipeline and the notebook that runs it (Part C) |
| `stream/` | RabbitMQ food-order stream: `dispatcher.py` and `driver.py` (Part D) |
| `tests/` | pytest tests, run by GitHub Actions on every push (Part A) |

## Run the tests

    pip install -r requirements.txt
    pytest -q

## Run the stream (needs a RabbitMQ broker; set AMQP_URL for a remote one)

    python stream/driver.py Ravi 'bangalore.koramangala.*'
    python stream/dispatcher.py

No secrets are stored in this repository: passwords are typed at runtime.
