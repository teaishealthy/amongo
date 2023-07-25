# amongo

A natively asynchronous MongoDB driver for Python.

As always, this is a work in progress. The API and name are not stable and will change.
Name suggestions are welcome 😊

## Installation

```bash
pip install git+github.com/teaishealthy/amongo.git
```

## Usage

```python
import asyncio
from amongo import Connection

async def main():
    conn = Connection('mongodb://localhost:27017')
    db = conn['test']
    collection = db['test']
    await collection.insert_one({'test': 'test'})
```