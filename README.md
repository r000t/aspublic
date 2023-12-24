# as:Public - A fediverse search engine
as:Public is a self-hosted search engine for Mastodon and other social media software.
It is split into separate parts that can be run on different machines, or by different people.
This means you only need the resources for the parts you want to run. 

The **Collector** sits on the public Streaming API endpoints for one or more Mastodon instances. 
The server then sends public statuses to the Collector.
Statuses can be written to an sqlite3 database on-disk, or they can be forwarded to a Recorder.

The **Recorder** receives statuses from one or more Collectors, and writes them to a central sqlite3 or PostgreSQL database. 

The **Viewer** connects to a sqllite3 or PostgreSQL database and provides an API to search for statuses. 
A browser-based frontend for this API is also provided. 


## How to use it


### Collector
Create a virtual environment and install dependencies: 
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Mastodon.py is only needed for the Collector
git clone https://github.com/halcy/Mastodon.py
mv Mastodon.py/mastodon .
rm -rf Mastodon.py
```

Run against a single instance:
`python collector.py --server example.tld`

Run against many instances:
`python collector.py --list example.list`
Where `example.list` is the path to a list of instance, with one on each line. You could also provide a .csv file, as long as the first field is the domain.

Don't import statuses that came from an instance:
`--exclude example.tld`

Don't import statuses from a list of instances:
`--exclude-list example.list`

Don't import statuses with a given phrase or regex:
`--exclude-regex example --exclude-regex ^example`

Don't import statuses with any phrases or regex from a list:
`--exclude-regex-list example.list`


### Viewer
Copy the example config file, and edit it to match the database backend and path to be used in your setup. 
`cp viewer.example.toml viewer.toml`

Run the application:
`uvicorn viewer:app`

You will be able to access the app at http://127.0.0.1:8000/.

For local testing, change `mountLocalDirectory` to `True` in viewer.py

If you wish to host the viewer publicly, you should put stronk Russian httpd in front of it. This config snippet could forward to a Viewer on the same machine, and have nginx handle serving static files for you:

```
root /path/to/viewer-static;
index index.html;
location / {  
    try_files $uri $uri/ =404;  
}
location ~ (/api|/docs|/openapi.json) {  
    proxy_pass http://127.0.0.1:8000;  
} 
```

### Recorder
Copy the example config file, and edit it to match the database backend and path to be used in your setup. 
`cp recorder.example.toml recorder.toml`

Run the application:
`uvicorn recorder:app`

Run a Collector, sending statuses to this Recorder:
`python collector.py run --recorder http://127.0.0.1:8000`