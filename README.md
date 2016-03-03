# monutils
Monitoring utilities written in python 3.5 using asyncio and bokeh

There are 2 interfaces available

* Standalone aiohttp server that proxies a bokeh server.
* Calling monutils.notebook from inside a jupyter notebook

## Standalone

This is the standard interface invoked if monutils.py is used as a cli-script.
It will check if a bokeh server is aleready running on the supplied port, if not 
a new one will be started that _should_ terminate with this application.

Then a aiohttp web server will be fired up in order to proxy the bokeh server session.
checkout `python3 monutils.py -h` for usage

## Notebook
The monitor can be used with a jupyter notebook like so
```python
import gpumon
import logging
gpumon.logger.setLevel(logging.INFO)
from bokeh.io import output_notebook
output_notebook()

gpumon.notebook("myuser@host1", "-p 5900 myuser@locahost", mon_local=False)
```


## Parameters
Both implementations use these parameters

### tunnels
ssh arguments used for logging into remote machines
Example:
    "-p 5900 myuser@somehost" -> log in using port 5900 with "mysuser" on "somehost"
For every ssh string a monitor column will be added to the interface

Remember to either have ssh keys for logging in prepared. atm there is no way 
to provide cleartext passwords (and frankly this might be a good thing)
Also make sure that the hosts are added to trusted hosts
### mon_local / remote-only
should the local machine be monitored?

### plots / plot_list
Sequence of strings or None

if not None, it overrides the default plots. 
Valid strings match plot method names of `monutils.BokehPlots` class