# waxpeers

This python script will download the list of peers from validate.eosnation.io and ping each of the peers recording the average response time.  It then will create a list of the peers in response time order (fastest first) for you to include into your nodeos config.ini file.  Optionally, it will edit the config.ini file by appending the new list to the end of the file.  It checks for markers from previous runs and will replace those lines if found.  Nothing else will be changed in the config file.

```
usage: waxpeers [-h] -n NPEERS [-i INCONFIG] [-o OUTCONFIG] [-p PINGS]
                [-t TIMEOUT]

optional arguments:
  -h, --help            show this help message and exit
  -n NPEERS, --npeers NPEERS
                        The number of peers to include in the new p2p-peer-
                        address list. 0 means all that answer pings. (default:
                        0)
  -i INCONFIG, --inconfig INCONFIG
                        The pathname of the nodeos config.ini file to read for
                        non- p2p-peer-address settings. (default: None)
  -o OUTCONFIG, --outconfig OUTCONFIG
                        The pathname of the nodeos config.ini file to write
                        out with p2p-peer-address information added. (default:
                        None)
  -p PINGS, --pings PINGS
                        How many pings to send to each peer for computing an
                        average (default: 10)
  -t TIMEOUT, --timeout TIMEOUT
                        The timeout in seconds to wait for replies from each
                        peer (default: 5)

NOTE: Well Known Text Line Markers are used to mark a block for this program.
They will be added to the end of the output if they do not previously exist.
The marked block will be stripped from the input when present. For proper
operation of this program please do not modify the marker lines or the lines
in between. If the input config and output config files are the same name, a
safe method of generating a new modified config file will be used. If an
output config file is not specified then stdout will be used.
```
