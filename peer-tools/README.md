# waxpeers
This python script will download the list of peers from validate.eosnation.io and ping each of the peers recording the average
response time.  It then will create a list of the peers in response time order (fastest first) for you to include into your
nodeos config.ini file.  Optionally, it will edit the config.ini file by appending the new list to the end of the file.
It checks for markers from previous runs and will replace those lines if found.  Nothing else will be changed in the config file.
#### How it works
waxpeers starts up a number of ping processes - one for each peer in the eosnation peer endpoint report.  By default it sends 10 ping
packets to each and waits for up to 5 seconds for a response.  Peers that don't answer pings are not included in the output of waxpeers.
The pings are done in separate threads so that all peers can be pinged more or less at the same time.
#### Recommended options and use
It is completely your choice to enable the in-place edit of the nodeos config.ini file.  Since this script is rather new I
do recommend keeping a backup copy of your config.ini file if you choose to do this.
Most Guilds have private peering nodes that may be in the config.ini file - place these before the start/end block marker lines
and they will be left intact.

Recommended use:  waxpeers -c waxtest -n 10

```
usage: waxpeers [-h] -c CHAIN -n NPEERS [-i INCONFIG] [-o OUTCONFIG] [-p PINGS]
                [-t TIMEOUT]

optional arguments:
  -h, --help            show this help message and exit
  -c CHAIN, --chain CHAIN
                        chain to get the peers for. Currently either wax or
                        waxtest. (default: None)
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
