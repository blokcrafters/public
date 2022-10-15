#!/usr/bin/env python3

import iso3166
import argparse
import sys
import json
import requests
import subprocess
import datetime
import dateutil.parser
import mimetypes
import time
import os

# The maximum age of the producers JSON file in seconds before we attempt to update it.
PRODUCERS_FILE_MAXAGE = 3600
# The maximum age of the top21 JSON file in seconds before we attempt to update it.
TOP21_FILE_MAXAGE = 3600
# The maximum age of the producerjson-actions JSON file in seconds before we attempt to update it.
PRODUCERJSON_ACTIONS_FILE_MAXAGE = 3600
# The maximum age of the urls-last-checked file in seconds before allowing to recheck the bp.json URLs
URLSLASTCHECKED_FILE_MAXAGE = 3600

def dirExists(filename):
  return os.path.exists(filename) and os.path.isdir(filename)

def fileExists(filename):
  return os.path.exists(filename) and os.path.isfile(filename)

def fileOlderThan(filename, maxAge):
  return (not fileExists(filename)) or (int(os.path.getmtime(filename)) < (int(datetime.datetime.utcnow().timestamp()) - maxAge))

def UpdateProducers(chainURL, producersFilename, top21Filename):
  print(f"system: fetching the producers from {chainURL} ...", file=log)
  numToGet = 50
  start = ''
  inactives = {}
  producers = {}
  top21 = []

  while True:
    info = subprocess.run(['cleos', '--url', chainURL, 'system', 'listproducers', '--json',
                          '--limit', str(numToGet), '--lower', start], stdout=subprocess.PIPE)
    someProducers = json.loads(info.stdout)
    for i in someProducers['rows']:
      p = i['owner']
      if i['is_active'] == 1:
        producers[p] = i
      else:
        inactives[p] = i
      print(f"system: producer {len(producers) + len(inactives)}: {p}", file=log)
    start = someProducers['more']
    if len(start) <= 0:
      break

  print(f"system: ... done.", file=log)
  print(f"system: producers: {len(producers)} active and {len(inactives)} inactive", file=log)

  # Get the Top21 producers
  info = subprocess.run(['cleos', '--url', chainURL, 'get', 'schedule', '--json'], stdout=subprocess.PIPE)
  someJSON = json.loads(info.stdout)
  for p in someJSON['active']['producers']:
    top21.append(p['producer_name'])
  print(f"system: top21 producers: {len(top21)}", file=log)

  f = open(top21Filename, 'w')
  json.dump(top21, f, indent=2)
  f.close()
  return top21, producers

def GetProducers(chainURL, producersFilename, top21Filename, force=False):
  producers = {}
  top21 = []
  if force or fileOlderThan(producersFilename, PRODUCERS_FILE_MAXAGE) or fileOlderThan(top21Filename, TOP21_FILE_MAXAGE):
    top21, producers = UpdateProducers(chainURL, producersFilename, top21Filename)
  else:
    with open(producersFilename) as fh:
      producers = json.load(fh)
    with open(top21Filename) as fh:
      top21 = json.load(fh)
  return top21, producers

def ProducerBPJSONFilename(s, p=''):
  name = ''
  if s == 'url':
    name = "{net}-jsons/{source}/{producer}-bp.json".format(net="mainnet" if args.mainnet else "testnet", source=s, producer=p)
  elif s == 'chain':
    name = "{net}-jsons/producerjson-actions.json".format(net="mainnet" if args.mainnet else "testnet")
  return name

def URLSLastCheckedFilename():
  return "{net}-jsons/urls-last-checked".format(net="mainnet" if args.mainnet else "testnet")

def ProducerLogoFilename(p):
  return "{net}-logos/{producer}-logo_256".format(producer=p, net="mainnet" if args.mainnet else "testnet")

def ProducerBPJSONURL(p):
  url = ''
  producer = producers[p]
  if len(producer['url']):
    url = f"{producer['url']}/bp.json"
  return url

def UpdateBPJSON(historyURL, source, p, force=False):
  failReasons = []
  zeroActions = False
  bpjson = {}
  producer = producers[p]
  if source == 'url':
    url = ProducerBPJSONURL(p)
    if len(url):
      filename = ProducerBPJSONFilename(source, p)
      if force:
        print(f"{p}: Force is True - removing the producer's cached url bp.json file - {filename}", file=log)
        try:
          os.remove(filename)
        except FileNotFoundError:
          pass
      # Only check the URL for a new bp.json file when:
      #  - we do not have a url file
      #  - the age of the urls-last-checked file is too old
      if fileOlderThan(URLSLastCheckedFilename(), URLSLASTCHECKED_FILE_MAXAGE):
        print(f"{p}: updating the {source} bp.json cache file from {url} ... ", end='', file=log)
        info = subprocess.run(['curl', '-RLSsf', '--connect-timeout', '5', '-z', filename, '-o', filename, url],
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if info.returncode != 0:
          failReasons.append(f"{p}: FAIL: {url}: curl return code {info.returncode}: {info.stdout.decode('utf-8').rstrip()}")
          print(f"{p}: failed", file=log)
        else:
          print(f"{p}: success", file=log)
      if fileExists(filename):
        with open(filename) as fh:
          try:
            bpjson = json.load(fh)
          except json.decoder.JSONDecodeError:
            failReasons.append(f"{p}: {source} bp.json cache file could not be decoded as JSON")
  elif source == 'chain':
    filename = ProducerBPJSONFilename(source, p)
    theActions = {}
    # Force is handled before getting here for source == chain
    if  fileOlderThan(filename, PRODUCERJSON_ACTIONS_FILE_MAXAGE):
      print(f"{p}: updating the {source} producerjson-actions.json cache file from {historyURL} ... ", end='', file=log)
      gaURL = historyURL + "/v2/history/get_actions?act.account=producerjson&limit=1000"
      info = subprocess.run(['curl', '-LSsf', '--connect-timeout', '5',
                            '-H', 'accept: application/json', '-H', 'Content-Type: application/json', gaURL], stdout=subprocess.PIPE)
      if info.returncode != 0:
        failReasons.append(f"FAIL: {gaURL}: curl return code {info.returncode}: {info.stdout.decode('utf-8').rstrip()}")
        print(f"{p}: failed", file=log)
      else:
        with open(filename, 'wb') as fh:
          fh.write(info.stdout)
        theActions = json.loads(info.stdout)
        print(f"{p}: success", file=log)
    else:
      with open(filename) as fh:
        theActions = json.load(fh)

    # Gather all the specific producer jsons from the actions into prodJSONs
    prodJSONs = {}
    if 'actions' in theActions:
      for action in theActions['actions']:
        if p != action['act']['data']['owner']:
          continue
        # Verify the action name
        if not action['act']['name'] in ('set', 'del'):
          failReasons.append(f"{p}: + unknown action name ({action['act']['name']}) is present")
          continue
        dt = dateutil.parser.parse(action['@timestamp'])
        timestamp = time.mktime(dt.timetuple()) + dt.microsecond / 1000000
        tsJSON = {}
        tsJSON['name'] = action['act']['name']
        tsJSONData = ''
        if tsJSON['name'] == 'set':
          tsJSONData = action['act']['data']['json']
          try:
            tsJSON['data'] = json.loads(tsJSONData)
            prodJSONs[timestamp] = tsJSON
          except json.decoder.JSONDecodeError:
            failReasons.append(f"{p}: chain producerjson action data @{action['@timestamp']} could not be decoded as JSON")
        elif tsJSON['name'] == 'del':
          prodJSONs[timestamp] = tsJSON
    print(f"{p}: {len(prodJSONs)} producjerjson actions on the chain", file=log)
    if len(prodJSONs):
      for ts, action in sorted(prodJSONs.items(), reverse=True):
        if action['name'] == 'del':
          # The newest action done was a delete - they are not on the chain.
          bpjson = {}
        elif action['name'] == 'set':
          # This is the newest set action.
          bpjson = action['data']
        break
    elif len(failReasons) == 0:
      zeroActions = True

  if len(bpjson) == 0:
    if source == 'url':
      print(f"{p}: failed to retrieve url bp.json for the cache", file=log)
    elif source == 'chain':
      if len(failReasons) > 0:
        print(f"{p}: failed to retrieve chain bp.json data for the cache", file=log)
        if zeroActions:
          failReasons.append(f"There are no producerjson action entries on the chain.")
    for reason in failReasons:
      print(f"{p}: + {reason}", file=log)
  return bpjson

def GetBPJSONs(historyURL, producers, force=False):
  bpjsons = {}
  if force or fileOlderThan(URLSLastCheckedFilename(), URLSLASTCHECKED_FILE_MAXAGE):
    try:
      os.remove(URLSLastCheckedFilename())
    except FileNotFoundError:
      pass
  for source in ['chain', 'url']:
    bpjsons[source] = {}
    for p in producers:
      if source == 'url':
        bpjson = UpdateBPJSON(historyURL, source, p, force)
      elif source == 'chain':
        bpjson = UpdateBPJSON(historyURL, source, p, force)
        if not dirExists('{net}-jsons/chain'.format(net="mainnet" if args.mainnet else "testnet")):
          os.mkdir('{net}-jsons/chain'.format(net="mainnet" if args.mainnet else "testnet"))
        f = open("{net}-jsons/chain/{producer}-bp.json".format(net="mainnet" if args.mainnet else "testnet", producer=p), 'w')
        json.dump(bpjson, f, indent=2)
        f.close()
      if bpjson:
        bpjsons[source][p] = bpjson
  if source == 'url' and (not fileExists(URLSLastCheckedFilename())):
    os.mknod(URLSLastCheckedFilename())
  return bpjsons

def UpdateLogo(source, p, force):
  logo = {}
  bpjson = bpjsons[source][p]
  if 'org' in bpjson:
    if 'branding' in bpjson['org']:
      if 'logo_256' in bpjson['org']['branding']:
        url = bpjson['org']['branding']['logo_256']
        if len(url):
          ext = requests.utils.urlparse(url).path.split('.')[-1]
          filename = ProducerLogoFilename(p)
          logoFile = f"{filename}.{ext}"
          if force:
            print(f"{p}: Force is True - removing the producer's cached url logo file - {logoFile}", file=log)
            try:
              os.remove(logoFile)
            except FileNotFoundError:
              pass
            updateLogoFile = True
          else:
            updateLogoFile = False
            urlTime = ''
            try:
              fileMTime = datetime.datetime.utcfromtimestamp(os.path.getmtime(logoFile))
              r = requests.head(url, timeout=5, allow_redirects=True)
              if 'Last-Modified' in r.headers:
                urlTime = r.headers['Last-Modified']
                urlDate = dateutil.parser.parse(urlTime, ignoretz=True)
                if urlDate > fileMTime:
                  updateLogoFile = True
                  print(f"{p}: the producers logo file is newer than the cache file", file=log)
              else:
                print(f"{p}: could not retrieve a Last-Modified value for the producer logo file", file=log)
                updateLogoFile = True
            except FileNotFoundError:
              print(f"{p}: url bp.json cache file ({logoFile}) is missing", file=log)
              updateLogoFile = True
            except requests.ConnectionError as e:
              print(f"{p}: url bp.json cache file ({logoFile}): ConnectionError exception: {e}", file=log)
              updateLogoFile = True

          if updateLogoFile:
            print(f"{p}: updating logo cache file ({logoFile}) from {url}", file=log)
            try:
              r = requests.get(url, None, timeout=5)
            except:
              print(f"{p}: Unexpected error retrieving url={url}", file=log)
              return logo
            if 'Last-Modified' in r.headers:
              urlTime = r.headers['Last-Modified']
            else:
              urlTime = ''
            ct = r.headers['Content-Type']
            ext = mimetypes.guess_extension(ct)
            # I prefer to use jpg extension rather than jpe.  Which is what mimetimes normally returns.
            if ext == ".jpe":
              ext = ".jpg"
            # ext here has a dot already in it.
            logoFile = f"{filename}{ext}"
            with open(logoFile, 'wb') as fh:
              fh.write(r.content)
            if len(urlTime) > 0:
              modTime = dateutil.parser.parse(urlTime)
              os.utime(logoFile, (int(time.time()), time.mktime(modTime.timetuple())))
              # All good.
              logo = logoFile
        else:
          print(f"{p}: the org.branding.logo_256 url is empty in org.branding", file=log)
      else:
        print(f"{p}: org.branding.logo_256 is missing from the url bp.json cache", file=log)
    else:
      print(f"{p}: org.branding is missing from the url bp.json cache", file=log)
  else:
    print(f"{p}: org is missing from the url bp.json cache", file=log)

  return logo

def GetLogos(source, producers, bpjsons, force=False):
  logos = {}
  for p in producers:
    if p in bpjsons[source]:
      logo = UpdateLogo(source, p, force)
      if logo:
        logos[p] = logo
  return logos

def VerifyProducers(producers):
  for p in producers:
    producer = producers[p]
    if len(producer['url']) == 0:
      print(f"{p}: NOTE: the url field is empty.", file=log)

# Does nothing at the moment.
def VerifyBPJSONs(bpjsons):
  for p in bpjsons['url']:
    bpjson = bpjsons['url'][p]

# CHECK
def CheckConsistency(producers, bpjsons, logos):
  print(f"system: + {len(producers)} producers", file=log)
  regen = False
  for p in producers:
    producer = producers[p]
    missing = []
    display = False
    if not producer['url']:
      missing.append('url')
    if not producer['location']:
      missing.append('location')
    else:
      if not (str(producer['location']).zfill(3) in iso3166.countries_by_numeric):
        display = True
        print(f"system: + WARN: producer {p} has an invalid value {producer['location']} for a country code (see ISO3166)", file=log)
    if len(missing):
      display = True
      print(f"system: + WARN: producer {p} does not have a setting for {missing}", file=log)
    if display:
      print(f"system: + + producer = {producer}", file=log)
  if regen:
    print(f"system: + CRIT:   You need to regenerate the producer cache with the -p option", file=log)

  for source in ['url', 'chain']:
    print(f"system: + {len(bpjsons[source])} {source} bp.json cache files", file=log)
    for b in bpjsons[source]:
      regen = False
      bpjson = bpjsons[source][b]

      for node in bpjson['nodes']:
        if 'latitude' in node['location']: 
          lat = node['location']['latitude']
        else:
          print(f"system: + WARN: producer {p} does not have a latitude for location {node['location']}", file=log)
          regen = True
        if 'longitude' in node['location']: 
          lon = node['location']['longitude']
        else:
          print(f"system: + WARN: producer {p} does not have a longitude for location {node['location']}", file=log)
          regen = True
      if regen:
        print(f"system: + CRIT:   You need to regenerate {source} bp.json cache files with the -b option", file=log)

  print(f"system: + {len(bpjsons['url'])} logo cache files", file=log)
  regen = False
  for l in logos:
    logo = logos[l]
  if regen:
    print(f"system: + CRIT:   You need to regenerate the logo cache files with the -l option", file=log)

def GetNodeTypes(bpjson, nodeType):
  nodes = []
  for node in bpjson['nodes']:
    if node['node_type'] != nodeType:
      continue
    nodes.append(node)
  return nodes

def NodesToFeatures(nodes, icon, producer):
  features = []
  for node in nodes:
    properties = {}
    if producer in top21:
      properties['icon'] = icon[0]
    else:
      properties['icon'] = icon[1]
    properties['name'] = producer
    feature = {}
    feature['type'] = 'Feature'
    feature['properties'] = properties
    location = node['location']
    geometry = {}
    geometry['type'] = 'Point'
    newCoordinates = []
    newCoordinates.append(location['longitude'])
    newCoordinates.append(location['latitude'])
    geometry['coordinates'] = newCoordinates
    feature['geometry'] = geometry
    features.append(feature)
  return features

def GenerateMapInfo(producers, bpjsons, logos):
  # The first icon for the type is for TOP21, the second is for Standby's.
  # A Hyperion node is a special case of a Full node answering a Hyperion health check.
  iconMap = {
    'full': ['http://maps.google.com/mapfiles/ms/micons/red.png', 'http://maps.google.com/mapfiles/ms/micons/purple.png'],
    'hyperion': ['http://maps.google.com/mapfiles/ms/micons/red-dot.png', 'http://maps.google.com/mapfiles/ms/micons/purple-dot.png'],
    'producer': ['http://maps.google.com/mapfiles/ms/micons/blue.png', 'http://maps.google.com/mapfiles/ms/micons/green.png'],
    'seed': ['http://maps.google.com/mapfiles/ms/micons/yellow.png', 'http://maps.google.com/mapfiles/ms/micons/orange.png']
  }
  nodeTypes = []
  for nt in iconMap:
    nodeTypes.append(nt)
  features = {}
  for nodeType in nodeTypes:
    features[nodeType] = []

  for p in bpjsons['chain']:
    bpjson = bpjsons['chain'][p]
    for nt in nodeTypes:
      nodes = GetNodeTypes(bpjson, nt)
      if len(nodes):
        features[nt].extend(NodesToFeatures(nodes, iconMap[nt], p))
      # Special case for full/hyperion:
      if nt == 'full':
        # All the full nodes are now in nodes, for each of them
        # check to see if the endpoint answers a Hyperion health check and if it does then create
        # a feature of hyperion for it.
        for node in nodes:
          # Check the api_endpoint first then the ssl_endpoint if the api_endpoint fails
          for endpoint in (node['api_endpoint'], node['ssl_endpoint']):
            hcURL = endpoint + "/v2/health"
            info = subprocess.run(['curl', '-LSsf', '--connect-timeout', '5',
              '-H', 'accept: application/json', '-H', 'Content-Type: application/json', hcURL],
              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if info.returncode == 0:
              # It answers Hyperion calls.  It's maybe not healthy, but Hyperion is there.
              features['hyperion'].extend(NodesToFeatures([node], iconMap['hyperion'], p))
              break

  for nt in nodeTypes:
    mapInfo = {}
    mapInfo['type'] = 'FeatureCollection'
    mapInfo['features'] = features[nt]
    f = open("pmi-{net}-{nodeType}.js".format(net="mainnet" if args.mainnet else "testnet", nodeType=nt), 'w')
    print(f"var {nt}Nodes =", file=f)
    json.dump(mapInfo, f, indent=2)
    print(";", file=f)
    f.close()
    print(f"system: generated {len(features[nt])} {nt} map markers", file=log)

  return

def FilterNodeActionsByProducer(nodeActions, producer):
  fna = {}
  for ts in nodeActions:
    na = nodeActions[ts]
    if na['data']['producer_account_name'] == producer:
      fna[ts] = na
  return fna

def GenerateNodeInfoTables():
  nodeTypes = ['full', 'hyperion', 'producer', 'seed']
  JSONActions = {}
  nodeActions = {}
  filename = ProducerBPJSONFilename('chain')
  with open(filename) as fh:
    JSONActions = json.load(fh)
  for action in JSONActions['actions']:
    dt = dateutil.parser.parse(action['@timestamp'])
    timestamp = time.mktime(dt.timetuple()) + dt.microsecond / 1000000
    tsJSON = {}
    tsJSON['action'] = action['act']['name']
    tsJSONData = ''
    if tsJSON['action'] == 'set':
      tsJSONData = action['act']['data']['json']
      try:
        tsJSON['data'] = json.loads(tsJSONData)
        nodeActions[timestamp] = tsJSON
      except json.decoder.JSONDecodeError:
        actor = action['act']['authorization'][0]['actor']
        print(f"system: chain producerjson action data @{action['@timestamp']} could not be decoded as JSON, actor={actor}", file=log)
    elif tsJSON['action'] == 'del':
      tsJSON['data'] = {'producer_account_name': action['act']['data']['owner']}
      nodeActions[timestamp] = tsJSON
    else:
      print(f"system: found an unknown action on the producerjson actions: {tsJSON['name']}", file=log)
  print(f"system: There are {len(nodeActions)} producerjson actions on the chain", file=log)
  countries = set()
  producerNodes = {}
  for p in producers:
    producerActions = {}
    for ts, action in sorted(FilterNodeActionsByProducer(nodeActions, p).items()):
      producerActions[ts] = action
    if len(producerActions): print(f"{p}: {len(producerActions)} node actions", file=log)
    producerNodes[p] = []
    for ts in producerActions:
      na = producerActions[ts]
      action = na['action']
      unknownNodes = set()
      # Any currently existing nodes in producerNodes become tentatively deleted.
      for pnode in producerNodes[p]:
        pnode['deleted'] = ts
      if action == 'set':
        for node in na['data']['nodes']:
          nodetype = node['node_type']
          producerNode = {}
          producerNode['fuzzy'] = ''
          if isinstance(nodetype, list):
            node_types = nodetype
            for nodetype in node_types:
              if not nodetype in nodeTypes:
                if nodetype.lower() in ['api', 'query']:
                  producerNode['fuzzy'] = nodetype
                  nodetype = 'full'
                  break
                else:
                  unknownNodes.add(nodetype)
                  continue
          else:
            if not nodetype in nodeTypes:
              if nodetype.lower() in ['api', 'query']:
                producerNode['fuzzy'] = nodetype
                nodetype = 'full'
              else:
                unknownNodes.add(nodetype)
                continue
          location = node['location']
          producerNode['timestamp'] = ts
          producerNode['deleted'] = ''
          producerNode['producer'] = p
          producerNode['node_type'] = nodetype
          producerNode['location'] = location
          countries.add(location['country'])
          # If we already have this node in producerNodes, then just mark it not deleted.
          found = False
          for pnode in producerNodes[p]:
            # TODO: We need to compare the country, lat & long of location
            if pnode['node_type'] == nodetype and pnode['location'] == location:
              found = True
              pnode['deleted'] = ''
          # If it wasn't in producerNodes - it is now.
          if not found:
            producerNodes[p].append(producerNode)
      elif action == 'del':
        for pnode in producerNodes[p]:
          pnode['deleted'] = ts
      if len(unknownNodes): print(f"{p}: + has unknown node types: {unknownNodes}", file=log)

  print(f"system: {len(countries)} countries have nodes in them.", file=log)
  print(f"system: + countries={sorted(countries)}", file=log)
  nodesByCountry = {}
  for country in countries:
    nodesByCountry[country] = []
  for p in producers:
    for pnode in producerNodes[p]:
      if pnode['deleted'] == '':
        nodesByCountry[pnode['location']['country']].append(pnode)
        if len(pnode['fuzzy']): print(f"{p}: + has fuzzy node type: {pnode['fuzzy']}", file=log)
  nbcFilename = "nodes-by-country-{net}.txt".format(net="mainnet" if args.mainnet else "testnet")
  with open(nbcFilename, "w") as f:
    print(f"# This data was collected at: {datetime.datetime.utcnow()} UTC", file=f)
    for country in sorted(countries):
      for pnode in sorted(nodesByCountry[country], key = lambda t: (t['timestamp'])):
        print(f"{country} {pnode['node_type']} {pnode['timestamp']} {pnode['producer']} {pnode['location']['latitude']} {pnode['location']['longitude']}", file=f)

  return

# MAIN
parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-m", "--mainnet", help="Process Mainnet", action="store_true")
group.add_argument("-t", "--testnet", help="Process Testnet", action="store_true")
parser.add_argument("-b", "--bpjsons", help="Force an update to the cached producer bp.json files", action="store_true")
#parser.add_argument("-c", "--check", help="Check the consistency of the producer information", action="store_true")
parser.add_argument("-l", "--logos", help="Force an update to the cached producer logo files", action="store_true")
parser.add_argument("-p", "--producers", help="Force an update for the cached producers json file", action="store_true")
#parser.add_argument("-v", "--verbose", help="Be verbose while processing", action="store_true")
parser.add_argument("-o", "--output", help="Where to write the output log (default is stdout)")
args = parser.parse_args()

if args.output and len(args.output) > 0:
  log = open(args.output, 'w')
else:
  log = sys.stdout

#chainURL = "https://chain.wax.io" if args.mainnet else "https://testnet.waxsweden.org"
#historyURL = "https://api.waxsweden.org" if args.mainnet else "https://testnet.waxsweden.org"
#chainURL = "https://chain.wax.io" if args.mainnet else "https://testnet.blokcrafters.io"
#historyURL = "https://api.blokcrafters.io" if args.mainnet else "https://testnet.blokcrafters.io"
#chainURL = "https://wax.blokcrafters.io" if args.mainnet else "https://testnet.waxsweden.org"
#historyURL = "https://wax.blokcrafters.io" if args.mainnet else "https://testnet.waxsweden.org"
chainURL = "https://wax.blokcrafters.io" if args.mainnet else "https://testnet.wax.pink.gg"
historyURL = "https://wax.blokcrafters.io" if args.mainnet else "https://testnet.wax.pink.gg"
producersFilename = "{net}-jsons/producers.json".format(net="mainnet" if args.mainnet else "testnet")
top21Filename = "{net}-jsons/top21.json".format(net="mainnet" if args.mainnet else "testnet")

top21, producers = GetProducers(chainURL, producersFilename, top21Filename, force=args.producers)
print("system: {total} producers in total for {net}".format(net="mainnet" if args.mainnet else "testnet", total=len(producers)), file=log)
VerifyProducers(producers)

bpjsons = GetBPJSONs(historyURL, producers, force=(args.bpjsons or args.producers))
print("system: {total} url bp.json files in total for {net}".format(net="mainnet" if args.mainnet else "testnet", total=len(bpjsons['url'])), file=log)
VerifyBPJSONs(bpjsons)

logos = GetLogos('url', producers, bpjsons, force=(args.logos or args.bpjsons or args.producers))
print("system: {total} logo files in total for {net}".format(net="mainnet" if args.mainnet else "testnet", total=len(logos)), file=log)

print(f"system: Checking consistency of the producer information...", file=log)
CheckConsistency(producers, bpjsons, logos)
print(f"system: Consistency check complete", file=log)

GenerateMapInfo(producers, bpjsons, logos)
GenerateNodeInfoTables()

print(f"system: complete.", file=log)

if args.output and len(args.output) > 0:
  log.close()

sys.exit(0)
