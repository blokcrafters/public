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

def fileExists(filename):
    return os.path.exists(filename) and os.path.isfile(filename)

def fileOlderThan(filename, maxAge):
    return (not fileExists(filename)) or (int(os.path.getmtime(filename)) < (int(datetime.datetime.utcnow().timestamp()) - maxAge))

def UpdateProducers(chainURL, producersFilename, top21Filename):
    print(f"system: fetching the producers from {chainURL} ...")
    numToGet = 50
    start = ''
    inactives = {}
    producers = {}
    top21 = []

    while True:
        info = subprocess.run(['cleos', '--url', chainURL, 'system', 'listproducers', '--json',
                               '--limit', str(numToGet), '--lower', start],
                              stdout=subprocess.PIPE)
        someProducers = json.loads(info.stdout)
        for i in someProducers['rows']:
            p = i['owner']
            if i['is_active'] == 1:
                producers[p] = i
            else:
                inactives[p] = i
            if args.verbose: print(f"system: producer {len(producers) + len(inactives)}: {p}")
        start = someProducers['more']
        if len(start) <= 0:
            break

    print(f"system: ... done.")
    print(f"system: producers: {len(producers)} active and {len(inactives)} inactive")

    # Get the Top21 producers
    info = subprocess.run(['cleos', '--url', chainURL, 'get', 'schedule', '--json'],
                            stdout=subprocess.PIPE)
    someJSON = json.loads(info.stdout)
    for p in someJSON['active']['producers']:
        top21.append(p['producer_name'])
    print(f"system: top21 producers: {len(top21)}")

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
                print(f"{p}: Force is True - removing the producer's cached url bp.json file - {filename}")
                try:
                    os.remove(filename)
                except FileNotFoundError:
                    pass
            # Only check the URL for a new bp.json file when:
            #  - we do not have a url file
            #  - the age of the urls-last-checked file is too old
            if fileOlderThan(URLSLastCheckedFilename(), URLSLASTCHECKED_FILE_MAXAGE):
                if args.verbose: print(f"{p}: updating the {source} bp.json cache file from {url} ... ", end='')
                info = subprocess.run(['curl', '-RLSsf', '--connect-timeout', '5', '-z', filename, '-o', filename, url],
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if info.returncode != 0:
                    failReasons.append(f"FAIL: {url}: curl return code {info.returncode}: {info.stdout.decode('utf-8').rstrip()}")
                    if args.verbose: print(f"failed")
                else:
                    if args.verbose: print(f"success")
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
            if args.verbose: print(f"{p}: updating the {source} producerjson-actions.json cache file from {historyURL} ... ", end='')
            gaURL = historyURL + "/v2/history/get_actions?act.account=producerjson&limit=1000"
            info = subprocess.run(['curl', '-LSsf', '--connect-timeout', '5',
                '-H', 'accept: application/json', '-H', 'Content-Type: application/json', gaURL],
                stdout=subprocess.PIPE)
            if info.returncode != 0:
                failReasons.append(f"FAIL: {url}: curl return code {info.returncode}: {info.stdout.decode('utf-8').rstrip()}")
                if args.verbose: print(f"failed")
            else:
                with open(filename, 'wb') as fh:
                    fh.write(info.stdout)
                theActions = json.loads(info.stdout)
                if args.verbose: print("success")
        else:
            with open(filename) as fh:
                theActions = json.load(fh)
        if args.verbose: print(f"system: {len(theActions['actions'])} total producerjson actions on the chain")

        # Gather all the specific producer jsons from the actions into prodJSONs
        prodJSONs = {}
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
        if args.verbose: print(f"system: {p} Has {len(prodJSONs)} producjerjson actions on the chain")
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
            print(f"{p}: failed to retrieve url bp.json for the cache")
        elif source == 'chain':
            if args.verbose or len(failReasons) > 0:
                print(f"{p}: failed to retrieve chain bp.json data for the cache")
                if zeroActions:
                    failReasons.append(f"There are no producerjson action entries on the chain.")
        for reason in failReasons:
            print(f"{p}: + {reason}")
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
                        print(f"{p}: Force is True - removing the producer's cached url logo file - {logoFile}")
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
                            r = requests.head(url)
                            if 'Last-Modified' in r.headers:
                                urlTime = r.headers['Last-Modified']
                                urlDate = dateutil.parser.parse(urlTime, ignoretz=True)
                                if urlDate > fileMTime:
                                    updateLogoFile = True
                                    print(f"{p}: the producers logo file is newer than the cache file")
                            else:
                                print(f"{p}: could not retrieve a Last-Modified value for the producer logo file")
                                updateLogoFile = True
                        except FileNotFoundError:
                            print(f"{p}: url bp.json cache file ({logoFile}) is missing")
                            updateLogoFile = True

                    if updateLogoFile:
                        print(f"{p}: updating logo cache file ({logoFile}) from {url}")
                        r = requests.get(url)
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
                    print(f"{p}: the org.branding.logo_256 url is empty in org.branding")
            else:
                print(f"{p}: org.branding.logo_256 is missing from the url bp.json cache")
        else:
            print(f"{p}: org.branding is missing from the url bp.json cache")
    else:
        print(f"{p}: org is missing from the url bp.json cache")

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
            print(f"{p}: NOTE: the url field is empty.")

# Does nothing at the moment.
def VerifyBPJSONs(bpjsons):
    for p in bpjsons['url']:
        bpjson = bpjsons['url'][p]

# CHECK
def CheckConsistency(producers, bpjsons, logos):
    #
    # Skip the WAX special place holder producers that have names ending in .wax
    #
    print(f"system: + {len(producers)} producers")
    regen = False
    for p in producers:
        if p.endswith('.wax'):
            continue
        producer = producers[p]
        missing = []
        display = False
        # This shouldn't ever happen - we use the producer owner as the key to the producers.
        # So it is Critical when it does happen.
        if p != producer['owner']:
            regen = True
            display = True
            print(f"system: + CRIT: producer owner ({producer['owner']}) does not match producer name ({p})")
        if not producer['url']:
            missing.append('url')
        if not producer['location']:
            missing.append('location')
        else:
            if not (str(producer['location']).zfill(3) in iso3166.countries_by_numeric):
                display = True
                print(f"system: + WARN: producer {p} has an invalid value {producer['location']} for a country code (see ISO3166)")
        if len(missing):
            display = True
            print(f"system: + WARN: producer {p} does not have settings for {missing}")
        if display:
            print(f"system: + + producer = {producer}")
    if regen:
        print(f"system: + CRIT:   You need to regenerate the producer cache with the -p option")

    for source in ['url', 'chain']:
        print(f"system: + {len(bpjsons[source])} url bp.json cache files")
        regen = False
        for b in bpjsons[source]:
            bpjson = bpjsons[source][b]
            if b.endswith('.wax'):
                continue

            if b != bpjson['producer_account_name']:
                print(f"system: + INFO: producer_account_name ({bpjson['producer_account_name']}) does not match producer name ({b})")

            for node in bpjson['nodes']:
                lat = node['location']['latitude']
                lon = node['location']['longitude']
        if regen:
            print(f"system: + CRIT:   You need to regenerate url bp.json cache files with the -b option")

    print(f"system: + {len(bpjsons['url'])} logo cache files")
    regen = False
    for l in logos:
        logo = logos[l]
    if regen:
        print(f"system: + CRIT:   You need to regenerate the logo cache files with the -l option")

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
        print(f"system: generated {len(features[nt])} {nt} map markers")

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
    if args.verbose: print("success")
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
                print(f"system: chain producerjson action data @{action['@timestamp']} could not be decoded as JSON, actor={actor}")
        elif tsJSON['action'] == 'del':
            tsJSON['data'] = {'producer_account_name': action['act']['data']['owner']}
            nodeActions[timestamp] = tsJSON
        else:
            print(f"system: found an unknown action on the producerjson actions: {tsJSON['name']}")
    print(f"system: There are {len(nodeActions)} producerjson actions on the chain")
    countries = set()
    producerNodes = {}
    for p in producers:
        producerActions = {}
        for ts, action in sorted(FilterNodeActionsByProducer(nodeActions, p).items()):
            producerActions[ts] = action
        if len(producerActions): print(f"{p}: {len(producerActions)} node actions")
        producerNodes[p] = []
        for ts in producerActions:
            na = producerActions[ts]
            action = na['action']
            unknowns = set()
            # Any currently existing nodes in producerNodes become tentatively deleted.
            for pnode in producerNodes[p]:
                pnode['deleted'] = ts
            if action == 'set':
                for node in na['data']['nodes']:
                    nodetype = node['node_type']
                    if not nodetype in nodeTypes:
                        unknowns.add(nodetype)
                        continue
                    location = node['location']
                    producerNode = {}
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
            if len(unknowns): print(f"{p}: + has unknown node types: {unknowns}")

    print(f"system: {len(countries)} countries have nodes in them.")
    print(f"system: + countries={sorted(countries)}")
    nodesByCountry = {}
    for country in countries:
        nodesByCountry[country] = []
    for p in producers:
        for pnode in producerNodes[p]:
            if pnode['deleted'] == '':
                nodesByCountry[pnode['location']['country']].append(pnode)
    nbcFilename = "nodes-by-country-{net}.txt".format(net="mainnet" if args.mainnet else "testnet")
    with open(nbcFilename, "w") as f:
        print(f"# This data was collected at: {datetime.datetime.utcnow()} UTC", file=f)
        for country in sorted(countries):
            for pnode in sorted(nodesByCountry[country], key = lambda t: (t['timestamp'])):
                print(f"{country} {pnode['node_type']} {pnode['timestamp']} {pnode['producer']} {pnode['location']['latitude']} {pnode['location']['longitude']}", file=f)

    return

# MAIN
parser = argparse.ArgumentParser()
parser.add_argument("-b", "--bpjsons", help="Force an update to the cached producer bp.json files", action="store_true")
parser.add_argument("-c", "--check", help="Check the consistency of the producer information", action="store_true")
parser.add_argument("-l", "--logos", help="Force an update to the cached producer logo files", action="store_true")
parser.add_argument("-p", "--producers", help="Force an update for the producers json file", action="store_true")
parser.add_argument("-v", "--verbose", help="Be verbose while processing", action="store_true")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-m", "--mainnet", help="Work on the Mainnet", action="store_true")
group.add_argument("-t", "--testnet", help="Work on the Testnet", action="store_true")
args = parser.parse_args()

#chainURL = "https://chain.wax.io" if args.mainnet else "https://testnet.waxsweden.org"
#historyURL = "https://api.waxsweden.org" if args.mainnet else "https://testnet.waxsweden.org"
chainURL = "https://chain.wax.io" if args.mainnet else "https://testnet.blokcrafters.io"
historyURL = "https://api.blokcrafters.io" if args.mainnet else "https://testnet.blokcrafters.io"
producersFilename = "{net}-jsons/producers.json".format(net="mainnet" if args.mainnet else "testnet")
top21Filename = "{net}-jsons/top21.json".format(net="mainnet" if args.mainnet else "testnet")

top21, producers = GetProducers(chainURL, producersFilename, top21Filename, force=args.producers)
print("system: {total} producers in total for {net}".format(net="mainnet" if args.mainnet else "testnet", total=len(producers)))
VerifyProducers(producers)

bpjsons = GetBPJSONs(historyURL, producers, force=(args.bpjsons or args.producers))
print("system: {total} url bp.json files in total for {net}".format(net="mainnet" if args.mainnet else "testnet", total=len(bpjsons['url'])))
VerifyBPJSONs(bpjsons)

logos = GetLogos('url', producers, bpjsons, force=(args.logos or args.bpjsons or args.producers))
print("system: {total} logo files in total for {net}".format(net="mainnet" if args.mainnet else "testnet", total=len(logos)))

if args.check:
    print("system: Checking consistency of the producer information...")
    CheckConsistency(producers, bpjsons, logos)
    print("system: Consistency check complete")

GenerateMapInfo(producers, bpjsons, logos)
GenerateNodeInfoTables()

print("system: complete.")

sys.exit(0)
