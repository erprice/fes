fes
===

Future Event Service

Fes is a REST service that accepts data payloads, holds them for a specified amount of time, and sends 
a POST call when that time is expired. This is a simple need that came up in several places of our 
infrastructure at Bronto Software. Each time we built a tightly integrated solution that was not 
applicable in other areas of the application. What we really needed was a service.

This implementation is a proof of concept. It is intended to accept and return payloads at very high 
throughput and up-to-the-second granularity. It uses a combination of hbase, redis, and lots of python 
daemons to achieve this.

The REST interface has four calls:
- add
- update event
- update expiration
- delete

Python dependencies include: flask, hashlib, redis, dateutil, requests, nose. Obviously, you must also 
have redis and hbase installed and running. Fes uses the hbase REST client.

NOTE: Fes is a work in progress. There is a laundry list of improvements in TODO.txt
