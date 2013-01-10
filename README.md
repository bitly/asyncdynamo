Asyncdynamo
===========

Asynchronous Amazon DynamoDB library for Tornado

Requires boto>=2.3 and python 2.7

Tested with Tornado 1.2.1

Installation
------------

Installing from github: `pip install  git+https://github.com/bitly/asyncdynamo.git`

Installing from source: `git clone git://github.com/bitly/asyncdynamo.git; cd asyncdynamo; python setup.py install`

Usage
-----
Asyncdynamo syntax seeks to mirror that of [Boto](http://github.com/boto/boto).

```python
from asyncdynamo import asyncdynamo
db = asyncdynamo.AsyncDynamoDB("YOUR_ACCESS_KEY", "YOUR_SECRET_KEY")

def item_cb(item):
	print item
	
db.get_item('YOUR_TABLE_NAME', 'ITEM_KEY', item_cb)
```

Requirements
------------
The following two python libraries are required

* [boto](http://github.com/boto/boto)
* [tornado](http://github.com/facebook/tornado)

Issues
------

Please report any issues via [github issues](https://github.com/bitly/asyncdynamo/issues)
