#!/bin/env python
# 
# Copyright 2010 bit.ly
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""
Created by Dan Frank on 2012-01-23.
Copyright (c) 2012 bit.ly. All rights reserved.
"""
import sys
assert sys.version_info >= (2, 7), "run this with python2.7"

import simplejson as json
from tornado.httpclient import HTTPRequest
from tornado.httpclient import AsyncHTTPClient
import functools
from collections import deque

from boto.connection import AWSAuthConnection
from boto.exception import DynamoDBResponseError
from boto.auth import HmacAuthV3HTTPHandler
from boto.provider import Provider

from async_aws_sts import AsyncAwsSts


class AsyncDynamoDB(AWSAuthConnection):
    DefaultHost = 'dynamodb.us-east-1.amazonaws.com'
    """The default DynamoDB API endpoint to connect to."""
    
    ServiceName = 'DynamoDB'
    """The name of the Service"""
    
    Version = '20111205'
    """DynamoDB API version."""
    
    ThruputError = "ProvisionedThroughputExceededException"
    """The error response returned when provisioned throughput is exceeded"""
    
    ExpiredSessionError = 'com.amazon.coral.service#ExpiredTokenException'
    """The error response returned when session token has expired"""
    
    UnrecognizedClientException = 'com.amazon.coral.service#UnrecognizedClientException'
    '''Another error response that is possible with a bad session token'''
    
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 host=None, debug=0, session_token=None,
                 authenticate_requests=True, validate_cert=True):
        if not host:
            host = self.DefaultHost
        self.validate_cert = validate_cert
        self.authenticate_requests = authenticate_requests 
        AWSAuthConnection.__init__(self, host,
                                   aws_access_key_id,
                                   aws_secret_access_key,
                                   is_secure, port, proxy, proxy_port,
                                   debug=debug, security_token=session_token)
        self.http_client = AsyncHTTPClient()
        self.pending_requests = deque()
        self.sts = AsyncAwsSts(aws_access_key_id, aws_secret_access_key)
        if authenticate_requests and not session_token:
            self.sts.get_session_token(self._update_session_token_cb) # init the session token
    
    def _required_auth_capability(self): # copied from boto layer1, looks important
        return ['hmac-v3-http']
    
    def _update_session_token_cb(self, creds, provider='aws', callback=None):
        '''
        Callback to use with `async_aws_sts`. The 'provider' arg is a bit misleading,
        it is a relic from boto and should probably be left to its default. This will
        take the new Credentials obj from `async_aws_sts.get_session_token()` and use
        it to update self.provider, and then will clear the deque of pending requests
        '''
        self.provider = Provider(provider,
                                 creds.access_key,
                                 creds.secret_key,
                                 creds.session_token)
        # force the correct auth, with the new provider
        self._auth_handler = HmacAuthV3HTTPHandler(self.host, None, self.provider)
        while self.pending_requests:
            request = self.pending_requests.pop()
            request()
        if callable(callback):
            return callback()
    
    def make_request(self, action, body='', callback=None, object_hook=None):
        '''
        Make an asynchronous HTTP request to DynamoDB. Callback should operate on
        the decoded json response (with object hook applied, of course). It should also
        accept an error argument, that will be a boto.exception.DynamoDBResponseError 
        '''
        this_request = functools.partial(self.make_request, action=action,
            body=body, callback=callback,object_hook=object_hook)
        if self.authenticate_requests and not self.provider.security_token: 
            self.pending_requests.appendleft(this_request)
            return
        headers = {'X-Amz-Target' : '%s_%s.%s' % (self.ServiceName,
                                                  self.Version, action),
                'Content-Type' : 'application/x-amz-json-1.0',
                'Content-Length' : str(len(body))}
        request = HTTPRequest('https://%s' % self.host, 
            method='POST',
            headers=headers,
            body=body,
            validate_cert=self.validate_cert)
        request.path = '/' # Important! set the path variable for signing by boto. '/' is the path for all dynamodb requests
        if self.authenticate_requests:
            self._auth_handler.add_auth(request) # add signature to headers of the request
        self.http_client.fetch(request, functools.partial(self._finish_make_request,
            callback=callback, orig_request=this_request, token_used=self.provider.security_token, object_hook=object_hook)) # bam!
    
    def _finish_make_request(self, response, callback, orig_request, token_used, object_hook=None):
        '''
        Check for errors and decode the json response (in the tornado response body), then pass on to orig callback
        '''
        json_response = json.loads(response.body, object_hook=object_hook)
        if response.error:
            if any((token_error in json_response.get('__type', []) \
                    for token_error in (self.ExpiredSessionError, self.UnrecognizedClientException))):
                if not self.provider.security_token:
                    # this means that we have just asked for a new session token, but have not gotten it back yet.
                    # consequently, we should add this to the list of requests to be retried when we get it back
                    self.pending_requests.appendleft(orig_request)
                    return
                elif token_used == self.provider.security_token:
                    # This means that we used an expired token, and have not tried to get a new one yet
                    # should insert logic to get a new session token and try again.
                    self.provider.security_token = None # invalidate the current security token
                    self.pending_requests.appendleft(orig_request) # schedule this request to be tried again
                    return self.sts.get_session_token(self._update_session_token_cb)
                else:
                    # the current session token is different from the one we used (ie it has been updated)
                    # should just try again with the new one
                    return orig_request()
            else:
                # because some errors are benign, include the response when an error is passed
                return callback(json_response, error=DynamoDBResponseError(response.error.code, 
                    response.error.message, response.body))
        return callback(json_response, error=None)
    
    def get_item(self, table_name, key, callback, attributes_to_get=None,
            consistent_read=False, object_hook=None):
        '''
        Issues an async tornado request to get an item
        '''
        data = {'TableName': table_name,
                'Key': key}
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if consistent_read:
            data['ConsistentRead'] = True
        return self.make_request('GetItem', body=json.dumps(data),
            callback=functools.partial(callback), object_hook=object_hook)
    
    def batch_get_item(self, request_items, callback):
        data = {'RequestItems' : request_items}
        json_input = json.dumps(data)
        self.make_request('BatchGetItem', json_input, callback)
        
    def put_item(self, table_name, item, callback, expected=None, return_values=None, object_hook=None):
        '''
        Issues an async request to create a new item or replace an old one.
        '''
        data = {'TableName' : table_name,
                'Item' : item}
        if expected:
            data['Expected'] = expected
        if return_values:
            data['ReturnValues'] = return_values
        json_input = json.dumps(data)
        return self.make_request('PutItem', json_input, callback=callback,
                                 object_hook=object_hook)
    
    def query(self, table_name, hash_key_value, callback, range_key_conditions=None,
              attributes_to_get=None, limit=None, consistent_read=False,
              scan_index_forward=True, exclusive_start_key=None,
              object_hook=None):
        '''
        Issues an async request to perform a query
        '''
        data = {'TableName': table_name,
                'HashKeyValue': hash_key_value}
        if range_key_conditions:
            data['RangeKeyCondition'] = range_key_conditions
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if limit:
            data['Limit'] = limit
        if consistent_read:
            data['ConsistentRead'] = True
        if scan_index_forward:
            data['ScanIndexForward'] = True
        else:
            data['ScanIndexForward'] = False
        if exclusive_start_key:
            data['ExclusiveStartKey'] = exclusive_start_key
        json_input = json.dumps(data)
        return self.make_request('Query', body=json_input,
                                 callback=callback, object_hook=object_hook)
