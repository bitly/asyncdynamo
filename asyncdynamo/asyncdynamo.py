#!/bin/env python
# 
# Copyright 2012 bit.ly
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
from tornado.ioloop import IOLoop
import functools
from collections import deque
import time
import logging

from boto.connection import AWSAuthConnection
from boto.exception import DynamoDBResponseError
from boto.auth import HmacAuthV3HTTPHandler
from boto.provider import Provider

from async_aws_sts import AsyncAwsSts, InvalidClientTokenIdError

PENDING_SESSION_TOKEN_UPDATE = "this is not your session token"

class AsyncDynamoDB(AWSAuthConnection):
    """
    The main class for asynchronous connections to DynamoDB.
    
    The user should maintain one instance of this class (though more than one is ok),
    parametrized with the user's access key and secret key. Make calls with make_request
    or the helper methods, and AsyncDynamoDB will maintain session tokens in the background.
    
    
    As in Boto Layer1:
    "This is the lowest-level interface to DynamoDB.  Methods at this
    layer map directly to API requests and parameters to the methods
    are either simple, scalar values or they are the Python equivalent
    of the JSON input as defined in the DynamoDB Developer's Guide.
    All responses are direct decoding of the JSON response bodies to
    Python data structures via the json or simplejson modules."
    """
    
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
                 authenticate_requests=True, validate_cert=True, max_sts_attempts=3, ioloop=None):
        if not host:
            host = self.DefaultHost
        self.validate_cert = validate_cert
        self.authenticate_requests = authenticate_requests 
        AWSAuthConnection.__init__(self, host,
                                   aws_access_key_id,
                                   aws_secret_access_key,
                                   is_secure, port, proxy, proxy_port,
                                   debug=debug, security_token=session_token)
        self.ioloop = ioloop or IOLoop.instance()
        self.http_client = AsyncHTTPClient(io_loop=self.ioloop)
        self.pending_requests = deque()
        self.sts = AsyncAwsSts(aws_access_key_id, aws_secret_access_key, ioloop=self.ioloop)
        assert (isinstance(max_sts_attempts, int) and max_sts_attempts >= 0)
        self.max_sts_attempts = max_sts_attempts
            
    def _init_session_token_cb(self, error=None):
        if error:
            logging.warn("Unable to get session token: %s" % error)
    
    def _required_auth_capability(self):
        return ['hmac-v3-http']
    
    def _update_session_token(self, callback, attempts=0, bypass_lock=False):
        '''
        Begins the logic to get a new session token. Performs checks to ensure
        that only one request goes out at a time and that backoff is respected, so
        it can be called repeatedly with no ill effects. Set bypass_lock to True to
        override this behavior.
        '''
        if self.provider.security_token == PENDING_SESSION_TOKEN_UPDATE and not bypass_lock:
            return
        self.provider.security_token = PENDING_SESSION_TOKEN_UPDATE # invalidate the current security token
        return self.sts.get_session_token(
            functools.partial(self._update_session_token_cb, callback=callback, attempts=attempts))
    
    def _update_session_token_cb(self, creds, provider='aws', callback=None, error=None, attempts=0):
        '''
        Callback to use with `async_aws_sts`. The 'provider' arg is a bit misleading,
        it is a relic from boto and should probably be left to its default. This will
        take the new Credentials obj from `async_aws_sts.get_session_token()` and use
        it to update self.provider, and then will clear the deque of pending requests.
        
        A callback is optional. If provided, it must be callable without any arguments,
        but also accept an optional error argument that will be an instance of BotoServerError.
        '''
        def raise_error():
            # get out of locked state
            self.provider.security_token = None
            if callable(callback):
                return callback(error=error)
            else:
                logging.error(error)
                raise error
        if error:
            if isinstance(error, InvalidClientTokenIdError):
                # no need to retry if error is due to bad tokens
                raise_error()
            else:
                if attempts > self.max_sts_attempts:
                    raise_error()
                else:
                    seconds_to_wait = (0.1*(2**attempts))
                    logging.warning("Got error[ %s ] getting session token, retrying in %.02f seconds" % (error, seconds_to_wait))
                    self.ioloop.add_timeout(time.time() + seconds_to_wait,
                        functools.partial(self._update_session_token, attempts=attempts+1, callback=callback, bypass_lock=True))
                    return
        else:
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
        accept an error argument, which will be a boto.exception.DynamoDBResponseError.
        
        If there is not a valid session token, this method will ensure that a new one is fetched
        and cache the request when it is retrieved. 
        '''
        this_request = functools.partial(self.make_request, action=action,
            body=body, callback=callback,object_hook=object_hook)
        if self.authenticate_requests and self.provider.security_token in [None, PENDING_SESSION_TOKEN_UPDATE]:
            # we will not be able to complete this request because we do not have a valid session token.
            # queue it and try to get a new one. _update_session_token will ensure that only one request
            # for a session token goes out at a time
            self.pending_requests.appendleft(this_request)
            def cb_for_update(error=None):
                # create a callback to handle errors getting session token
                # callback here is assumed to take a json response, and an instance of DynamoDBResponseError
                if error:
                    return callback({}, error=DynamoDBResponseError(error.status, error.reason, error.body))
                else:
                    return
            self._update_session_token(cb_for_update)
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
        Check for errors and decode the json response (in the tornado response body), then pass on to orig callback.
        This method also contains some of the logic to handle reacquiring session tokens.
        '''
        json_response = json.loads(response.body, object_hook=object_hook)
        if response.error:
            if any((token_error in json_response.get('__type', []) \
                    for token_error in (self.ExpiredSessionError, self.UnrecognizedClientException))):
                if self.provider.security_token == token_used:
                    # the token that we used has expired. wipe it out
                    self.provider.security_token = None
                return orig_request() # make_request will handle logic to get a new token if needed, and queue until it is fetched
            else:
                # because some errors are benign, include the response when an error is passed
                return callback(json_response, error=DynamoDBResponseError(response.error.code, 
                    response.error.message, response.body))
        return callback(json_response, error=None)
    
    def get_item(self, table_name, key, callback, attributes_to_get=None,
            consistent_read=False, object_hook=None):
        '''
        Return a set of attributes for an item that matches
        the supplied key.
        
        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)
        
        :type table_name: str
        :param table_name: The name of the table to delete.

        :type key: dict
        :param key: A Python version of the Key data structure
            defined by DynamoDB.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type consistent_read: bool
        :param consistent_read: If True, a consistent read
            request is issued.  Otherwise, an eventually consistent
            request is issued.        '''
        data = {'TableName': table_name,
                'Key': key}
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if consistent_read:
            data['ConsistentRead'] = True
        return self.make_request('GetItem', body=json.dumps(data),
            callback=callback, object_hook=object_hook)
    
    def batch_get_item(self, request_items, callback):
        """
        Return a set of attributes for a multiple items in
        multiple tables using their primary keys.
        
        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type request_items: dict
        :param request_items: A Python version of the RequestItems
            data structure defined by DynamoDB.
        """
        data = {'RequestItems' : request_items}
        json_input = json.dumps(data)
        self.make_request('BatchGetItem', json_input, callback)
        
    def put_item(self, table_name, item, callback, expected=None, return_values=None, object_hook=None):
        '''
        Create a new item or replace an old item with a new
        item (including all attributes).  If an item already
        exists in the specified table with the same primary
        key, the new item will completely replace the old item.
        You can perform a conditional put by specifying an
        expected rule.
        
        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type table_name: str
        :param table_name: The name of the table to delete.

        :type item: dict
        :param item: A Python version of the Item data structure
            defined by DynamoDB.

        :type expected: dict
        :param expected: A Python version of the Expected
            data structure defined by DynamoDB.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.        
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
        Perform a query of DynamoDB.  This version is currently punting
        and expecting you to provide a full and correct JSON body
        which is passed as is to DynamoDB.
        
        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type table_name: str
        :param table_name: The name of the table to delete.

        :type hash_key_value: dict
        :param key: A DynamoDB-style HashKeyValue.

        :type range_key_conditions: dict
        :param range_key_conditions: A Python version of the
            RangeKeyConditions data structure.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type limit: int
        :param limit: The maximum number of items to return.

        :type consistent_read: bool
        :param consistent_read: If True, a consistent read
            request is issued.  Otherwise, an eventually consistent
            request is issued.

        :type scan_index_forward: bool
        :param scan_index_forward: Specified forward or backward
            traversal of the index.  Default is forward (True).

        :type exclusive_start_key: list or tuple
        :param exclusive_start_key: Primary key of the item from
            which to continue an earlier query.  This would be
            provided as the LastEvaluatedKey in that query.
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
