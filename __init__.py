#!/usr/bin/env python

"""
sample code 

import PyTycoon

tycoon = PyTycoon.PyTycoon.open()
tycoon.set({"key" : "hello"
            ,"value" : "world"})

res = tycoon.get({"key" : "hello"})
print res["value"]    -> world
"""

__author__ = "KAMEDAkyosuke"
__version__ = "0.9.0-0.0.1"

import httplib
import urllib
import base64
import quopri
import sys
import re

class TycoonBaseError(Exception):
  pass

class TycoonRequiredArgumentError(TycoonBaseError):
  pass

class TycoonUnexpectedStatusError(TycoonBaseError):
  pass

class TycoonLogicalError(TycoonBaseError):
  pass

class TycoonCommandFailedError(TycoonBaseError):
  pass

class TycoonRecordExistError(TycoonBaseError):
  pass

class TycoonRecordNotExistError(TycoonBaseError):
  pass

class TycoonNotCompatibleError(TycoonBaseError):
  pass

class TycoonAssumptionFaildError(TycoonBaseError):
  pass

class TycoonInvalidCursorError(TycoonBaseError):
  pass

class TycoonNotImplementedError(TycoonBaseError):
  pass

MAJOR_VERSION = 2.6
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1978
DEFAULT_TIMEOUT = 10
ENCODE_TYPE = {"BASE64" : "B"
               ,"QUOTED_PRINTABLE" : "Q"
               ,"URL" : "U"}
COLENC_MATCH = re.compile(r".*colenc=([{0}])$".format("|".join(ENCODE_TYPE.values())))
RESPONSE_STATUS = {"echo" : {200 : None}
                   ,"report" : {200 : None}
                   ,"play_script" : {200 : None
                                     ,450 : TycoonLogicalError}
                   ,"status" : {200 : None}
                   ,"clear" : {200 : None}
                   ,"synchronize" : {200 : None
                                     ,450 : TycoonCommandFailedError}
                   ,"set" : {200 : None}
                   ,"add" : {200 : None
                             ,450 : TycoonRecordExistError}
                   ,"replace" : {200 : None
                                 ,450 : TycoonRecordNotExistError}
                   ,"append" : {200 : None}
                   ,"increment" : {200 : None
                                   ,450 : TycoonNotCompatibleError}
                   ,"increment_double" : {200 : None
                                          ,450 : TycoonNotCompatibleError}
                   ,"cas" : {200 : None
                             ,450 : TycoonAssumptionFaildError}
                   ,"remove" : {200 : None
                                ,450 : TycoonRecordNotExistError}
                   ,"get" : {200 : None
                             ,450 : TycoonRecordNotExistError}
                   ,"set_bulk" : {200 : None}
                   ,"remove_bulk" : {200 : None}
                   ,"get_bulk" : {200 : None}
                   ,"vacuum" : {200 : None}
                   ,"cur_jump" : {200 : None
                                  ,450 : TycoonInvalidCursorError}
                   ,"cur_jump_back" : {200 : None
                                       ,450 : TycoonInvalidCursorError
                                       ,501 : TycoonNotImplementedError}
                   ,"cur_step" : {200 : None
                                  ,450 : TycoonInvalidCursorError}
                   ,"cur_step_back" : {200 : None
                                       ,450 : TycoonInvalidCursorError
                                       ,501 : TycoonNotImplementedError}
                   ,"cur_set_value" : {200 : None
                                       ,450 : TycoonInvalidCursorError}
                   ,"cur_remove" : {200 : None
                                    ,450 : TycoonInvalidCursorError}
                   ,"cur_get_key" : {200 : None
                                     ,450 : TycoonInvalidCursorError}
                   ,"cur_get_value" : {200 : None
                                       ,450 : TycoonInvalidCursorError}
                   ,"cur_get" : {200 : None
                                 ,450 : TycoonInvalidCursorError}
                   ,"cur_delete" : {200 : None
                                    ,450 : TycoonInvalidCursorError}
                   }

class PyTycoon(object):
  @classmethod
  def open(cls, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=DEFAULT_TIMEOUT):
    version = float(sys.version[0:3])
    if version >= MAJOR_VERSION:
      try:
        connection = httplib.HTTPConnection(host, port, timeout)
        return cls(connection)
      except:
        raise TycoonBaseError()
    else:
      raise TycoonBaseError()

  def __init__(self, connection):
    self.connection = connection
    
  def close(self):
    try:
      self.connection.close()
    except:
      raise TycoonBaseError()

  def __checkStatus(self, funcName, status):
    if status not in RESPONSE_STATUS[funcName]:
      raise TycoonUnexpectedStatusError()
    elif RESPONSE_STATUS[funcName][status]:
      raise RESPONSE_STATUS[funcName][status]()

  def __getKeyValue(self, contentType, body):
    if body == "": return None
    d = {}
    m = COLENC_MATCH.match(contentType)
    if m is not None and m.groups()[0] == ENCODE_TYPE["BASE64"]:
      d = dict([base64.b64decode(line).split("\t") for line in body.split("\n")])
    elif m is not None and m.groups()[0] == ENCODE_TYPE["QUOTED_PRINTABLE"]:
      d = dict([quopri.decodestring(line).split("\t") for line in body.split("\n")])
    elif m is not None and m.groups()[0] == ENCODE_TYPE["URL"]:
      d = dict([urllib.unquote(line).split("\t") for line in body.split("\n")])
    else:
      d = dict([line.split("\t") for line in body.split("\n")])
    return d

  # /rpc/echo
  # Echo back the input data as the output data, just for testing.
  # input: (optional): arbitrary records.
  # output: (optional): corresponding records to the input data.
  # status code: 200.
  def echo(self, d=None):
    url = None
    if d:
      url = "/rpc/echo?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/echo"
    self.connection.request("GET", url)
    response = None
    try:
      response = self.connection.getresponse()
      self.__checkStatus("echo", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/report
  # Get the report of the server information.
  # output: (optional): arbitrary records.
  # status code: 200.
  def report(self):
    url = "/rpc/report"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("report", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e
  
  # /rpc/play_script
  # Call a procedure of the script language extension.
  # input: name: the name of the procedure to call.
  # input: (optional): arbitrary records whose keys trail the character "_".
  # output: (optional): arbitrary keys which trail the character "_".
  # status code: 200, 450 (arbitrary logical error).
  def play_script(self, d):
    if "name" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/play_script?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("play_script", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/status
  # Get the miscellaneous status information of a database.
  # input: DB: (optional): the database identifier.
  # output: count: the number of records.
  # output: size: the size of the database file.
  # output: (optional): arbitrary records for other information.
  # status code: 200.
  def status(self, d=None):
    url = None
    if d:
      url = "/rpc/status?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/status"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("status", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/clear
  # Remove all records in a database.
  # input: DB: (optional): the database identifier.
  # status code: 200.
  def clear(self, d=None):
    url = None
    if d:
      url = "/rpc/clear?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/clear"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("clear", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/synchronize
  # Synchronize updated contents with the file and the device.
  # input: DB: (optional): the database identifier.
  # input: hard: (optional): for physical synchronization with the device.
  # input: command: (optional): the command name to process the database file.
  # status code: 200, 450 (the postprocessing command failed).
  def synchronize(self, d=None):
    url = None
    if d:
      url = "/rpc/synchronize?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/synchronize"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("synchronize", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/set
  # Set the value of a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: value: the value of the record.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # status code: 200.
  def set(self, d):
    if "key" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/set?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("set", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/add
  # Add a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: value: the value of the record.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # status code: 200, 450 (existing record was detected).
  def add(self, d):
    if "key" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/add?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("add", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/replace
  # Replace the value of a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: value: the value of the record.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # status code: 200, 450 (no record was corresponding).
  def replace(self, d):
    if "key" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/replace?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("replace", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/append
  # Append the value of a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: value: the value of the record.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # status code: 200.
  def append(self, d):
    if "key" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/append?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("append", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())    
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/increment
  # Add a number to the numeric integer value of a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: num: the additional number.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # output: num: the result value.
  # status code: 200, 450 (the existing record was not compatible).
  def increment(self, d):
    if "key" not in d or "num" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/increment?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("increment", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/increment_double
  # Add a number to the numeric double value of a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: num: the additional number.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # output: num: the result value.
  # status code: 200, 450 (the existing record was not compatible).
  def increment_double(self, d):
    if "key" not in d or "num" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/increment_double?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("increment_double", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e
    
  # /rpc/cas
  # Perform compare-and-swap.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # input: oval: (optional): the old value. If it is omittted, no record is meant.
  # input: nval: (optional): the new value. If it is omittted, the record is removed.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # status code: 200, 450 (the old value assumption was failed).
  def cas(self, d):
    if "key" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cas?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cas", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/remove
  # Remove a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # status code: 200, 450 (no record was found).
  def remove(self, d):
    if "key" not in d: 
      raise TycoonRequiredArgumentError()
    url = "/rpc/remove?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("remove", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/get
  # Retrieve the value of a record.
  # input: DB: (optional): the database identifier.
  # input: key: the key of the record.
  # output: value: (optional): the value of the record.
  # output: xt: (optional): the absolute expiration time. If it is omitted, there is no expiration time.
  # status code: 200, 450 (no record was found).
  def get(self, d):
    if "key" not in d: 
      raise TycoonRequiredArgumentError()
    url = "/rpc/get?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("get", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/set_bulk
  # Store records at once.
  # input: DB: (optional): the database identifier.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # input: (optional): arbitrary records whose keys trail the character "_".
  # output: num: the number of stored reocrds.
  # status code: 200.
  def set_bulk(self, d=None):
    url = None
    if d:
      url = "/rpc/set_bulk?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/set_bulk"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("set_bulk", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/remove_bulk
  # Store records at once.
  # input: DB: (optional): the database identifier.
  # input: (optional): arbitrary keys which trail the character "_".
  # output: num: the number of removed reocrds.
  # status code: 200.
  def remove_bulk(self, d=None):
    url = None
    if d:
      url = "/rpc/remove_bulk?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/remove_bulk"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("remove_bulk", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/get_bulk
  # Retrieve records at once.
  # input: DB: (optional): the database identifier.
  # input: (optional): arbitrary keys which trail "_".
  # output: num: the number of retrieved reocrds.
  # output: (optional): arbitrary keys which trail the character "_".
  # status code: 200.
  def get_bulk(self, d):
    url = None
    if d:
      url = "/rpc/get_bulk?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/get_bulk"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("get_bulk", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/vacuum
  # Scan the database and eliminate regions of expired records.
  # input: DB: (optional): the database identifier.
  # input: step: (optional): the number of steps. If it is omitted or not more than 0, the whole region is scanned.
  # status code: 200.  
  def vacuum(self, d=None):
    url = None
    if d:
      url = "/rpc/vacuum?{0}".format(urllib.urlencode(d))
    else:
      url = "/rpc/vacuum"
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("vacuum", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_jump
  # Jump the cursor to the first record for forward scan.
  # input: DB: (optional): the database identifier.
  # input: CUR: the cursor identifier.
  # input: key: (optional): the key of the destination record. If it is omitted, the first record is specified.
  # status code: 200, 450 (cursor is invalidated).
  def cur_jump(self, d):
    if "CUR" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_jump?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_jump_back", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_jump_back
  # Jump the cursor to a record for forward scan.
  # input: DB: (optional): the database identifier.
  # input: CUR: the cursor identifier.
  # input: key: (optional): the key of the destination record. If it is omitted, the last record is specified.
  # status code: 200, 450 (cursor is invalidated), 501 (not implemented).
  def cur_jump_back(self, d):
    if "CUR" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_jump_back?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_jump_back", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_step
  # Step the cursor to the next record.
  # input: CUR: the cursor identifier.
  # status code: 200, 450 (cursor is invalidated).
  def cur_step(self, d):
    if "CUR" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_step?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_step", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_step_back
  # Step the cursor to the previous record.
  # input: CUR: the cursor identifier.
  # status code: 200, 450 (cursor is invalidated), 501 (not implemented).
  def cur_step_back(self, d):
    if "CUR" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_step_back?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_step_back", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_set_value
  # Set the value of the current record.
  # input: CUR: the cursor identifier.
  # input: value: the value of the record.
  # input: step: (optional): to move the cursor to the next record. If it is omitted, the cursor stays at the current record.
  # input: xt: (optional): the expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time. If it is omitted, no expiration time is specified.
  # status code: 200, 450 (cursor is invalidated).
  def cur_set_value(self, d):
    if "CUR" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_set_value?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_set_value", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_remove
  # Remove the current record.
  # input: CUR: the cursor identifier.
  # status code: 200, 450 (cursor is invalidated).
  def cur_remove(self, d):
    if "CUR" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_remove?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_remove", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_get_key
  # Get the key of the current record.
  # input: CUR: the cursor identifier.
  # input: step: (optional): to move the cursor to the next record. If it is omitted, the cursor stays at the current record.
  # status code: 200, 450 (cursor is invalidated).
  def cur_get_key(self, d):
    if "CUR" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_get_key?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_get_key", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_get_value
  # Get the value of the current record.
  # input: CUR: the cursor identifier.
  # input: step: (optional): to move the cursor to the next record. If it is omitted, the cursor stays at the current record.
  # status code: 200, 450 (cursor is invalidated).
  def cur_get_value(self, d):
    if "CUR" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_get_value?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_get_value", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_get
  # Get a pair of the key and the value of the current record.
  # input: CUR: the cursor identifier.
  # input: step: (optional): to move the cursor to the next record. If it is omitted, the cursor stays at the current record.
  # output: xt: (optional): the absolute expiration time. If it is omitted, there is no expiration time.
  # status code: 200, 450 (cursor is invalidated).
  def cur_get(self, d):
    if "CUR" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_get?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_get", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

  # /rpc/cur_delete
  # Delete a cursor implicitly.
  # input: CUR: the cursor identifier.
  # status code: 200, 450 (cursor is invalidated).
  def cur_delete(self, d):
    if "CUR" not in d or "value" not in d:
      raise TycoonRequiredArgumentError()
    url = "/rpc/cur_remove?{0}".format(urllib.urlencode(d))
    response = None
    try:
      self.connection.request("GET", url)
      response = self.connection.getresponse()
      self.__checkStatus("cur_delete", response.status)
      return self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
    except Exception, e:
      if response:
        args = self.__getKeyValue(response.getheader("content-type"), response.read().rstrip())
        response.close()
        e.args = e.args + (args["ERROR"],)
      raise e

def main():
  import unittest
  import time
  class TestSequenceFunctions(unittest.TestCase):
    def setUp(self):
      self.tycoon = PyTycoon.open()
      self.tycoon.clear()

    def tearDown(self):
      self.tycoon.close()

    def test_rpc_echo(self):
      try:
        r = self.tycoon.echo()
        self.assertEqual(r, None)
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.echo({"key" : "hoge"
                              ,"value" : "hage"})
        self.assertEqual("hoge", r["key"])
        self.assertEqual("hage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_report_(self):
      try:
        r = self.tycoon.report()
        self.assertFalse("ERROR" in r)
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_play_script(self):
      """THIS TEST IS FAIL"""
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.play_script
                        ,{})

      try:
        self.tycoon.play_script({"name" : "hoge"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_status(self):
      try:
        r = self.tycoon.status()
        self.assertTrue("count" in r)
        self.assertTrue("size" in r)
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.status({"DB" : "0"})
        self.assertTrue("count" in r)
        self.assertTrue("size" in r)
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.status
                        ,{"DB" : "not_exist_db"})

    def test_rpc_clear(self):
      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.status
                        ,{"DB" : "not_exist_db"})

      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "hage"})
        r = self.tycoon.status()
        self.assertEqual(1, int(r["count"]))
        
        self.tycoon.clear()
        r = self.tycoon.status()
        self.assertEqual(0, int(r["count"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "hage"})
        r = self.tycoon.status()
        self.assertEqual(1, int(r["count"]))
        
        self.tycoon.clear({"DB" : "0"})
        r = self.tycoon.status()
        self.assertEqual(0, int(r["count"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))
    
    def test_rpc_synchronize(self):
      pass

    def test_rpc_set_and_get(self):
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.set
                        ,{"key" : "hoge"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.set
                        ,{"value" : "hoge"})

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.set
                        ,{"key" : "hoge"
                          ,"value" : "hage"
                          ,"DB" : "not_exist_db"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.get
                        ,{})

      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.get
                        ,{"key" : "not_exist_key"})
      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "hage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "hagehage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hagehage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "foo"
                         ,"DB" : "0"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("foo", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "foobar"
                         ,"DB" : "0"
                         ,"xt" : "1"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("foobar", r["value"])

        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))
      
    def test_rpc_add(self):
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.add
                        ,{"key" : "hoge"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.add
                        ,{"value" : "hoge"})

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.add
                        ,{"key" : "hoge"
                          ,"value" : "hage"
                          ,"DB" : "not_exist_db"})

      try:
        self.tycoon.add({"key" : "hoge"
                         ,"value" : "hage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))


      self.assertRaises(TycoonRecordExistError
                        ,self.tycoon.add
                        ,{"key" : "hoge", "value" : "hagehage"})
      try:
        self.tycoon.add({"key" : "foo"
                         ,"value" : "bar"
                         ,"DB" : "0"})
        r =  self.tycoon.get({"key" : "foo"
                              ,"DB" : "0"})
        self.assertEqual("bar", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.add({"key" : "hello"
                         ,"value" : "world"
                         ,"DB" : "0"
                         ,"xt" : "1"})
        r =  self.tycoon.get({"key" : "hello"
                              ,"DB" : "0"})
        self.assertEqual("world", r["value"])

        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hello"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_replace(self):
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.replace
                        ,{"key" : "hoge"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.replace
                        ,{"value" : "hoge"})

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.replace
                        ,{"key" : "hoge"
                          ,"value" : "hage"
                          ,"DB" : "not_exist_db"})

      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.replace
                        ,{"key" : "hoge", "value" : "hagehage"})

      try:
        self.tycoon.set({"key" : "hoge"
                         ,"value" : "hage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])

        self.tycoon.replace({"key" : "hoge"
                             ,"value" : "foo"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("foo", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.replace({"key" : "hoge"
                             ,"value" : "bar"
                             ,"DB" : "0"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("bar", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.replace({"key" : "hoge"
                             ,"value" : "hello"
                             ,"DB" : "0"
                             ,"xt" : "1"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("hello", r["value"])

        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_append(self):
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.append
                        ,{"key" : "hoge"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.append
                        ,{"value" : "hoge"})

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.append
                        ,{"key" : "hoge"
                          ,"value" : "hage"
                          ,"DB" : "not_exist_db"})

      try:
        self.tycoon.append({"key" : "hoge"
                            ,"value" : "hage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.append({"key" : "hoge"
                            ,"value" : "bar"
                            ,"DB" : "0"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("hagebar", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.append({"key" : "hoge"
                            ,"value" : "hello"
                            ,"DB" : "0"
                            ,"xt" : "1"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("hagebarhello", r["value"])
        
        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_increment(self):
      """THIS TEST IS FAIL"""
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.increment
                        ,{"key" : "hoge"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.increment
                        ,{"num" : "1"})

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.increment
                        ,{"key" : "hoge"
                          ,"num" : "1"
                          ,"DB" : "not_exist_db"})
      try:
        r = self.tycoon.increment({"key" : "hoge", "num" : "1"})
        self.assertEqual(1, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.increment({"key" : "hoge"
                                   ,"num" : "10"
                                   ,"DB" : "0"})
        self.assertEqual(11, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.increment({"key" : "hoge"
                                   ,"num" : "100"
                                   ,"DB" : "0"
                                   ,"xt" : "1"})
        self.assertEqual(111, int(r["num"]))
        
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual(111, int(r["value"]))
      
        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_increment_double(self):
      """THIS TEST IS FAIL"""
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.increment_double
                        ,{"key" : "hoge"})

      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.increment_double
                        ,{"num" : "0.1"})

      self.assertRaises(TycoonUnexpectedStatusError
                        ,self.tycoon.increment_double
                        ,{"key" : "hoge"
                          ,"num" : "0.1"
                          ,"DB" : "not_exist_db"})
      try:
        r = self.tycoon.increment_double({"key" : "hoge", "num" : "0.1"})
        self.assertEqual(0.1, float(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.increment_double({"key" : "hoge"
                                          ,"num" : "1.1"
                                          ,"DB" : "0"})
        self.assertEqual(1.2, float(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.increment_double({"key" : "hoge"
                                          ,"num" : "11.1"
                                          ,"DB" : "0"
                                          ,"xt" : "1"})
        self.assertEqual(12.3, float(r["num"]))
        
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual(12.3, float(r["value"]))
        
        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_cas(self):
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.cas
                        ,{})
      
      try:
        self.tycoon.cas({"key" : "hoge"})
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.cas({"key" : "hoge"
                         ,"nval" : "hage"
                         ,"DB" : "0"})
        r =  self.tycoon.get({"key" : "hoge"
                              ,"DB" : "0"})
        self.assertEqual("hage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      self.assertRaises(TycoonAssumptionFaildError
                        ,self.tycoon.cas
                        ,{"key" : "hoge"
                          ,"nval" : "hage"})
        
      try:
        self.tycoon.cas({"key" : "hoge"
                         ,"oval" : "hage"
                         ,"nval" : "foo"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("foo", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.cas({"key" : "hoge"
                         ,"oval" : "foo"})
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.cas({"key" : "hoge"
                         ,"nval" : "hello"
                         ,"xt" : "1"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hello", r["value"])
        
        time.sleep(2)
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"
                            ,"DB" : "0"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_remove(self):
      self.assertRaises(TycoonRequiredArgumentError
                        ,self.tycoon.remove
                        ,{})
      
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.remove
                        ,{"key" : "not_exist_key"})

      try:
        self.tycoon.set({"key" : "hoge", "value" : "hage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])

        self.tycoon.remove({"key" : "hoge"})
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        self.tycoon.set({"key" : "hoge", "value" : "hage"})
        r =  self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])

        self.tycoon.remove({"key" : "hoge"
                            ,"DB" : "0"})
        self.assertRaises(TycoonRecordNotExistError
                          ,self.tycoon.get
                          ,{"key" : "hoge"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_set_bulk_and_get_bulk(self):
      try:
        r = self.tycoon.set_bulk()
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.set_bulk({"prefix_not_underline" : "hage"})
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.set_bulk({"_hoge" : "hage"
                                  ,"_foo" : "bar"
                                  ,"_hello" : "world"
                                  ,"DB" : "0"})
        self.assertEqual(3, int(r["num"]))

        r = self.tycoon.get_bulk({"_hoge" : ""
                                  ,"_foo" : ""
                                  ,"_hello" : ""})
        self.assertEqual(3, int(r["num"]))
        self.assertEqual("hage", r["_hoge"])
        self.assertEqual("bar", r["_foo"])
        self.assertEqual("world", r["_hello"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.get_bulk({"prefix_not_underline" : ""})
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.get_bulk({"_hoge" : ""
                                  ,"_foo" : ""
                                  ,"_not_exist_key" : ""
                                  ,"DB" : "0"})
        self.assertEqual(2, int(r["num"]))
        self.assertEqual("hage", r["_hoge"])
        self.assertEqual("bar", r["_foo"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.set_bulk({"_hoge" : "hage"
                                  ,"_foo" : "bar"
                                  ,"_hello" : "world"
                                  ,"DB" : "0"
                                  ,"xt" : "1"})
        self.assertEqual(3, int(r["num"]))
        r = self.tycoon.get_bulk({"_hoge" : ""
                                  ,"_foo" : ""
                                  ,"_hello" : ""
                                  ,"DB" : "0"})
        self.assertEqual(3, int(r["num"]))
        self.assertEqual("hage", r["_hoge"])
        self.assertEqual("bar", r["_foo"])
        self.assertEqual("world", r["_hello"])

        time.sleep(2)
        r = self.tycoon.get_bulk({"_hoge" : ""
                                  ,"_foo" : ""
                                  ,"_hello" : ""
                                  ,"DB" : "0"})
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_remove_bulk(self):
      try:
        r = self.tycoon.remove_bulk()
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.remove_bulk({"prefix_not_underline" : "hage"})
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.set_bulk({"_hoge" : "hage"
                                  ,"_foo" : "bar"
                                  ,"_hello" : "world"
                                  ,"DB" : "0"})
        r = self.tycoon.remove_bulk({"_hoge" : ""
                                     ,"_foo" : ""
                                     ,"_hello" : ""})
        self.assertEqual(3, int(r["num"]))
        r = self.tycoon.get_bulk({"_hoge" : ""
                                  ,"_foo" : ""
                                  ,"_hello" : ""
                                  ,"DB" : "0"})
        self.assertEqual(0, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r = self.tycoon.set_bulk({"_hoge" : "hage"
                                  ,"_foo" : "bar"
                                  ,"_hello" : "world"
                                  ,"DB" : "0"})
        r = self.tycoon.remove_bulk({"_hoge" : ""
                                     ,"_foo" : ""
                                     ,"_not_exist_key" : ""})
        self.assertEqual(2, int(r["num"]))
        r = self.tycoon.get_bulk({"_hoge" : ""
                                  ,"_foo" : ""
                                  ,"_hello" : ""})
        self.assertEqual(1, int(r["num"]))
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

#     def test_rpc_vacuum(self):
#       pass

#     def test_rpc_cur_jump(self):
#       pass

#     def test_rpc_cur_jump_back(self):
#       pass

#     def test_rpc_cur_step(self):
#       pass

#     def test_rpc_cur_back(self):
#       pass

#     def test_rpc_cur_back(self):
#       pass

#     def test_rpc_set_value(self):
#       pass

#     def test_rpc_cur_remove(self):
#       pass

#     def test_rpc_cur_get_key(self):
#       pass

#     def test_rpc_cur_get_value(self):
#       pass

#     def test_rpc_cur_get(self):
#       pass

#     def test_rpc_cur_delete(self):
#       pass

  suite = unittest.TestLoader().loadTestsFromTestCase(TestSequenceFunctions)
  unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
  main()
