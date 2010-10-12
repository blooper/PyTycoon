#!/usr/bin/env python

__author__ = "KAMEDAkyosuke"
__version__ = "0.7.1-0.0.1"

import httplib
import urllib
import sys
import re

class TycoonBaseError(Exception):
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
COLENC_MATCH = re.compile(r"colenc=([{0}])".format("|".join(ENCODE_TYPE.values())))
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
    self.response = None
    
  def close(self):
    try:
      if not self.response.isclosed():
        self.response.close()
      self.connection.close()
    except:
      raise TycoonBaseError()

  def __checkStatus(self, funcName, response):
    if response.status not in RESPONSE_STATUS[funcName]:
      raise TycoonUnexpectedStatusError()
    elif RESPONSE_STATUS[funcName][response.status]:
      raise RESPONSE_STATUS[funcName][response.status]()

  def __getKeyValue(self, response):
    body = response.read().rstrip()
    d = None
    contentType = response.getheader("content-type")
    m = COLENC_MATCH.match(contentType)
    if m is not None and m.groups()[0] == ENCODE_TYPE["BASE64"]:
      pass
    elif m is not None and m.groups()[0] == ENCODE_TYPE["QUOTED_PRINTABLE"]:
      pass
    elif m is not None and m.groups()[0] == ENCODE_TYPE["URL"]:
      pass
    else:
      d = dict([line.split("\t") for line in body.split("\n")])
    return d

  def __responseCleanuper(func):
    def inner(self, *args, **kw):
      try:
        self.response = None
        result = func(self, *args, **kw)
        self.response.read()
        self.response.close()
        return result
      except Exception, e:
        self.response.read()
        if self.response is not None:
          self.response.close()
        raise e
    return inner

  @__responseCleanuper
  def echo(self, dic):
      url = "/rpc/echo?{0}".format(urllib.urlencode(dic))
      self.connection.request("GET", url)
      self.response = self.connection.getresponse()
      self.__checkStatus("echo", self.response)
      return self.__getKeyValue(self.response)

  @__responseCleanuper
  def report(self):
    url = "/rpc/report"
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("report", self.response)
    return self.__getKeyValue(self.response)
  
  @__responseCleanuper
  def play_script(self, d):
    url = "/rpc/play_script?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("play_script", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def status(self, db):
    url = "/rpc/status?{0}".format(urllib.urlencode({"DB" : db}))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("status", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def clear(self, db=None):
    url = None
    if db:
      url = "/rpc/clear?{0}".format(urllib.urlencode({"DB" : db}))
    else:
      url = "/rpc/clear"
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("clear", self.response)

  @__responseCleanuper
  def synchronize(self, db, hard, command):
    url = "/rpc/synchronize?{0}".format(urllib.urlencode({"DB" : db
                                                          ,"hard" : hard
                                                          ,"command" : command}))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("synchronize", self.response)

  @__responseCleanuper
  def set(self, d):
    url = "/rpc/set?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("set", self.response)

  @__responseCleanuper
  def add(self, d):
    url = "/rpc/add?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("add", self.response)

  @__responseCleanuper
  def replace(self, d):
    url = "/rpc/replace?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("replace", self.response)

  @__responseCleanuper
  def append(self, d):
    url = "/rpc/append?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("append", self.response)
    
  @__responseCleanuper
  def increment(self, d):
    url = "/rpc/increment?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("increment", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def increment_double(self, d):
    url = "/rpc/increment_double?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("increment_double", self.response)
    return self.__getKeyValue(self.response)
    
  @__responseCleanuper
  def cas(self, d):
    url = "/rpc/cas?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cas", self.response)

  @__responseCleanuper
  def remove(self, d):
    url = "/rpc/remove?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("remove", self.response)

  @__responseCleanuper
  def get(self, d):
    url = "/rpc/get?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("get", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def set_bulk(self, d):
    url = "/rpc/set_bulk?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("set_bulk", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def remove_bulk(self, d):
    url = "/rpc/remove_bulk?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("remove_bulk", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def get_bulk(self, d):
    url = "/rpc/get_bulk?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("get_bulk", self.response)
    return self.__getKeyValue(self.response)
    
  @__responseCleanuper
  def cur_jump(self, d):
    url = "/rpc/cur_jump?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_jump", self.response)

  @__responseCleanuper
  def cur_jump_back(self, d):
    url = "/rpc/cur_jump_back?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_jump_back", self.response)

  @__responseCleanuper
  def cur_step(self, cur):
    url = "/rpc/cur_step?{0}".format(urllib.urlencode({"CUR" : cur}))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_step", self.response)

  @__responseCleanuper
  def cur_step_back(self, cur):
    url = "/rpc/cur_step_back?{0}".format(urllib.urlencode({"CUR" : cur}))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_step_back", self.response)

  @__responseCleanuper
  def cur_set_value(self, d):
    url = "/rpc/cur_set_value?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_set_value", self.response)

  @__responseCleanuper
  def cur_remove(self, cur):
    url = "/rpc/cur_remove?{0}".format(urllib.urlencode({"CUR" : cur}))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_remove", self.response)

  @__responseCleanuper
  def cur_get_key(self, d):
    url = "/rpc/cur_get_key?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_get_key", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def cur_get_value(self, d):
    url = "/rpc/cur_get_value?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_get_value", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def cur_get(self, d):
    url = "/rpc/cur_get?{0}".format(urllib.urlencode(d))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_get", self.response)
    return self.__getKeyValue(self.response)

  @__responseCleanuper
  def cur_delete(self, cur):
    url = "/rpc/cur_remove?{0}".format(urllib.urlencode({"CUR" : cur}))
    self.connection.request("GET", url)
    self.response = self.connection.getresponse()
    self.__checkStatus("cur_delete", self.response)

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
      exceptKey = "hoge"
      exceptValue = "hage"
      try:
        r = self.tycoon.echo({"key" : exceptKey
                              ,"value" : exceptValue})
        self.assertEqual(exceptKey, r["key"])
        self.assertEqual(exceptValue, r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_report_(self):
      try:
        r = self.tycoon.report()
        self.assertFalse("ERROR" in r)
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_play_script(self):
      try:
        r = self.tycoon.play_script({"key" : "hoge"
                                     ,"value" : "hage"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_status(self):
      try:
        r0 = self.tycoon.status("0")
        self.assertTrue("count" in r0)
        self.assertTrue("size" in r0)
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        r1 = self.tycoon.status("1")    # not exist db
        self.fail()
      except Exception, e:
        pass

    def test_rpc_set_and_get(self):
      try:
        exceptKey0 = "hoge0"
        exceptValue0 = "hage0"
        self.tycoon.set({"key" : exceptKey0
                         ,"value" : exceptValue0})
        r0 = self.tycoon.get({"key" : exceptKey0})
        self.assertEqual(exceptValue0, r0["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        exceptKey1 = "hoge1"
        exceptValue1 = "hage1"
        self.tycoon.set({"DB" : "0"
                         ,"key" : exceptKey1
                         ,"value" : exceptValue1})
        r1 = self.tycoon.get({"key" : exceptKey1})
        self.assertEqual(exceptValue1, r1["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      try:
        exceptKey2 = "hoge2"
        exceptValue2 = "hage2"
        self.tycoon.set({"DB" : "0"
                         ,"key" : exceptKey2
                         ,"value" : exceptValue2
                         ,"xt" : 1})
        r2 = self.tycoon.get({"key" : exceptKey2})
        self.assertEqual(exceptValue2, r2["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      time.sleep(2)
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.get
                        ,{"key" : exceptKey2})

    def test_rpc_add(self):
      try:
        self.tycoon.add({"key" : "hoge"
                         ,"value" : "hage"})
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      exceptKey = "hoge"
      exceptValue = "hage"
      self.tycoon.set({"key" : exceptKey
                       ,"value" : exceptValue})
      self.assertRaises(TycoonRecordExistError
                        ,self.tycoon.add
                        ,{"key" : exceptKey ,"value" : "foo"})

    def test_rpc_replace(self):
      try:
        self.tycoon.set({"key" : "hoge", "value" : "hage"})
        self.tycoon.replace({"key" : "hoge", "value" : "bar"})
        r = self.tycoon.get({"key" : "hoge"})
        self.assertEqual("bar", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.replace
                        ,{"key" : "not_exist_key", "value" : "hage"})

    def test_rpc_replace(self):
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.replace
                        ,{"key" : "not_exist_key", "value" : "hage"})
      try:
        self.tycoon.set({"key" : "hoge", "value" : "hage"})
        self.tycoon.replace({"key" : "hoge", "value" : "bar"})
        r = self.tycoon.get({"key" : "hoge"})
        self.assertEqual("bar", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_append(self):
      try:
        self.tycoon.append({"key" : "hoge", "value" : "hage"})
        r = self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hage", r["value"])

        self.tycoon.append({"key" : "hoge", "value" : "hage"})
        r = self.tycoon.get({"key" : "hoge"})
        self.assertEqual("hagehage", r["value"])
      except Exception, e:
        self.fail("{0}\t:\t{1}".format(e.__class__.__name__, e))

    def test_rpc_increment(self):
      r = self.tycoon.increment({"key" : "hoge"
                                 ,"num" : 1})
      self.assertEqual('1', r["num"])

      r = self.tycoon.increment({"key" : "hoge"
                                 ,"num" : 10})
      self.assertEqual('11', r["num"])

      self.tycoon.set({"key" : "not_number"
                       ,"value" : "\t"})
      self.assertRaises(TycoonNotCompatibleError
                        ,self.tycoon.increment
                        ,{"key" : "not_number", "num" : 1})

    def test_rpc_increment_double(self):
      r = self.tycoon.increment_double({"key" : "hage"
                                        ,"num" : 0.1})
      self.assertEqual(float("0.1"), float(r["num"]))
      
      r = self.tycoon.increment_double({"key" : "hage"
                                        ,"num" : 1.0})
      self.assertEqual(float('1.1'), float(r["num"]))
      
      self.tycoon.set({"key" : "not_number"
                       ,"value" : "\t"})
      self.assertRaises(TycoonNotCompatibleError
                        ,self.tycoon.increment_double
                        ,{"key" : "not_number", "num" : 0.1})

    def test_rpc_cas(self):
      self.tycoon.cas({"key" : "foo"
                       ,"nval" : "bar"})
      r = self.tycoon.get({"key" : "foo"})
      self.assertEqual("bar", r["value"])

      self.tycoon.cas({"key" : "foo"
                       ,"oval" : "bar"
                       ,"nval" : "newbar"})
      r = self.tycoon.get({"key" : "foo"})
      self.assertEqual("newbar", r["value"])

      self.assertRaises(TycoonAssumptionFaildError
                        ,self.tycoon.cas
                        ,{"key" : "foo"
                       ,"oval" : "diff_value"
                       ,"nval" : "newnewbar"})

      self.tycoon.cas({"key" : "foo"
                       ,"oval" : "newbar"})
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.get
                        ,{"key" : "foo"})

    def test_rpc_remove(self):
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.remove
                        ,{"key" : "not_exist_key"})
      
      self.tycoon.set({"key" : "foo"
                       ,"value" : "bar"})
      r = self.tycoon.get({"key" : "foo"})
      self.assertEqual("bar", r["value"])
      
      self.tycoon.remove({"key" : "foo"})
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.get
                        ,{"key" : "foo"})

    def test_rpc_set_balk(self):
      r = self.tycoon.set_bulk({"_foo" : "bar"})
      self.assertEqual("1", r["num"])      
      r = self.tycoon.get({"key" : "foo"})
      self.assertEqual("bar", r["value"])

      r = self.tycoon.set_bulk({"_foo" : "barbar"})
      self.assertEqual("1", r["num"])
      r = self.tycoon.get({"key" : "foo"})
      self.assertEqual("barbar", r["value"])

      r = self.tycoon.set_bulk({"_foo" : "bar"
                                ,"_hoge" : "hage"
                                ,"_aaa" : "bbb"})
      self.assertEqual("3", r["num"])
      r = self.tycoon.get({"key" : "foo"})
      self.assertEqual("bar", r["value"])
      r = self.tycoon.get({"key" : "hoge"})
      self.assertEqual("hage", r["value"])
      r = self.tycoon.get({"key" : "aaa"})
      self.assertEqual("bbb", r["value"])



    def test_rpc_remove_balk(self):
      r = self.tycoon.remove_bulk({"_foo" : ""})
      self.assertEqual("0", r["num"])      
      
      r = self.tycoon.set_bulk({"_foo" : "bar"
                                ,"_hoge" : "hage"
                                ,"_aaa" : "bbb"})
      
      r = self.tycoon.remove_bulk({"_foo" : ""
                                  ,"_hoge" : ""})

      self.assertEqual("2", r["num"])
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.get
                        ,{"key" : "foo"})
      self.assertRaises(TycoonRecordNotExistError
                        ,self.tycoon.get
                        ,{"key" : "hoge"})
      r = self.tycoon.get({"key" : "aaa"})
      self.assertEqual("bbb", r["value"])

    def test_rpc_get_balk(self):
      r = self.tycoon.get_bulk({"_foo" : ""})
      self.assertEqual("0", r["num"])

      r = self.tycoon.set_bulk({"_foo" : "bar"
                                ,"_hoge" : "hage"
                                ,"_aaa" : "bbb"})
      r = self.tycoon.get_bulk({"_foo" : ""
                                ,"_hoge" : ""
                                ,"_aaa" : ""})
      self.assertEqual("3", r["num"])
      self.assertEqual("bar", r["_foo"])
      self.assertEqual("hage", r["_hoge"])
      self.assertEqual("bbb", r["_aaa"])

    def test_rpc_cur(self):
      self.assertRaises(TycoonInvalidCursorError
                        ,self.tycoon.cur_jump
                        ,{"CUR" : "0"})

      r = self.tycoon.set_bulk({"_foo" : "bar"
                                ,"_hoge" : "hage"
                                ,"_aaa" : "bbb"})

      self.tycoon.cur_jump({"CUR" : "0"})
      k = self.tycoon.cur_get_key({"CUR" : "0"})
      v = self.tycoon.cur_get_value({"CUR" : "0"})
      r = self.tycoon.get({"key" : k["key"]})
      self.assertEqual(v["value"], r["value"])

      k2 = self.tycoon.cur_get_key({"CUR" : "0"
                                    ,"step" : ""})
      self.assertTrue(k["key"] == k2["key"])
      k3 = self.tycoon.cur_get_key({"CUR" : "0"})
      self.assertFalse(k2["key"] == k3["key"])

  suite = unittest.TestLoader().loadTestsFromTestCase(TestSequenceFunctions)
  unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
  main()
  
  
