#!/bin/env python
# -*- coding:utf-8 -*-


"""
CREATE DATABASE  `defs` DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;
GRANT SELECT , INSERT , UPDATE , DELETE , CREATE , DROP , INDEX , ALTER ON  `defs` . * TO  'asteriskuser'@'localhost';
CREATE TABLE IF NOT EXISTS `defs`.`3x` (
`def` SMALLINT( 3 ) NOT NULL COMMENT  'Код',
`start` INT( 7 ) NOT NULL COMMENT  'От',
`end` INT( 7 ) NOT NULL COMMENT  'До',
`operator` VARCHAR( 40 ) NOT NULL COMMENT  'Оператор',
`region` VARCHAR( 80 ) NOT NULL COMMENT  'Регион'
) ENGINE = MYISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci COMMENT =  'Коды операторов'
ALTER TABLE  `3x` ADD INDEX (  `def` )
"""

import urllib2,re,sys


DEFAULT_RE=r'<tr>\t<td>\t(?P<def>\d{3})\t</td>\t?<td>\t(?P<start>\d{7})\t</td>\t?<td>\t(?P<end>\d{7})\t</td>\t?<td>\t(?P<capacity>\d{,9})\t</td>\t?<td>\t(?P<operator>.*)\t</td>\t?<td>\t(?P<region>.*)\t</td>\t</tr>'
DEFAULT_DB='defs'
MERGE_TABLE='allx'
DEFAULT_URLS={
  '3x':'http://www.rossvyaz.ru/docs/articles/ABC-3x.html',
  '9x':'http://www.rossvyaz.ru/docs/articles/DEF-9x.html',
  '4x':'http://www.rossvyaz.ru/docs/articles/ABC-4x.html',
  '8x':'http://www.rossvyaz.ru/docs/articles/ABC-8x.html'}


class DEFUploader(object):
  def __init__(self,urls=DEFAULT_URLS,defs_re=DEFAULT_RE):
    self.urls=urls
    self.def_re=defs_re
    self.count=0
          
  def __del__(self):
    try:
      self.html_page_obj.close()
    except AttributeError:
      pass
    
  def urlopener(self):
    try:
      self.html_page_obj=urllib2.urlopen(self.url)
      return self.html_page_obj
    except urllib2.URLError:
      print "Не удается открыть URL %s" % self.url

    
  def getdefs(self):
    DF_RE=re.compile(self.def_re)
    if not hasattr(self,'htmlpage'):
      self.htmlpage=self.urlopener()
    for i in self.htmlpage:
      i=unicode(i,'cp1251')
      m=DF_RE.match(i)
      if m:
          yield (m.group('def'),m.group('start'), m.group('end'),m.group('operator'),m.group('region'))
          
  def defs_upload(self,cur,table):
    self.url=DEFAULT_URLS[table]
    self.table=table
    self.cur=cur
    if not hasattr(self,'htmlpage'):
      self.htmlpage=self.urlopener()
    #self.cur.query("""INSERT IGNORE INTO %s_old SELECT * FROM %s""" % (self.table,self.table))
    self.create_table()
    print "Заносим данные в таблицу %s" % self.table
    c=0
    for DEF,START,END,OPERATOR,REGION in self.getdefs():
      self.cur.query("""INSERT INTO %(TABLE)s VALUES('%(DEF)s','%(START)s','%(END)s','%(OPERATOR)s','%(REGION)s')""" % {'TABLE':self.table,'DEF':DEF,'START':START,'END':END,'OPERATOR':OPERATOR,'REGION':REGION})
      c+=1
    print "Занесено %d значений для таблицы %s" % (c,self.table)
    self.count+=c
    delattr(self,'htmlpage')
  
  def create_table(self,table=None):
    if table is None:
      table=self.table
    self.del_table(table)
    print "Создаем таблицу %(TABLE)s" % {'TABLE':table}
    self.cur.query("""CREATE TABLE IF NOT EXISTS `%(TABLE)s` (
      `def` SMALLINT( 3 ) NOT NULL COMMENT  'Код', INDEX(`def`),
      `start` INT( 7 ) NOT NULL COMMENT  'От',
      `end` INT( 7 ) NOT NULL COMMENT  'До',
      `operator` TEXT NOT NULL COMMENT  'Оператор',
      `region` TEXT NOT NULL COMMENT  'Регион'
      ) ENGINE = MYISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci COMMENT =  'Коды операторов'""" % {'TABLE':table} )
  
  def del_table(self,table=None):
    if table is None:
      table=self.table
    print "Чистим таблицу %s" % table
    self.cur.query("""DROP TABLE IF EXISTS `%s`""" % table)
   
  def merge_data(self,merge_table=None):
    if merge_table is None:
      self.merge_table=MERGE_TABLE
    """INSERT INTO allx SELECT * FROM 9x UNION SELECT * FROM 8x UNION SELECT * FROM 4x UNION SELECT * FROM 3x"""
    self.create_table(self.merge_table)
    print "Объединяем данные в таблицу %s , %s записей" % (self.merge_table,self.count)
    query='SELECT * FROM %s UNION '*len(DEFAULT_URLS.keys())
    query='INSERT INTO %s '+query.rsplit(' ',2)[0]
    self.cur.query(query % tuple(self.merge_table.splitlines() +DEFAULT_URLS.keys()))
    print "Готово..." 
  
  def get_config(self,amp_conf='/etc/amportal.conf'):
    self.amp_conf=amp_conf
    conf_match=re.compile(r'(^[^#;]\w+)=([^\s]*)\s*')
    config=open(self.amp_conf,'r')
    self.conf_dict={}
    for i in config.readlines():
     if conf_match.match(i):
        self.conf_dict[conf_match.match(i).group(1)]=conf_match.match(i).group(2)
    return self.conf_dict
    
    
class DBcon(object):
  def __init__(self, host, db, user, passwd):
    import MySQLdb
    self.MySQLdb=MySQLdb
    self.host = host
    self.db = db
    self.username = user 
    self.password = passwd
    self.connect()
    
  def connect(self):
    try:
      self.conn = self.MySQLdb.connect(host=self.host, user=self.username, passwd=self.password, db=self.db,use_unicode=True)
      self.conn.set_character_set('utf8')
    except self.MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
      sys.exit(1)
      
  def query(self, sql):
    cursor = self.conn.cursor()
    from warnings import filterwarnings
    filterwarnings('ignore', category = self.MySQLdb.Warning)
    try:
      cursor.execute(sql)
    except self.MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    cursor.close()
  def close(self):
    self.conn.close()
   
  
if __name__ == '__main__':
  DefUp=DEFUploader()
  CONF=DefUp.get_config()
  dbcon=DBcon(host=CONF['AMPDBHOST'],user=CONF['AMPDBUSER'],passwd=CONF['AMPDBPASS'],db=DEFAULT_DB)
  for table in DEFAULT_URLS.keys():
    DefUp.defs_upload(dbcon,table)
  if MERGE_TABLE:
    DefUp.merge_data()
    map(DefUp.del_table,DEFAULT_URLS.keys())
  dbcon.close()