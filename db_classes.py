import sys
sys.dont_write_bytecode=True
class DbMysql(object):
  def __init__(self, host, db, username, password):
    global MySQLdb
    import MySQLdb
    self.host = host
    self.db = db
    self.username = username
    self.password = password
    self.connect()
  def set_character_set(self, character_set):
    try:
      self.conn.set_character_set(character_set)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
  def connect(self):
    try:
      self.conn = MySQLdb.connect(host=self.host, user=self.username, passwd=self.password, db=self.db)
    except MySQLdb.Error, e:
     print "Error %d: %s" % (e.args[0], e.args[1])
     sys.exit (1)
  def query(self, sql):
    cursor = self.conn.cursor()
    try:
      cursor.execute(sql)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    cursor.close()
  def getall(self, sql):
    cursor = self.conn.cursor()
    try:
      cursor.execute(sql)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    result_set = cursor.fetchall()
    cursor.close()
    return result_set
  def getone(self, sql):
    cursor = self.conn.cursor()
    try:
      cursor.execute(sql)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    result_set = cursor.fetchone()
    cursor.close()
    return result_set
  def createMemTable(self, name, field):
    cursor = self.conn.cursor()
    query = "create table %s (%s) TYPE=HEAP" % (name, field)
    try:
      cursor.execute(query)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    cursor.close()
  def createTable(self, name, field):
    cursor = self.conn.cursor()
    query = "create table %s (%s) TYPE=MyISAM DEFAULT CHARSET=utf8" % (name, field)
    try:
      cursor.execute(query)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    cursor.close()
  def renameTable(self, oldname, newname):
    cursor = self.conn.cursor()
    query = "rename table %s to btmp, %s to %s" % (newname, oldname, newname)
    try:
      cursor.execute(query)
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    try:
      cursor.execute("drop table btmp")
    except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
    cursor.close()
  def closeConnect(self):
    self.conn.close()
