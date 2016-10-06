#!/bin/env python
# -*- coding:utf-8 -*-


import sys
sys.path.append('./')
import db_classes
from asterisk.agi import AGI

DEFAULT_DB='defs'
DEFS_TABLES=['3x','4x','8x','9x']
DEFAUL_PRIFIX='812'
  
def get_config(amp_conf='/etc/amportal.conf'):
  import re
  amp_conf=amp_conf
  conf_match=re.compile(r'(^[^#;]\w+)=([^\s]*)\s*')
  config=open(amp_conf,'r')
  conf_dict={}
  for i in config.readlines():
    if conf_match.match(i):
      conf_dict[conf_match.match(i).group(1)]=conf_match.match(i).group(2)
  config.close()
  return conf_dict
    
def ext_check(ast_exten='88001000000'):
  if len(ast_exten) == 11: 
    ast_exten=ast_exten[1:]
  if len(ast_exten) == 7: 
    ast_exten=DEFAUL_PRIFIX+ast_exten
  if len(ast_exten) == 10:
    DEF_TABLE=ast_exten[:1]+'x'
    DEF_CODE=ast_exten[:3]
    DEF_NUMBER=ast_exten[3:]
    return DEF_TABLE,DEF_CODE,DEF_NUMBER
  else:
    return None,None,None
  
  
    
def defs_check(ast_exten):
  table_ext,def_ext,num_ext = ext_check(ast_exten)
  if not table_ext:
    return "Unknown","Unknown"
  CONF=get_config()
  db_con=db_classes.DbMysql(host=CONF['AMPDBHOST'],username=CONF['AMPDBUSER'],password=CONF['AMPDBPASS'],db=DEFAULT_DB)
  db_con.set_character_set('utf8')
  res=db_con.getone("""SELECT `operator`,`region` FROM `%s` WHERE start <= '%s' AND end >= '%s' AND def='%s' LIMIT 1;""" % (table_ext,num_ext,num_ext,def_ext))
  if res:
    return res
  else:
    return "Unknown","Unknown"



def main():
  agi=AGI()
  try:
    AST_EXTEN=agi.get_variable('OP_NUM')
    OPERATOR,REGION=defs_check(AST_EXTEN)
    agi.verbose(""">>> Номер: %s , Оператор: %s , Регион: %s""" % (AST_EXTEN.strip(),OPERATOR.strip(),REGION.strip()))
    agi.set_variable('REGION',REGION)
    agi.set_variable('OPERATOR',OPERATOR)
  except ValueError:
    pass
  
     
if __name__ == '__main__':
  print sys.argv
  if len(sys.argv) > 1:
    OPERATOR,REGION=defs_check(sys.argv[1])
    print "Оператор:\t\t%s\nРегион:\t\t%s\n" % (OPERATOR,REGION)
  else:
    main()
