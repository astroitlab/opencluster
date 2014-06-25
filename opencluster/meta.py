#ITEM META-INFO
META="._me_ta."
METAVERSION="._me_ta.version-"
METACREATER="._me_ta.createby-"
METACREATERIP="._me_ta.creatip"
METACREATETIME="._me_ta.creattime"
METAAUTH="._me_ta.auth"
METAPROP="._me_ta.prop"
METAUPDATER="._me_ta.updateby"
METAUPDATEIP="._me_ta.updateip"
METAUPDATETIME="._me_ta.updatetime"
HEARTBEAT="heartbeat"
#PROTOCOL
RMIRESPONSETIMEOUT="sun.rmi.transport.tcp.responseTimeout"
RMIHOSTNAME="java.rmi.server.hostname"
RMILOGCALL="java.rmi.server.logCalls"
RMIPROTOCOL="rmi\://"
RMICODEBASE="java.rmi.server.codebase"
SECURITYPOLICY="java.security.policy"
TMPDIR="java.io.tmpdir"
POLICY="grant{permission java.security.AllPermission;};"
#APP
REQROOT="/"
REQRES="/res"
REQADMIN="/admin"
REQFTTP="/admin/fttp.jsp"
REQGETFTTP="/admin/getFttp.jsp"
RSPE401="/WEB-INFO/err401.jsp"
RSPFTTPJSP="/WEB-INFO/fttp.jsp"
RSPFTTPJS="/WEB-INFO/fttp.js"
RSPE404="/WEB-INFO/err404.jsp"

def getMeta(domain):
    return domain + META
def getMetaVersion(domain):
    return domain + METAVERSION    
def getMetaCreater(domain):
    return domain + METACREATER    
def getMetaCreaterIP(domain):
    return domain + METACREATERIP
def getMetaCreateTime(domain):
    return domain + METACREATETIME   
def getMetaAuth(domain):
    return domain + METAAUTH    
def getMetaProperties(domain):
    return domain + METAPROP
    
def getMetaUpdater(domain):
    return domain + METAUPDATER    
def getMetaUpdaterIP(domain):
    return domain + METAUPDATEIP
def getMetaUpdateTime(domain):
    return domain + METAUPDATETIME     
  
