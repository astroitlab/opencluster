#ITEM META-INFO
META="._me_ta."
METAVERSION="._me_ta.version-"
METACREATER="._me_ta.createby-"
METACREATERIP="._me_ta.creatip"
METACREATETIME="._me_ta.creattime"
METATIMEOUT="._me_ta.timeout"
METAAUTH="._me_ta.auth"
METAPROP="._me_ta.prop"
METAUPDATER="._me_ta.updateby"
METAUPDATEIP="._me_ta.updateip"
METAUPDATETIME="._me_ta.updatetime"
HEARTBEAT="heartbeat"

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

def getMetaTimeout(domain):
    return domain + METATIMEOUT

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
  
