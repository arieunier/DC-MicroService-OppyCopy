import ujson, datetime, json, uuid, time
from dclibs import queuer, logs, rabbitmq_utils, config, rediscache, sfapi, postgres, utils, aws
from sqlalchemy.sql import text 

LOGGER = logs.LOGGER

SERVICE_NAME='oppycopy'
SERVICE_DESCRIPTION="Copies all your opportunities for a given account, fiscal year and status into a new fiscal year. Uses the ContextRecordId__c field given in the flow definition to identify the account id."
SERVICE_LABEL="Mass opportunities copy"
SERVICE_ATTRIBUTES=[
        {'AttributeName':'OriginFiscalYear', 'AttributeLabel': 'Origin Fiscal Year', 'AttributeDescription':'Optional - Origin Fiscal Year to copy the oppy from. If nothing is given, will copy oppies from all previous fiscal years.'},
        {'AttributeName':'DestinationFiscalYear', 'AttributeLabel': 'Destination Fiscal Year', 'AttributeDescription':'Optional - Destination Fiscal Year to copy the oppies to, default to current + 1 year.'},
        {'AttributeName':'OriginStageName', 'AttributeLabel': 'Origin Stage Name', 'AttributeDescription':'Optional - Filters opportunities to copy based this status.'},
        {'AttributeName':'DestinationStageName', 'AttributeLabel': 'Destination Stage NAme', 'AttributeDescription':'Optional - Sets the status of the opportunity created.'}
    ]
SERVICE_STRUCTURE = {'ServiceName':SERVICE_NAME,
    'ServiceLabel':SERVICE_LABEL,
    'ServiceDescription':SERVICE_DESCRIPTION,
    'ServiceAttributes' : SERVICE_ATTRIBUTES,
    'PublishExternally':True}
###
OBJOPPORTUNITY='OPPORTUNITY'
DEFAULTSTAGE='Prospecting'
MAXROWS = 500

# {!DestinationStageName}{!OriginFiscalYear}{!OriginStageName}


def transactional_insert(sqlRequest,rows, tableName, externalidfield):
    isolatedsession = postgres.dbSession_postgres()
    textSQLRequest=text(sqlRequest)
    #LOGGER.debug(sqlRequest)
    #LOGGER.debug(textSQLRequest)
    externalids = []
    nbInserts = len(rows)

    # lets add 500 records at a time
    batchesInsert = []
    batchesExternalIds = []
    tmpArrayInsert = []
    tmpArrayExternalIds = []
    i = 0
    bucketidx = 0
    j = 0
    for row in rows:
        i += 1
        j += 1
        if ( j ==  MAXROWS):
            tmpArrayInsert.append(row)        
            tmpArrayExternalIds.append(row[externalidfield])

            #max reached, add last and inc bucketidx, reinit the array
            batchesInsert.append(tmpArrayInsert)
            batchesExternalIds.append(tmpArrayExternalIds)
            tmpArrayInsert=[]
            tmpArrayExternalIds = []
            bucketidx +=1
            LOGGER.info("One bucket created")
            j = 0
        else:
            j += 1
            tmpArrayInsert.append(row)
            tmpArrayExternalIds.append(row[externalidfield])
    #check if id=0
    if (bucketidx == 0 and i != 0): # means that we have less than max rows
        batchesInsert.append(tmpArrayInsert)
        batchesExternalIds.append(tmpArrayExternalIds)
        
    LOGGER.info("{} buckets created for {} entries in total".format(bucketidx, i))

    try:
        batchId = 0
        for batch in batchesInsert:
            LOGGER.info("Inserting #{} records in table {} for batchId {}".format(len(batch), tableName, batchId))
            isolatedsession.execute(textSQLRequest, batch)
            batchId+=1
        
        # commits write        
        isolatedsession.commit()

        # now check results
        batchId = 0
        for batch in batchesExternalIds:
            LOGGER.info("Checking Heroku Connect sync for batchid {} ".format(batchId))
            getRows =  """ select count(*) as nbRows, _hc_lastop from """ + tableName + """ where """ + externalidfield + """ in %(ids)s group by _hc_lastop """
            hasHerokuConnectFinished = False
            while(hasHerokuConnectFinished == False):
                results = postgres.execRequest(getRows, {'ids':tuple(batch)})
                for result in results['data']:
                    if ('PENDING' in result['_hc_lastop']):
                        #LOGGER.info("Replication in progress")
                        hasHerokuConnectFinished=False
                        #sleeping 10 s to give some time
                        time.sleep(1)
                    else:
                        hasHerokuConnectFinished=True
                        #LOGGER.debug("Replication is now over")
            batchId +=1

        """
        try:
        for row in rows:
            externalids.append(row[externalidfield])
        LOGGER.info("Inserting #{} records in table {}".format(nbInserts, tableName))
        isolatedsession.execute(textSQLRequest, rows)
        LOGGER.info("Performing Heroku connect sync")
        


        isolatedsession.commit()
        #LOGGER.debug(externalids)
        getRows =  "select count(*) as nbRows, _hc_lastop from " + tableName + " where  + externalidfield + " in %(ids)s group by _hc_lastop "
        hasHerokuConnectFinished = False
        while(hasHerokuConnectFinished == False):
            results = postgres.execRequest(getRows, {'ids':tuple(externalids)})
            for result in results['data']:
                #LOGGER.debug(result)
                if ('PENDING' in result['_hc_lastop']):
                    #LOGGER.info("Replication in progress")
                    hasHerokuConnectFinished=False
                    #sleeping 10 s to give some time
                    time.sleep(10)
                else:
                    hasHerokuConnectFinished=True
                    #LOGGER.debug("Replication is now over")
        """

    except Exception as e:
        import traceback
        traceback.print_exc()
        isolatedsession.rollback()
    finally:
        isolatedsession.close()
        LOGGER.info("Finished interting data")

def storeSalesforceId(CreatedById, ContextRecordId__c, SFObjectName, SFPreviousRecordId, SFPreviousExternalId, SFNewExternalId):
    key = {'Process':'OppyCopy','CreatedById': CreatedById,'ContextRecordId__c':ContextRecordId__c, 'SFObjectName':SFObjectName, 'SFPreviousRecordId':SFPreviousRecordId,}
    value = {'SFNewExternalId':SFNewExternalId}
    rediscache.__setCache(key, ujson.dumps(value), 600)
    key = {'Process':'OppyCopy','CreatedById': CreatedById,'ContextRecordId__c':ContextRecordId__c, 'SFObjectName':SFObjectName, 'SFPreviousExternalId':SFPreviousExternalId,}
    value = {'SFNewExternalId':SFNewExternalId}
    rediscache.__setCache(key, ujson.dumps(value), 600)

def Step01_ProcessOpportunities(dictValue,OwnerId,ContextRecordId__c,OriginFiscalYear,OriginStageName,DestinationFiscalYear,DestinationStageName):
    # gets all the opportunities attached to the account given
    sqlRequest = """select stagename, accountid, amount, description, closedate, name, externalid__c, sfid, ownerid from salesforce.opportunity where ownerid = %(ownerid)s"""
    sqlAttribute = {'ownerid':OwnerId}

    now = datetime.datetime.now()


    if (ContextRecordId__c != ''):
        sqlRequest += ' and accountid = %(accountid)s '
        sqlAttribute['accountid'] = ContextRecordId__c
    if (OriginFiscalYear != ''):
        sqlRequest += ' and fiscalyear = %(OriginFiscalYear)s '
        sqlAttribute['OriginFiscalYear'] = OriginFiscalYear
    else:
        sqlRequest += ' and fiscalyear <= %(OriginFiscalYear)s '
        sqlAttribute['OriginFiscalYear'] = now.year
    if (OriginStageName != ''):
        sqlRequest += ' and stagename = %(OriginStageName)s '
        sqlAttribute['OriginStageName'] = OriginStageName
    
    #LOGGER.info("SQL Request => {}".format(sqlRequest))
    #LOGGER.info("SQL Attribute => {}".format(sqlAttribute))

    # destination current year
    if (DestinationFiscalYear == '' or DestinationFiscalYear == None):
        DestinationFiscalYear = now.year + 1
    if (DestinationStageName == ''):
        DestinationStageName=DEFAULTSTAGE


    oppies = postgres.execRequest(sqlRequest, sqlAttribute)
    # dict copy
    newDict = oppies
    for oppy in newDict['data']:
        # creates a unique external id
        uniqueexternalid = uuid.uuid4().__str__()
        # backup in redis
        storeSalesforceId(OwnerId, ContextRecordId__c, OBJOPPORTUNITY, oppy['sfid'],oppy['externalid__c'],uniqueexternalid)
        # updates external id
        oppy['externalid__c']=uniqueexternalid
        # updates fiscal year
        # oppy['fiscalyear'] = DestinationFiscalYear
        # updates stagename
        oppy['stagename'] = DestinationStageName
        newName = 'FY ' + str(DestinationFiscalYear) + ' - ' + oppy['name']
        oppy['name'] =  newName[0:120]
        oppy['closedate'] = oppy['closedate'].replace(year=int(DestinationFiscalYear))#datetime.datetime(DestinationFiscalYear, oppy['closedate'].month, oppy['closedate'].day)
        # removes previous sfid
        oppy.pop('sfid')

    # removes sfid
    newDict['columns'].remove('sfid')

    if (config.OPPYCOPY_DEFAULT_SEND_METHOD == 'HC'):
         # now generates the new SQL request 
        newSqlRequest = "INSERT INTO salesforce.opportunity("
        columns = ""
        values = ""
        for column in newDict['columns']:
            columns += column + ','
            values += " :" + column + ","
            newSqlRequest = newSqlRequest + ''
        columns = columns[:-1]
        values = values[:-1]
        newSqlRequest = newSqlRequest + columns + ') VALUES (' + values + ')'
        transactional_insert(newSqlRequest,newDict['data'], 'salesforce.opportunity', 'externalid__c')
        LOGGER.info(newSqlRequest)


    else:#means using bulkv2
        filename, CSV_Structure = utils.CSVWrite(newDict['columns'], newDict['data'])
        s3_url = aws.uploadData(filename, filename.split('/')[-1])
        dictValue['S3Url'] = s3_url
        dictValue['CSVStructure'] = CSV_Structure
        dictValue['SFObjectName'] = 'Opportunity'
        # loading 
        # sends now to the bulk service
        queuer.sendToQueuer(dictValue, config.SERVICE_BULK)
   


def treatMessage(dictValue):
    LOGGER.info(dictValue)
    # notifies the user the service starts treating
    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Process Started', True)
    
    ContextRecordId__c = dictValue['data']['payload']['ContextRecordId__c']
    OwnerId=dictValue['data']['payload']['CreatedById'] #0051t000002FB13AAG
    ContextRecordId = ''


    serviceAttributes=ujson.loads(dictValue['data']['payload']['ComputeAttributes__c'])
    # gets all the different value
    AccountId = ''
    if 'AccountId' in serviceAttributes: 
        AccountId=serviceAttributes['AccountId']
    OriginFiscalYear=serviceAttributes['OriginFiscalYear']
    DestinationFiscalYear=serviceAttributes['DestinationFiscalYear']
    OriginStageName=serviceAttributes['OriginStageName']
    DestinationStageName=serviceAttributes['DestinationStageName']


    # make sure we use the proper attribute either on accountid or contextrecordid__c
    if (ContextRecordId__c != '' and ContextRecordId__c != None):
        #will use this one as the context record id     
        ContextRecordId = ContextRecordId__c
    elif (AccountId != '' and AccountId != None):
        ContextRecordId = AccountId

    Step01_ProcessOpportunities(dictValue, OwnerId,ContextRecordId,OriginFiscalYear,OriginStageName,DestinationFiscalYear,DestinationStageName)

    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Process Ended', True)
    
# create a function which is called on incoming messages
def genericCallback(ch, method, properties, body):
    try:
        # transforms body into dict
        treatMessage(ujson.loads(body))
        
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        LOGGER.error(e.__str__())
def announce():
    # sends a message to the proper rabbit mq queue to announce himself
    queuer.sendToQueuer(SERVICE_STRUCTURE, config.SERVICE_REGISTRATION)    


if __name__ == "__main__":
    queuer.initQueuer()
    announce()
    queuer.listenToTopic(config.SUBSCRIBE_CHANNEL, 
    {
        config.QUEUING_KAFKA : treatMessage,
        config.QUEUING_CLOUDAMQP : genericCallback,
    })
