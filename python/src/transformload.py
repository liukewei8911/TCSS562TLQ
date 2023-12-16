#cloud_function(platforms=[Platform.AWS], memory=3008, config=config)
def yourFunction(request, context):
    import json
    import logging
    from Inspector import Inspector
    import time
    import boto3
    from datetime import datetime
    import mysql.connector
    
    # Import the module and collect data 
    inspector = Inspector()
    inspector.inspectAll()
    s3 = boto3.resource('s3')
    
    filename = '100000 Sales Records.csv'
    obj = s3.Object('test.bucket.462562f23.kl', filename)
    body = obj.get()['Body'].read().decode('utf-8')
    body = body.strip().split('\r\n')
    title = body[0].split(',')
    main = body[1:]
    lstmain = []
    
    
    table_name = filename.split('.csv')[0]

    connection = mysql.connector.connect(host='tlq.cluster-xxxx.us-west-2.rds.amazonaws.com',
                                         database='TLQ',
                                         port='3306',
                                         user='admin',
                                         passwd='xxxx')
    
    
    sql = "create table " + "`" + filename.split('.csv')[0] +  "`" + " (`Region` text, `Country` text, `Item Type` text, `Sales Channel` text, `Order Priority` text, `Order Date` date, `Order ID` integer PRIMARY KEY, `Ship Date` date, `Units Sold` integer, `Unit Price` float, `Unit Cost` float, `Total Revenue` float, `Total Cost` float, `Total Profit` float, `Order Processing Time` integer, `Gross Margin` float);"
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS `%s`;"%(filename.split('.csv')[0]))
    cursor.execute(sql)


    sql_insert_query = f'INSERT INTO `{table_name}` (`Region`, `Country`, `Item Type`, `Sales Channel`, `Order Priority`, `Order Date`, `Order ID`, `Ship Date`, `Units Sold`, `Unit Price`, `Unit Cost`, `Total Revenue`, `Total Cost`, `Total Profit`, `Order Processing Time`, `Gross Margin`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    prepared_data = []
    
    transform_dict = {'L': 'Low', 'M': 'Medium', 'H': 'High', 'C': 'Critical'}
    orderidset = set()
    title.append('Order Date')
    title.append('Gross Margin')
    for i in main:
        lst = i.split(',')

        #1
        orderdata = lst[5]
        shipdate = lst[7]
        date1 = datetime.strptime(orderdata, '%m/%d/%Y')
        date2 = datetime.strptime(shipdate, '%m/%d/%Y')
        lst.append(str((date2 - date1).days))

        #2
        lst[4] = transform_dict[lst[4]]

        #3
        lst.append(str(float(lst[-2]) / float(lst[-3])))

        #4
        before = len(orderidset)
        orderidset.add(lst[6])
        if before != len(orderidset):
            lstmain.append(lst)

            #insert to sql
            lst[5] = datetime.strptime(lst[5], '%m/%d/%Y').date()
            lst[7] = datetime.strptime(lst[7], '%m/%d/%Y').date()
            
            
            prepared_data.append(lst)
    
    cursor.executemany(sql_insert_query, prepared_data)
    connection.commit()
    # object = s3.Object('test.bucket.462562f23.kl', '100001 Sales Records.csv')
    # object.put(Body=','.join(title) + '\r\n' + '\r\n'.join([','.join(i) for i in lstmain]))

    
    inspector.inspectPlatform()
    return inspector.finish()
