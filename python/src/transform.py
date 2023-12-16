#cloud_function(platforms=[Platform.AWS], memory=3008, config=config)
def yourFunction(request, context):
    import json
    import logging
    from Inspector import Inspector
    import time
    import boto3
    from datetime import datetime
    
    # Import the module and collect data 
    inspector = Inspector()
    inspector.inspectAll()
    s3 = boto3.resource('s3')
    obj = s3.Object('test.bucket.462562f23.kl', '100000 Sales Records.csv')
    body = obj.get()['Body'].read().decode('utf-8')
    body = body.strip().split('\r\n')
    title = body[0].split(',')
    main = body[1:]
    lstmain = []

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


        

    object = s3.Object('test.bucket.462562f23.kl', '100001 Sales Records.csv')
    object.put(Body=','.join(title) + '\r\n' + '\r\n'.join([','.join(i) for i in lstmain]))
    # inspector.addAttribute("value1", ','.join(title) + '\r\n' + '\r\n'.join([','.join(i) for i in lstmain]) )
    # inspector.addAttribute("value2", f'count: %s, total: %s, ave: %s'%(count, total, total/count))
    # inspector.addAttribute("value2", obj.get())
    # inspector.addAttribute("value3", obj.get()['Body'].read())


    # Add custom message and finish the function
    if ('name' in request):
        inspector.addAttribute("message", "Mello " + str(request['name']) + "!")
    else:
        inspector.addAttribute("message", "Dello World!")
    
    inspector.inspectPlatform()
    return inspector.finish()
