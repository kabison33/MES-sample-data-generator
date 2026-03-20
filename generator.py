import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

route = pd.read_excel('RouteOrg.xlsx', 'route')
org = pd.read_excel('RouteOrg.xlsx', 'org')
# oper = pd.DataFrame(columns = ['ORDER_ID', 'OPER_KEY', 'OPER_NO', 'STEP_KEY', 'ACTUAL_INQUEUE_DATE', 'ACTUAL_START_DATE', 'ACTUAL_END_DATE', 'LAST_ACTIVITY_TIME_STAMP', 'ASGND_LOCATION_ID', 'ASGND_DEPARTMENT_ID', 'ASGND_CENTER_ID', 'UPDT_USERID', 'TITLE', 'OPER_STATUS', 'PART_NO', 'STD_HOURS', 'COMPLETE_QTY', 'SCRAP_QTY'])
# soper = pd.DataFrame(columns=['ORDER_ID', 'OPER_KEY', 'STEP_KEY', 'ACTUAL_INQUEUE_DATE', 'ACTUAL_START_DATE', 'ACTUAL_END_DATE', 'LAST_ACTIVITY_TIME_STAMP', 'SERIAL_ID', 'LOT_ID', 'SERIAL_OPER_STATUS'])

time_zero = datetime.strptime('01/01/2024 00:00:00', "%d/%m/%Y %H:%M:%S")

part_number_list = ['EUPAR1','EUPAR2','EUPAR3','EUPAR4','EUPAR5','EUPAR6','MUPAR1','MUPAR2','MUPAR3','ECPAR1','ECPAR2','ECPAR3','ECPAR4','ECPAR5','ECPAR6','MCPAR1','MCPAR2','MCPAR3','MCPAR4','MCPAR5','MCPAR6']


def create_order(part_no, completion, order_start_date):
    #########################
    #       INITIALIZE      #
    #########################
    order_start_date = datetime.strptime(order_start_date, "%d/%m/%Y %H:%M:%S")
    oper = route[route['PART_NO'] == part_no].copy()
    soper = route[route['PART_NO'] == part_no].copy()
    oper = pd.merge(oper, org, left_on= 'PLND_CENTER_ID', right_on = 'CENTER_ID', how = 'left')
    soper = pd.merge(soper, org, left_on= 'PLND_CENTER_ID', right_on = 'CENTER_ID', how = 'left')
    order = pd.DataFrame(columns = ['ORDER_ID', 'ORDER_NO', 'ACTUAL_START_DATE', 'ACTUAL_END_DATE', 'ORDER_STATUS', 'ITEM_ID', 'ROUTE_ID', 'LAST_ACTIVITY_TIME_STAMP', 'PART_NO', 'ASGND_LOCATION_ID', 'FAIR'])
    sdesc = pd.DataFrame(columns=['ORDER_ID', 'LOT_ID', 'SERIAL_ID', 'SERIAL_NO', 'SERIAL_STATUS', 'LAST_ACTIVITY_TIME_STAMP'])
    route_id = oper.at[0, 'ROUTE_ID']
    location_id = oper.at[0, 'LOCATION_ID']
    part_no = oper.at[0, 'PART_NO']
    item_id = part_no
    order_id = ''
    route_length = len(oper)

    #creating individual columns with its data
    oper_keys = route[route['PART_NO'] == part_no]['OPER_KEY'].tolist()

    update_userID = []
    for i in range(route_length):
        update_userID.append(oper.iat[i, int(np.round(19 + np.random.rand()))])

    batch_qty = soper['BATCH'].tolist()[0]

    sn_info = pd.read_excel('sn.xlsx')
    sn = int( sn_info[sn_info['PART_NO'] == part_no]['SN'] )

    on_info = pd.read_excel('order.xlsx')
    on = int( on_info[on_info['LOCATION_ID'] == location_id]['ORDER'] )
    on = on + 1

    order_id = location_id + '-ORDID-' + str(on)

    batch_id = location_id[-1] + '-BAT-' + str(on)


    #########################
    #   SERIAL OPER TABLE   #
    #########################
    soper.drop(columns=['ROUTE_ID', 'PART_NO', 'PLAN_NO', 'ORG_KEY', 'USER1', 'USER2', 'BATCH', 'PLAN_VERSION', 'PLAN_REVISION', 'REV_STATUS', 'OPER_NO', 'OPER_TITLE', \
        'PLND_CENTER_ID', 'CENTER_ID', 'CENTER_NAME', 'DEPT_ID', 'DEPT_NAME', 'LOCATION_ID', 'LOCATION_NAME', 'STD_HOURS'], inplace=True)
    soper['STEP_KEY'] = -1
    soper['ORDER_ID'] = order_id
    soper['LOT_ID'] = batch_id

    soper_full = pd.DataFrame(columns = soper.columns)

    for i in range(batch_qty):
        sn = sn + 1
        soper['SERIAL_ID'] = part_no + '-SID-' + str(sn)
        #sn_completion = np.random.rand() * completion
        if completion == 1 or batch_qty == 1:
            multiplier = 1
        else:
            multiplier = np.random.rand()

        sn_complete_op_status = int(np.round(route_length * completion * multiplier))
        sn_remaining_op_status = route_length - sn_complete_op_status
        sn_complete_op_status = ['COMPLETE'] * sn_complete_op_status
        sn_remaining_op_status = ['PENDING'] * (sn_remaining_op_status - 1)
        sn_current_op_status = [ ['IN QUEUE', 'ACTIVE'][int(np.round(np.random.rand()))] ] if len(sn_complete_op_status) != route_length else []
        sn_full_op_status = sn_complete_op_status + sn_current_op_status + sn_remaining_op_status
        soper['SERIAL_OPER_STATUS'] = sn_full_op_status

        sn_aiqd = [''] * route_length
        sn_asd = [''] * route_length
        sn_aed = [''] * route_length
        sn_lat = [''] * route_length
        sn_aiqd[0] = order_start_date.strftime("%d/%m/%Y %H:%M:%S")

        #populating operations date columns
        for i in range(route_length):

            if sn_full_op_status[i] == 'PENDING':
                sn_aiqd[i] = ''
                sn_asd[i] = ''
                sn_aed[i] = ''
                sn_lat[i] = ''

            elif sn_full_op_status[i] == 'IN QUEUE':
                sn_aiqd[i] = sn_aed[i-1] if i != 0 else sn_aiqd[i]
                sn_asd[i] = ''
                sn_aed[i] = ''
                sn_lat[i] = sn_aiqd[i]

            elif sn_full_op_status[i] == 'ACTIVE':
                sn_aiqd[i] = sn_aed[i-1] if i != 0 else sn_aiqd[i]
                sn_asd[i] = datetime.strftime( (datetime.strptime(sn_aiqd[i], "%d/%m/%Y %H:%M:%S") + timedelta(seconds = np.random.rand()*50000)), "%d/%m/%Y %H:%M:%S")
                sn_aed[i] = ''
                sn_lat[i] = sn_asd[i]

            elif sn_full_op_status[i] == 'COMPLETE':
                sn_aiqd[i] = sn_aed[i-1] if i != 0 else sn_aiqd[i]
                sn_asd[i]= datetime.strftime( (datetime.strptime(sn_aiqd[i], "%d/%m/%Y %H:%M:%S") + timedelta(seconds = np.random.rand()*50000)), "%d/%m/%Y %H:%M:%S")
                sn_aed[i] = datetime.strftime( (datetime.strptime(sn_asd[i], "%d/%m/%Y %H:%M:%S") + timedelta(hours= (np.random.rand()*10*oper.at[i, 'STD_HOURS']))), "%d/%m/%Y %H:%M:%S")
                sn_lat[i] = sn_aed[i]

        soper['ACTUAL_INQUEUE_DATE'] = sn_aiqd
        soper['ACTUAL_START_DATE'] = sn_asd
        soper['ACTUAL_END_DATE'] = sn_aed
        soper['LAST_ACTIVITY_TIME_STAMP'] = sn_lat

        soper_full = pd.concat([soper_full, soper], ignore_index = True)

        soper.drop(['SERIAL_OPER_STATUS', 'ACTUAL_INQUEUE_DATE', 'ACTUAL_START_DATE', 'ACTUAL_END_DATE', 'LAST_ACTIVITY_TIME_STAMP'], axis=1, inplace=True)

    soper_full = soper_full[['ORDER_ID', 'OPER_KEY', 'LOT_ID', 'SERIAL_ID', 'STEP_KEY', 'SERIAL_OPER_STATUS', 'ACTUAL_INQUEUE_DATE',\
         'ACTUAL_START_DATE', 'ACTUAL_END_DATE', 'LAST_ACTIVITY_TIME_STAMP']]
    sn_info.loc[sn_info['PART_NO'] == part_no, ['SN']] = sn
    on_info.loc[on_info['LOCATION_ID'] == location_id, ['ORDER']] = on

    #########################
    #   SERIAL DESC TABLE   #
    #########################
    sid_list = soper_full['SERIAL_ID'].drop_duplicates().tolist()
    print(sid_list)
    print(sdesc)
    sn_list = [(i.split('-')[0] + 'SN' + i.split('-')[2]) for i in sid_list]
    sdesc['SERIAL_ID'] = sid_list
    sdesc['ORDER_ID'] = order_id
    sdesc['LOT_ID'] = batch_id
    sdesc['SERIAL_NO'] = sn_list #pending in queue in process complete

    for i in sid_list:
        soper_status_list = soper_full.loc[soper_full['SERIAL_ID'] == i, 'SERIAL_OPER_STATUS'].tolist()
        max_lat =  soper_full.loc[soper_full['SERIAL_ID'] == i, 'LAST_ACTIVITY_TIME_STAMP'].tolist()
        max_lat = [b for b in max_lat if b != '']
        max_lat = max([datetime.strptime(b, "%d/%m/%Y %H:%M:%S") for b in max_lat]) if max_lat!= [] else ''
        sdesc.loc[sdesc['SERIAL_ID'] == i, 'LAST_ACTIVITY_TIME_STAMP'] = max_lat.strftime("%d/%m/%Y %H:%M:%S") if max_lat != '' else ''

        if 'ACTIVE' in soper_status_list :
            sdesc.loc[soper_full['SERIAL_ID'] == i, 'SERIAL_STATUS'] = 'IN PROCESS'
        elif all(x == 'PENDING' for x in soper_status_list):
            sdesc.loc[soper_full['SERIAL_ID'] == i, 'SERIAL_STATUS'] = 'PENDING'
        elif all(x == 'COMPLETE' for x in soper_status_list):
            sdesc.loc[soper_full['SERIAL_ID'] == i, 'SERIAL_STATUS'] = 'COMPLETE'
        else:
            sdesc.loc[soper_full['SERIAL_ID'] == i, 'SERIAL_STATUS'] = 'IN QUEUE'

    sdesc = sdesc[['ORDER_ID', 'LOT_ID', 'SERIAL_ID', 'SERIAL_NO', 'SERIAL_STATUS', 'LAST_ACTIVITY_TIME_STAMP']]
    #########################
    #       OPER TABLE      #
    #########################
    oper.drop(columns=['PLND_CENTER_ID', 'ROUTE_ID', 'PLAN_NO', 'PLAN_VERSION', 'PLAN_REVISION', 'REV_STATUS', 'ORG_KEY', 'USER1', 'USER2', 'BATCH', 'STD_HOURS', 'PART_NO'], inplace=True)
    oper.rename(columns={"LOCATION_ID": "ASGND_LOCATION_ID", "DEPT_ID": "ASGND_DEPARTMENT_ID", "CENTER_ID": "ASGND_CENTER_ID", "OPER_TITLE": "TITLE"}, inplace=True)
    oper['STEP_KEY'] = -1
    oper['ORDER_ID'] = order_id
    oper['UPDT_USERID'] = update_userID
    oper['OPER_STATUS'] = ''
    oper['SCRAP_QTY'] = 0
    oper['COMPLETE_QTY'] = 0
    oper_status = [''] * route_length

    #DATE POPULATION
    aiqd = [''] * route_length
    asd = [''] * route_length
    aed = [''] * route_length
    lat = [''] * route_length
    aiqd[0] = order_start_date.strftime("%d/%m/%Y %H:%M:%S")
    
    for i in range(route_length):
        min_aiqd = soper_full[soper_full['OPER_KEY'] == oper_keys[i]]['ACTUAL_INQUEUE_DATE'].tolist()
        min_aiqd = [b for b in min_aiqd if b != '']
        min_aiqd = min([datetime.strptime(b, "%d/%m/%Y %H:%M:%S") for b in min_aiqd]) if min_aiqd != [] else ''

        min_asd = soper_full[soper_full['OPER_KEY'] == oper_keys[i]]['ACTUAL_START_DATE'].tolist()
        min_asd = [b for b in min_asd if b != '']
        min_asd = min([datetime.strptime(b, "%d/%m/%Y %H:%M:%S") for b in min_asd]) if min_asd != [] else ''

        max_lat = soper_full[soper_full['OPER_KEY'] == oper_keys[i]]['LAST_ACTIVITY_TIME_STAMP'].tolist()
        max_lat = [b for b in max_lat if b != '']
        max_lat = max([datetime.strptime(b, "%d/%m/%Y %H:%M:%S") for b in max_lat]) if max_lat!= [] else ''

        max_aed = ''
        if len([p for p in soper_full[soper_full['OPER_KEY'] == oper_keys[i]]['ACTUAL_END_DATE'] if p == '']) == 0:
            max_aed = soper_full[soper_full['OPER_KEY'] == oper_keys[i]]['ACTUAL_END_DATE'].tolist()
            max_aed = [b for b in max_aed if b != '']
            max_aed = max([datetime.strptime(b, "%d/%m/%Y %H:%M:%S") for b in max_aed]) if max_aed != [] else ''

        aiqd[i] = min_aiqd.strftime("%d/%m/%Y %H:%M:%S") if min_aiqd != '' else ''
        asd[i] = min_asd.strftime("%d/%m/%Y %H:%M:%S") if min_asd != '' else ''
        lat[i] = max_lat.strftime("%d/%m/%Y %H:%M:%S") if max_lat != '' else ''
        aed[i] = max_aed.strftime("%d/%m/%Y %H:%M:%S") if max_aed != '' else ''

        curr_oper_status = soper_full[soper_full['OPER_KEY'] == oper_keys[i]]['SERIAL_OPER_STATUS'].tolist()
        if all(item == 'IN QUEUE' for item in curr_oper_status):
            curr_oper_status = 'IN QUEUE'
        elif all(item == 'PENDING' for item in curr_oper_status):
            curr_oper_status = 'PENDING'
        elif all(item == 'COMPLETE' for item in curr_oper_status):
            curr_oper_status = 'CLOSE'
        else:
            curr_oper_status = 'ACTIVE'

        oper_status[i] = curr_oper_status
   
    oper['ACTUAL_INQUEUE_DATE'] = aiqd
    oper['ACTUAL_START_DATE'] = asd
    oper['ACTUAL_END_DATE'] = aed
    oper['LAST_ACTIVITY_TIME_STAMP'] = lat
    oper['OPER_STATUS'] = oper_status
    oper = oper[['ORDER_ID', 'OPER_KEY', 'OPER_NO', 'STEP_KEY', 'TITLE', 'OPER_STATUS', 'ACTUAL_INQUEUE_DATE', 'ACTUAL_START_DATE', 'ACTUAL_END_DATE', 'LAST_ACTIVITY_TIME_STAMP',\
         'UPDT_USERID', 'ASGND_CENTER_ID', 'ASGND_DEPARTMENT_ID', 'ASGND_LOCATION_ID', 'COMPLETE_QTY', 'SCRAP_QTY']]

    #########################
    #      ORDER TABLE      #
    #########################
    order_status = 'PENDING'
    order_lats_arr = oper['LAST_ACTIVITY_TIME_STAMP'].tolist()
    order_lats_arr = [i for i in order_lats_arr if i != '']
    order_lats_arr = [datetime.strptime(i, "%d/%m/%Y %H:%M:%S") for i in order_lats_arr]
    order_lats = datetime.strftime(max(order_lats_arr), "%d/%m/%Y %H:%M:%S")

    if oper.at[0, 'OPER_STATUS'] == 'IN QUEUE':
        order_status = 'IN QUEUE'
    elif oper.at[route_length - 1, 'OPER_STATUS'] == 'CLOSE':
        order_status = 'CLOSE'
    else:
        order_status = 'ACTIVE'

    order.loc[len(order)] = [order_id, order_id + 'nnn', order_start_date, oper.at[route_length - 1, 'ACTUAL_END_DATE'], order_status, item_id, route_id, order_lats, part_no, location_id, 0]

    #----------------------------------------------------
    #########################
    #       FINISHING       #
    #########################

    print('\nORDER')
    print('------------------------')
    print(order)
    print('\nOPER')
    print('------------------------')
    print(oper)
    print('\nSERIAL OPER')
    print('------------------------')
    print(soper_full)
    print('\nSERIAL DESC')
    print('------------------------')
    print(sdesc)

    sn_info.loc[sn_info['PART_NO'] == part_no, ['SN']] = sn
    on_info.loc[on_info['LOCATION_ID'] == location_id, ['ORDER']] = on
    sn_info.to_excel('sn.xlsx', index = False)
    on_info.to_excel('order.xlsx', index = False)

    # oper.drop(['USER1', 'USER2', 'BATCH', 'STD_HOURS', 'PART_NO'], axis=1)
    # soper_full.drop(['STEP_KEY', 'STD_HOURS'], axis=1)

    with pd.ExcelWriter('MLF_ORDER1.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        order.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.book['Sheet1'].max_row)

    with pd.ExcelWriter('MLF_OPER1.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        oper.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.book['Sheet1'].max_row)
    
    with pd.ExcelWriter('MLF_SERIAL_OPER1.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        soper_full.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.book['Sheet1'].max_row)

    with pd.ExcelWriter('MLF_SERIAL_DESC1.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        sdesc.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.book['Sheet1'].max_row)
    return 0

def create_discrepancies(max_disc_percentage):
    disc_item_table = pd.DataFrame(columns = ['DISC_ID', 'DISC_LINE_TITLE', 'NOTES', 'DISC_LINE_STATUS', 'DISP_TYPE', 'ORDER_ID', 'OPER_KEY'])
    disc_item_serial_table = pd.DataFrame(columns = ['DISC_ID', 'LOT_NO', 'SERIAL_NO'])
    sop = pd.read_excel('MLF_SERIAL_OPER1.xlsx')
    sd = pd.read_excel('MLF_SERIAL_DESC1.xlsx')
    sop = pd.merge(sop, sd, how='left', on='SERIAL_ID')
    sn_list = np.array(sop[sop['SERIAL_OPER_STATUS'] == 'COMPLETE']['SERIAL_NO'].drop_duplicates().tolist())
    print('sn count: ', len(sn_list))
    disc_sn_count = int(np.round(max_disc_percentage * np.random.rand() * len(sn_list)))
    print('disc sn count: ', disc_sn_count)
    disc_sn_mask = []

    for dsc in range(len(sn_list)):
        if np.random.rand() > 0.5 and disc_sn_mask.count(True) < disc_sn_count:
            disc_sn_mask.append(True)
        else:
            disc_sn_mask.append(False)
    
    disc_sn_mask = np.array(disc_sn_mask)
    disc_sn_list = sn_list[disc_sn_mask]

    for u in range(len(disc_sn_list)):
        temp1 = {
            'DISC_ID': 'disc-' + str(u)
            ,'DISC_LINE_TITLE': 'disc title ' + str(u)
            ,'NOTES': 'notes abcdef ' + str(u)
            ,'DISC_LINE_STATUS': 'line status ' + str(u)
            ,'DISP_TYPE': random.choice(['REWORK', 'DISPOSITIONED', 'AS IS', 'CONCESSION'])
            ,'ORDER_ID': random.choice( sop[sop['SERIAL_NO'] == disc_sn_list[u]]['ORDER_ID_x'].drop_duplicates().tolist() )
            ,'OPER_KEY': random.choice( sop[sop['SERIAL_NO'] == disc_sn_list[u]]['OPER_KEY'].drop_duplicates().tolist() )
        }
        temp2 = {
            'DISC_ID': 'disc-' + str(u)
            ,'LOT_NO': random.choice( sop[sop['SERIAL_NO'] == disc_sn_list[u]]['LOT_ID_x'].drop_duplicates().tolist() )
            ,'SERIAL_NO': disc_sn_list[u]
        }
        disc_item_table = pd.concat([disc_item_table, pd.DataFrame(temp1, index=[0])])
        disc_item_serial_table = pd.concat([disc_item_serial_table, pd.DataFrame(temp2, index=[0])])

    disc_item_table.to_excel('MLF_DISC_ITEM.xlsx')
    disc_item_serial_table.to_excel('MLF_DISC_ITEM_SERIAL.xlsx')

for i in range(150):
    sel_pn = part_number_list[int(np.round(np.random.rand()*20))]
    order_st = datetime.strftime( time_zero + timedelta(seconds = np.random.rand()*31536000), "%d/%m/%Y %H:%M:%S")
    create_order(sel_pn, np.random.rand(), order_st)
    # create_order(sel_pn, 1, order_st)

for i in range(50):
    sel_pn = part_number_list[int(np.round(np.random.rand()*20))]
    order_st = datetime.strftime( time_zero + timedelta(seconds = np.random.rand()*31536000), "%d/%m/%Y %H:%M:%S")
    # create_order(sel_pn, np.random.rand(), order_st)
    create_order(sel_pn, 1, order_st)


create_discrepancies(0.3)