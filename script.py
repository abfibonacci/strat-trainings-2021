import ast
from authlib.client import AssertionSession
import pandas as pd
import numpy as np
import json
import pygsheets
from gspread import Client
import time
import sys
import os



def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

running = True

while running:
    try:
        start_time = time.clock()
        def create_assertion_session(conf_file, scopes, subject=None):
            with open(conf_file, 'r') as f:
                conf = json.load(f)

            token_url = conf['token_uri']
            issuer = conf['client_email']
            key = conf['private_key']
            key_id = conf.get('private_key_id')

            header = {'alg': 'RS256'}
            if key_id:
                header['kid'] = key_id
        #
            # Google puts scope in payload
            claims = {'scope': ' '.join(scopes)}
            return AssertionSession(
                grant_type=AssertionSession.JWT_BEARER_GRANT_TYPE,
                token_url=token_url,
                issuer=issuer,
                audience=token_url,
                claims=claims,
                subject=subject,
                key=key,
                header=header,
            )


        scopes = ['https://www.googleapis.com/auth/spreadsheets',
                'https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', ]
        session = create_assertion_session('client_secret.json', scopes)

        gc = Client(None, session)

        sh = gc.open("RawDataDMR")
        sh1 = gc.open("CalcSheet")
        sh2 = gc.open("Interface2020")
        wks = sh.worksheet("RawData").get_all_values()
        pd.set_option('display.max_rows', 500)

        df = pd.DataFrame(wks[1:], columns=wks[0])
        pd.set_option('display.max_columns', 50)
        df_orig = df.copy()
        del df['timestamp']
        del df['ScoutName']
        df.insert(0, 'Record', 0)

        #writes dforig to RawData
        wks1 = sh1.worksheet("RawData")
        cell_list = wks1.range('A2:AH1000')
        for i in range(0, df_orig.shape[1]):
            for j in range(0, df_orig.shape[0]):
                if isinstance(df_orig.iloc[j, i], np.generic):
                    val = df_orig.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+df_orig.shape[1]*j].value = pyval
                else:
                    cell_list[i+df_orig.shape[1]*j].value = df_orig.iloc[j, i]
        cell_list2 = wks1.range('A1:AH1')
        for i in range(0, df.shape[1]):
            cell_list2[i].value = df_orig.columns[i]
        wks1.update_cells(cell_list2)
        wks1.update_cells(cell_list)

        #finds index of x,y coord for starting position
        iStart = df.columns.get_loc('StartPosition')
        positions = df.iloc[:, iStart]
        xcoord = []
        ycoord = []
        #goes through and splits the coord list into x and y coords, puts each in their own column
        for coord in positions:
            assert type(coord) == str
            if coord == '' or coord[0] != '[':
                xcoord.append(-1)
                ycoord.append(-1)
            else:
                potx = ast.literal_eval(coord)[0]
                poty = ast.literal_eval(coord)[1]
                if(potx > 0.5):
                    xcoord.append(695*(1 - potx))
                    ycoord.append(364*(1 - poty))
                else:
                    xcoord.append(695*potx)
                    ycoord.append(364*poty)
        df.insert(iStart + 1, 'yCoordStart', ycoord)
        df.iloc[:, iStart] = xcoord
        df = df.rename(columns={'StartPosition': 'xCoordStart',
                                'TeleopScoring': 'TeleScoring'})

        #Adds columns for high ball fail, xcoord, ycoord for both tele
        iTeleScore = df.columns.get_loc('TeleScoring')
        df.insert(iTeleScore + 1, 'BallsHighFailTele', 0.0)
        df.insert(iTeleScore + 2, 'xCoordHighTele', 0.0)
        df.insert(iTeleScore + 3, 'yCoordHighTele', 0.0)

        #takes care of weird NoShow values
        for i in range(0, df.shape[0]):
            if df['NoShow'][i] == 1:
                if len(df['TeleScoring'][i]) >= 1:
                    df['NoShow'][i] = 0

        #turns string lists into lists
        for i in range(0, df.shape[0]):
            assert type(df.iloc[:, iTeleScore][i]) == str
            if df.iloc[:, iTeleScore][i] == '':
                df.iloc[:, iTeleScore][i] = []
            else:
                teleScores = ast.literal_eval(df.iloc[:, iTeleScore][i])
                df.iloc[:, iTeleScore][i] = teleScores
        #turns coordinate submissions into the form [0, x, y, 1, 0]
        for i in range(0, df.shape[0]):
            pntl = df['TeleScoring'][i]
            if(type(pntl) == tuple):
                df['TeleScoring'][i] = [[0, pntl[0], pntl[1], 1, 0]]

        #fixes data types
        icols = list(df.columns)
        icols.remove('Record')
        icols.remove('xCoordStart')
        icols.remove('yCoordStart')
        icols.remove('TeleScoring')
        icols.remove('xCoordHighTele')
        icols.remove('yCoordHighTele')
        icols.remove('Comments')
        icols.remove('BallsHighFailTele')
        for i in range(0, len(icols)):
            for k in range(0, df.shape[0]):
                assert type(df[icols[i]][k]) == str
                try:
                    df[icols[i]][k] = float(df[icols[i]][k])
                except:
                    df[icols[i]][k] = 0.0

        #splits records and copies into separate rows
        upBound = df.shape[0]
        for i in range(upBound):
            num = len(df['TeleScoring'][i])
            if num == 0:
                df['TeleScoring'][i] = 0.0
            elif num == 1:
                try:
                    df['TeleScoring'][i] = df['TeleScoring'][i][0]
                except:
                    df['TeleScoring'][i] = 0.0
            else:
                for k in range(1, num):
                    df = df.append([df[i:i+1]], ignore_index=True)
                    df['Record'][df.shape[0] - 1] = k
                    try:
                        df['TeleScoring'][df.shape[0] - 1] = df['TeleScoring'][i][k]
                    except:
                        df['TeleScoring'][df.shape[0] - 1] = []
                try:
                    df['TeleScoring'][i] = df['TeleScoring'][i][0]
                except:
                    df['TeleScoring'][i] = []

        #distrubutes records into corresponding columns
        df = df.rename(columns={'TeleScoring': 'BallsHighTele'})
        for i in range(df.shape[0]):
            if df['BallsHighTele'][i] == 0.0:
                continue
            else:
                df['yCoordHighTele'][i] = df['BallsHighTele'][i][2]
                df['xCoordHighTele'][i] = df['BallsHighTele'][i][1]
                df['BallsHighFailTele'][i] = df['BallsHighTele'][i][4]
                df['BallsHighTele'][i] = df['BallsHighTele'][i][3]
       
        #reflects x and y coords
        for i in range(df.shape[0]):
            potx = df['xCoordStart'][i]
            xTele = df['xCoordHighTele'][i]
            yTele = df['yCoordHighTele'][i]
            if potx > 0.5:
                df['xCoordHighTele'][i] = (1-xTele) * 695
                df['yCoordHighTele'][i] = (1-yTele) * 364
            else:
                df['xCoordHighTele'][i] = xTele * 695
                df['yCoordHighTele'][i] = yTele * 364

        #data points for starting heatmap
        df2 = df.iloc[:, 0:3]
        df2['xCoordStart'] = df['xCoordStart']
        df2['yCoordStart'] = df['yCoordStart']
        df2['Crossed'] = df['Crossed']

        for i in range(df2.shape[0]):
            if not(df2['xCoordStart'][i] > 0):
                df2 = df2.drop(i)
                i -= 1
        df2 = df2.reset_index(drop=True)

        #creates dataframe for shot coords
        df3 = df.iloc[:, 0:3]
        df3['xCoordHighTele'] = df['xCoordHighTele']
        df3['yCoordHighTele'] = df['yCoordHighTele']
        df3['BallsHighTele'] = df['BallsHighTele']
        df3['BallsHighFailTele'] = df['BallsHighFailTele']
        for i in range(df3.shape[0]):
            if(not(df3['xCoordHighTele'][i] > 0)):
                df3 = df3.drop(i)
                i -= 1
        df3 = df3.reset_index(drop=True)
        for i in range(df3.shape[0]):
            if(df3['BallsHighTele'][i] <= 0 and df3['BallsHighFailTele'][i] <= 0):
                df3 = df3.drop(i)
                i -= 1
        df3 = df3.reset_index(drop=True)


        upBound = df3.shape[0]
        df3['Hit/Miss'] = ''
        for i in range(upBound):
            numS = int(df3['BallsHighTele'][i])
            numF = int(df3['BallsHighFailTele'][i])
            list_sf = []
            for m in range(numS):
                list_sf.append("Hit")
            for n in range(numF):
                list_sf.append("Miss")
            df3['Hit/Miss'][i] = list_sf[0]
            list_sf.pop(0)
            num = len(list_sf)
            for k in range(num):
                df3 = df3.append([df3[i:i+1]], ignore_index=True)
                df3['Hit/Miss'][df3.shape[0] - 1] = list_sf[0]
                list_sf.pop(0)
        del df3['BallsHighTele']
        del df3['BallsHighFailTele']

        #writes df to NumberFix
        wks2 = sh1.worksheet("NumberFix")
        cell_list = wks2.range('A2:AK1000')
        for i in range(0, df.shape[1]):
            for j in range(0, df.shape[0]):
                if isinstance(df.iloc[j, i], np.generic):
                    val = df.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+df.shape[1]*j].value = pyval
                else:
                    cell_list[i+df.shape[1]*j].value = df.iloc[j, i]
        cell_list2 = wks2.range('A1:AK1')
        for i in range(0, df.shape[1]):
            cell_list2[i].value = df.columns[i]
        wks2.update_cells(cell_list2)
        wks2.update_cells(cell_list)

        #writes starting position coords to StartingPosCoords
        wks3 = sh1.worksheet("StartingPosCoords")
        cell_list = wks3.range('A2:F1000')
        for i in range(0, df2.shape[1]):
            for j in range(0, df2.shape[0]):
                if isinstance(df2.iloc[j, i], np.generic):
                    val = df2.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+df2.shape[1]*j].value = pyval
                else:
                    cell_list[i+df2.shape[1]*j].value = df2.iloc[j, i]
        cell_list2 = wks3.range('A1:F1')
        for i in range(0, df2.shape[1]):
            cell_list2[i].value = df2.columns[i]
        wks3.update_cells(cell_list2)
        wks3.update_cells(cell_list)

        #writes shot coordinates to ShotCoords
        wks4 = sh2.worksheet("ShotsCoords")
        cell_list = wks4.range('A2:F1000')
        for i in range(0, df3.shape[1]):
            for j in range(0, df3.shape[0]):
                if isinstance(df3.iloc[j, i], np.generic):
                    val = df3.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+df3.shape[1]*j].value = pyval
                else:
                    cell_list[i+df3.shape[1]*j].value = df3.iloc[j, i]
        cell_list2 = wks4.range('A1:F1')
        for i in range(0, df3.shape[1]):
            cell_list2[i].value = df3.columns[i]

        wks4.update_cells(cell_list2)
        wks4.update_cells(cell_list)

        #writes shot coordinates to interface ShotCoords
        wks41 = sh2.worksheet("ShotsCoords")
        cell_list = wks41.range('A2:F1000')
        for i in range(0, df3.shape[1]):
            for j in range(0, df3.shape[0]):
                if isinstance(df3.iloc[j, i], np.generic):
                    val = df3.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+df3.shape[1]*j].value = pyval
                else:
                    cell_list[i+df3.shape[1]*j].value = df3.iloc[j, i]
        cell_list2 = wks41.range('A1:F1')
        for i in range(0, df3.shape[1]):
            cell_list2[i].value = df3.columns[i]
        wks41.update_cells(cell_list2)
        wks41.update_cells(cell_list)

        #creates MAR, df5
        teams = df['TeamNumber'].unique().tolist()
        df5 = pd.DataFrame(columns=df.columns)
        for team in teams:
            temp = df[df['TeamNumber'] == team].reset_index(drop=True)
            mnums = temp['MatchNumber'].unique().tolist()
            for match in mnums:
                temp2 = temp[temp['MatchNumber'] == match].reset_index(drop=True)
                temp4 = temp2[temp2['Record'] == 0].reset_index(drop=True)
                temp3 = pd.DataFrame(temp2[:1], columns=temp2.columns)
                num = temp2['Record'].shape[0] / \
                    len(temp2.groupby('Record').size())
                assert num > 0

                noShow = 0
                for value in temp4['NoShow']:
                    if value == 1:
                        noShow = 1
                        break
                    else:
                        noShow = 0
                temp3['NoShow'][0] = noShow

                lineCross = 0
                for value in temp4['Crossed']:
                    if value == 1:
                        lineCross = 1
                        break
                    else:
                        lineCross = 0
                temp3['Crossed'][0] = lineCross

                extraBalls = False
                for value in temp4['ExtraBalls']:
                    if value == 1:
                        extraBalls = 1
                        break
                    else:
                        extraBalls = 0
                temp3['ExtraBalls'][0] = extraBalls

                temp3['BallsUpperAuto'][0] = temp4['BallsUpperAuto'].sum()/num
                temp3['BallsUpperFailAuto'][0] = temp4['BallsUpperFailAuto'].sum()/num
                temp3['BallsLowerAuto'][0] = temp4['BallsLowerAuto'].sum()/num
                temp3['BallsLowerFailAuto'][0] = temp4['BallsLowerFailAuto'].sum()/num
                temp3['BallsHighTele'][0] = temp4['BallsHighTele'].sum()/num
                temp3['BallsHighFailTele'][0] = temp4['BallsHighFailTele'].sum()/num
                temp3['BallsLowerTele'][0] = temp4['BallsLowerTele'].sum()/num
                temp3['BallsLowerFailTele'][0] = temp4['BallsLowerFailTele'].sum()/num

                parked = 0
                for value in temp4['Parked']:
                    if value == 1:
                        parked = 1
                        break
                    else:
                        parked = 0
                temp3['Parked'][0] = parked

                climbAttempt = 0
                for value in temp4['ClimbAttempted']:
                    if value == 1:
                        climbAttempt = 1
                        break
                    else:
                        climbAttempt = 0
                temp3['ClimbAttempted'][0] = climbAttempt

                climbSuc = 0
                for value in temp4['ClimbSuccess']:
                    if value == 1:
                        climbSuc = 1
                        break
                    else:
                        climbSuc = 0
                temp3['ClimbSuccess'][0] = climbSuc

                lvlClimb = 0
                for value in temp4['LevelClimb']:
                    if value == 1:
                        lvlClimb = 1
                        break
                    else:
                        lvlClimb = 0
                temp3['LevelClimb'][0] = lvlClimb

                budClimbL = 0
                for value in temp4['BuddyClimbLifted']:
                    if value == 1:
                        budClimbL = 1
                        break
                    else:
                        budClimbL = 0
                temp3['BuddyClimbLifted'][0] = budClimbL

                budClimbP = 0
                for value in temp4['BuddyClimbPickedUp']:
                    if value == 1:
                        budClimbP = 1
                        break
                    else:
                        budClimbP = 0
                temp3['BuddyClimbPickedUp'][0] = budClimbP

                breakdown = 0
                for value in temp4['Breakdown']:
                    if value == 1:
                        breakdown = 1
                        break
                    else:
                        breakdown = 0
                temp3['Breakdown'][0] = breakdown

                temp3['Trench'][0] = temp4['Trench'].sum()/num
                temp3['Rendezvous'][0] = temp4['Rendezvous'].sum()/num
                temp3['Defense'][0] = temp4['Defense'].sum()/num
                temp3['Climb'][0] = temp4['ControlPanel'].sum()/num
                temp3['LoadingZoneIntake'][0] = temp4['GroundIntake'].sum()/num
                temp3['Maneuverability'][0] = temp4['Maneuverability'].sum()/num

                comments = []
                for comment in temp4['Comments']:
                    if len(comment) <= 0:
                        continue
                    else:
                        comments.append(comment)

                temp3['Comments'][i] = ''.join([str(elem) for elem in comments])
                comments = []
                df5 = df5.append(temp3, ignore_index=True)

        #takes out coordinate and record columns from df
        del df5['Record']
        del df5['xCoordStart']
        del df5['yCoordStart']
        del df5['xCoordHighTele']
        del df5['yCoordHighTele']

        #writes df5 to MAR
        wks5 = sh1.worksheet("MAR")
        cell_list = wks5.range('A2:AF1000')
        for i in range(0, df5.shape[1]):
            for j in range(0, df5.shape[0]):
                if isinstance(df5.iloc[j, i], np.generic):
                    val = df5.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+df5.shape[1]*j].value = pyval
                else:
                    cell_list[i+df5.shape[1]*j].value = df5.iloc[j, i]
        cell_list2 = wks5.range('A1:AF1')
        for i in range(0, df5.shape[1]):
            cell_list2[i].value = df5.columns[i]
        wks5.update_cells(cell_list2)
        wks5.update_cells(cell_list)

        #creates perTeamData, p1
        wks = sh2.worksheet("TeamNamePic").get_all_values()
        dfNames = pd.DataFrame(wks[1:], columns=wks[0])
        dfNames['Team#'] = dfNames['Team#'].astype(int)
        del dfNames['ImageURL']
        del dfNames['ImagePreview']
        p1cols = ['TeamNumber', 'TeamName', 'MatchesScouted', 'Breakdowns', 'NoShows', 'ESPM', 'Cross%',
                'MeanLowAuto', 'StDevLowAuto', 'MaxLowAuto', 'Low%Auto', 'MeanHighAuto', 'StDevHighAuto',
                'MaxHighAuto', 'High%Auto', 'ExtraBalls%', 'MeanLowTele', 'StDevLowTele',
                'MaxLowTele', 'Low%Tele', 'MeanHighTele', 'StDevHighTele', 'MaxHighTele', 'High%Tele', 'RotCtrl%',
                'PosCtrl%', 'Parked%', 'ClimbAttempt%', 'ClimbSuccess%', 'LvlClimb%', 'Lifted%', 'PickedUp%', 'MeanTrench', 'StDevTrench',
                'MeanRdvz', 'StDevRdvz', 'MeanDefense', 'StDevDefense', 'MeanClimb', 'StDevClimb', 'MeanCtrlPnl',
                'StDevCtrlPnl', 'MeanLZIntake', 'StDevLZIntake', 'MeanGIntake', 'StDevGIntake', 'MeanMnv', 'StDevMnv', 'Comments']

        p1 = pd.DataFrame(columns=p1cols)
        p1['TeamNumber'] = df['TeamNumber'].unique()

        for i in range(0, p1.shape[0]):
            tnum = p1['TeamNumber'][i]
            try:
                p1['TeamName'][i] = dfNames.loc[dfNames['Team#']
                                                == tnum].reset_index(drop=True).iloc[0, 1]
            except:
                p1['TeamName'][i] = "NA"
            p1['MatchesScouted'][i] = len(
                df5.loc[df5['TeamNumber'] == tnum]['MatchNumber'].unique())
            p1['Breakdowns'][i] = len(
                df5.loc[df5['TeamNumber'] == tnum][df5['Breakdown'] == 1])
            p1['NoShows'][i] = len(
                df5.loc[df5['TeamNumber'] == tnum][df5['NoShow'] == 1])
            
            try:
                p1['Cross%'][i] = len(df5.loc[df5['TeamNumber'] == tnum]
                                [df5['Crossed'] == 1])/p1['MatchesScouted'][i] * 100
            except:
                p1['Cross%'][i] = 0

            p1['MeanLowAuto'][i] = df5.loc[df5['TeamNumber']
                                        == tnum]['BallsLowerAuto'].mean()
            p1['StDevLowAuto'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['BallsLowerAuto'].std()
            p1['MaxLowAuto'][i] = df5.loc[df5['TeamNumber']
                                        == tnum]['BallsLowerAuto'].max()
            try:
                p1['Low%Auto'][i] = df5.loc[df5['TeamNumber'] == tnum]['BallsLowerAuto'].sum()/(df5.loc[df5['TeamNumber'] == tnum]['BallsLowerAuto'].sum()+ +                                                            df5.loc[df5['TeamNumber'] == tnum]['BallsLowerFailAuto'].sum()) * 100
            except:
                 p1['Low%Auto'][i] = 0
            p1['MeanHighAuto'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['BallsUpperAuto'].mean()
            p1['StDevHighAuto'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['BallsUpperAuto'].std()
            p1['MaxHighAuto'][i] = df5.loc[df5['TeamNumber']
                                        == tnum]['BallsUpperAuto'].max()
            try:
                p1['High%Auto'][i] = df5.loc[df5['TeamNumber'] == tnum]['BallsUpperAuto'].sum()/(df5.loc[df5['TeamNumber'] == tnum]['BallsUpperAuto'].sum() +
                 df5.loc[df5['TeamNumber'] == tnum]['BallsUpperFailAuto'].sum()) * 100
            except:
                p1['High%Auto'][i] = 0
            try:
                p1['ExtraBalls%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['ExtraBalls'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['ExtraBalls']) * 100
            except:
                p1['ExtraBalls%'][i] = 0
            p1['MeanLowTele'][i] = df5.loc[df5['TeamNumber']
                                        == tnum]['BallsLowerTele'].mean()
            p1['StDevLowTele'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['BallsLowerTele'].std()
            p1['MaxLowTele'][i] = df5.loc[df5['TeamNumber']
                                        == tnum]['BallsLowerTele'].max()
            try:
                p1['Low%Tele'][i] = df5.loc[df5['TeamNumber'] == tnum]['BallsLowerTele'].sum()/(df5.loc[df5['TeamNumber'] == tnum]['BallsLowerTele'].sum() +                                                               df5.loc[df5['TeamNumber'] == tnum]['BallsLowerFailTele'].sum()) * 100
            except: 
                p1['Low%Tele'][i] = 0                                     
            p1['MeanHighTele'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['BallsHighTele'].mean()
            p1['StDevHighTele'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['BallsHighTele'].std()
            p1['MaxHighTele'][i] = df5.loc[df5['TeamNumber'] == tnum]['BallsHighTele'].max()
            try:
                p1['High%Tele'][i] = df5.loc[df5['TeamNumber'] == tnum]['BallsHighTele'].sum()/(df5.loc[df5['TeamNumber'] == tnum]['BallsHighTele'].sum() +
                df5.loc[df5['TeamNumber'] == tnum]['BallsHighFailTele'].sum()) * 100                                                        
            except:
                p1['High%Tele'][i] = 0
            try:
                p1['RotCtrl%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['RotationControlSuccess'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['RotationControlSuccess']) * 100
            except:
                p1['RotCtrl%'][i] = 0
            try:
                p1['PosCtrl%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['PositionControlSuccess'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['PositionControlSuccess']) * 100
            except:
                p1['PosCtrl%'][i] = 0
            try:
                p1['Parked%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['Parked'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['Parked']) * 100
            except:
                p1['Parked%'][i] = 0
            try:
                p1['ClimbAttempt%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['ClimbAttempted'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['ClimbAttempted']) * 100
            except:
                p1['ClimbAttempt%'][i] = 0
            try:
                p1['ClimbSuccess%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['ClimbSuccess'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['ClimbSuccess']) * 100
            except:
                p1['ClimbSuccess%'][i] = 0
            try:
                p1['LvlClimb%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['LevelClimb'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['LevelClimb']) * 100
            except:
                p1['LvlClimb%'][i] = 0
            try:
                p1['Lifted%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['BuddyClimbLifted'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['BuddyClimbLifted']) * 100
            except:
                p1['Lifted%'][i] = 0
            try:
                p1['PickedUp%'][i] = len(df5.loc[df5['TeamNumber'] == tnum][df5['BuddyClimbPickedUp'] == 1]) / \
                    len(df5.loc[df5['TeamNumber'] == tnum]['BuddyClimbPickedUp']) * 100
            except:
                p1['PickedUp%'][i] = 0
            p1['MeanTrench'][i] = df5.loc[df5['TeamNumber']
                                        == tnum]['Trench'].mean()
            p1['StDevTrench'][i] = df5.loc[df5['TeamNumber'] == tnum]['Trench'].std()
            p1['MeanRdvz'][i] = df5.loc[df5['TeamNumber'] == tnum]['Rendezvous'].mean()
            p1['StDevRdvz'][i] = df5.loc[df5['TeamNumber'] == tnum]['Rendezvous'].std()
            p1['MeanDefense'][i] = df5.loc[df5['TeamNumber'] == tnum]['Defense'].mean()
            p1['StDevDefense'][i] = df5.loc[df5['TeamNumber'] == tnum]['Defense'].std()
            p1['MeanClimb'][i] = df5.loc[df5['TeamNumber'] == tnum]['Climb'].mean()
            p1['StDevClimb'][i] = df5.loc[df5['TeamNumber'] == tnum]['Climb'].std()
            p1['MeanDefense'][i] = df5.loc[df5['TeamNumber'] == tnum]['Defense'].mean()
            p1['StDevDefense'][i] = df5.loc[df5['TeamNumber'] == tnum]['Defense'].std()
            p1['MeanCtrlPnl'][i] = df5.loc[df5['TeamNumber'] == tnum]['ControlPanel'].mean()
            p1['StDevCtrlPnl'][i] = df5.loc[df5['TeamNumber'] == tnum]['ControlPanel'].std()
            p1['MeanLZIntake'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['LoadingZoneIntake'].mean()
            p1['StDevLZIntake'][i] = df5.loc[df5['TeamNumber']
                                            == tnum]['LoadingZoneIntake'].std()
            p1['MeanGIntake'][i] = df5.loc[df5['TeamNumber'] == tnum]['GroundIntake'].mean()
            p1['StDevGIntake'][i] = df5.loc[df5['TeamNumber'] == tnum]['GroundIntake'].std()
            p1['MeanMnv'][i] = df5.loc[df5['TeamNumber']
                                    == tnum]['Maneuverability'].mean()
            p1['StDevMnv'][i] = df5.loc[df5['TeamNumber'] == tnum]['Maneuverability'].std()
            comments = []
            for comment in df5.loc[df5['TeamNumber'] == tnum]['Comments']:
                if len(comment) <= 0:
                    continue
                else:
                    comments.append("\"" + comment + "\"")

            p1['Comments'][i] = '+'.join([str(elem) for elem in comments])
            comments = []

            p1['ESPM'][i] = p1['Cross%'][i] * 5 * 0.01 + p1['MeanLowAuto'][i] * 2 + p1['MeanHighAuto'][i] * 4 + p1['MeanLowTele'][i] * 1 + p1['MeanHighTele'][i] * 2 + p1['RotCtrl%'][i]* 10 * 0.01 + p1['PosCtrl%'][i] * 20 * 0.01 + p1['LvlClimb%'][i] * 15 * 0.01
            
            if(p1['Parked%'][i] > p1['ClimbSuccess%'][i]):
                p1['ESPM'][i] += p1['Parked%'][i] * 5 / 100 
            else:
                p1['ESPM'][i] += p1['ClimbSuccess%'][i] * 25 / 100
            



        p1.fillna("NA", inplace = True)

        #writes p1 to perTeamData
        wks6 = sh1.worksheet("PerTeamData")
        cell_list = wks6.range('A2:AW1000')
        for i in range(0, p1.shape[1]):
            for j in range(0, p1.shape[0]):
                if isinstance(p1.iloc[j, i], np.generic):
                    val = p1.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+p1.shape[1]*j].value = pyval
                else:
                    cell_list[i+p1.shape[1]*j].value = p1.iloc[j, i]
        cell_list2 = wks6.range('A1:AW1')
        for i in range(0, p1.shape[1]):
            cell_list2[i].value = p1.columns[i]
        wks6.update_cells(cell_list2)
        wks6.update_cells(cell_list)

        wks7 = sh2.worksheet("PerTeamData")
        cell_list = wks7.range('A4:AW1000')
        for i in range(0, p1.shape[1]):
            for j in range(0, p1.shape[0]):
                if isinstance(p1.iloc[j, i], np.generic):
                    val = p1.iloc[j, i]
                    pyval = val.item()
                    cell_list[i+p1.shape[1]*j].value = pyval
                else:
                    cell_list[i+p1.shape[1]*j].value = p1.iloc[j, i]
        cell_list2 = wks7.range('A3:AW3')
        for i in range(0, p1.shape[1]):
            cell_list2[i].value = p1.columns[i]
        wks7.update_cells(cell_list2)
        wks7.update_cells(cell_list)


        print("SHEETS UPDATE SUCCESS") 
        print (time.clock() - start_time, "seconds")
        time.sleep(300)
        print("REUPDATING")
    except KeyboardInterrupt:
        running = False


print("Program ending.")
ans = input("Would you like to stop importation? y/n")
if(ans.strip().lower() == 'y'):
    print("Okay!")
    pass

elif(ans.strip().lower() == 'n'):
    print("Program restarting!")
    restart_program()
else:
    print("Sorry, I don't understand. Ending program now.")
